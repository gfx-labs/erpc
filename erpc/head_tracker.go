package erpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/data"
	"github.com/erpc/erpc/telemetry"
	"github.com/rs/zerolog"
	"golang.org/x/sync/singleflight"
)

// ─── Fleet head tracker ("stalker") ──────────────────────────────────────────
//
// WHY. "latest" used to come only from the per-upstream state pollers. Every
// replica polls every upstream on statePollerInterval, so a slow interval
// (60s) leaves the served head up to a minute stale, and a fast one multiplies
// cost by upstreams × replicas.
//
// WHAT. Per network, exactly ONE replica (the holder of a shared-state lease)
// loops eth_getBlockByNumber("latest") through the network's NORMAL forwarding
// path, so selection policy, failover and integrity checks all apply, and the
// cheapest healthy upstream answers. It waits about one block time between
// polls, aligned to the expected timestamp of the next block plus the
// observed propagation delay (the venn stalker's schedule). Each new head is
// published through a shared-state counter; every replica receives it by
// pub/sub and serves it as the network's "latest". Total cost is ~1 upstream
// call per block per network, independent of replica and upstream count.
//
// NO MAJORITY. The tracker head is NOT passed through PickServedTip: the
// leader's routed, integrity-checked observation IS the network's latest.
// Only checks that need no other upstream to agree guard it:
//   - a head that regresses more than DefaultToleratedBlockHeadRollback below
//     the published head is rejected (small regressions are just a lagging
//     upstream answering and are ignored by the counter);
//   - a head whose on-chain timestamp is in the future, or that advanced far
//     faster than the measured block time allows since the previous head, is
//     rejected;
//   - a jump larger than the major-move threshold is accepted only after the
//     serving upstream's eth_chainId matches the network.
//
// FALLBACK. When the tracker head has not advanced for max(3 × block time,
// headTrackerMinStale), e.g. no leader or a halted chain, replicas fall back
// to the previous served-tip path (per-upstream poller heads) and say so:
// erpc_head_tracker_fallback_active=1, a counter, and a rate-limited WARN.
// Once majority servedTip is dropped from such networks, that fallback is the
// default corroborated (second-highest) poller head.
//
// TRADEOFF. An upstream behind the tracker head may be asked for a block it
// does not have yet. Routing already handles that: block-range methods
// force-poll the upstream's head on demand (EvmAssertBlockAvailability), and
// missing-data responses are retried on other upstreams. The upstream that
// served the tracker's poll has its own head advanced through the normal
// response enrichment (SuggestLatestBlock), so it is never considered behind.
// Requests carrying a use-upstream selector keep the selector-scoped poller
// head: the tracker head describes the network, not an arbitrary subset.

const (
	// headTrackerMinStale floors the staleness window so a fast chain's normal
	// jitter never flips replicas into fallback.
	headTrackerMinStale = 5 * time.Second
	// headTrackerStaleBlocks is the staleness window in block times.
	headTrackerStaleBlocks = 3
	// headTrackerMaxPollFailures is how many consecutive failed polls a leader
	// tolerates before releasing the lease so another replica (maybe with a
	// healthier network path) takes over.
	headTrackerMaxPollFailures = 20
	// headTrackerDelaySamples is the propagation-delay window.
	headTrackerDelaySamples = 32
	// headTrackerFutureSlack is how far in the future a block timestamp may be
	// (clock skew between the chain and this host) before it is rejected.
	headTrackerFutureSlack = 60 * time.Second
	// headTrackerWarnEvery rate-limits the fallback WARN log.
	headTrackerWarnEvery = time.Minute
	// headTrackerGapSamples is how many recent liveness gaps staleAfter
	// considers (the max of them).
	headTrackerGapSamples = 64
	// headTrackerMaxGap bounds a poll interval that counts as cadence.
	headTrackerMaxGap = 5 * time.Minute
	// headTrackerColdStale is the staleness window while neither a measured
	// block time nor a published leader window is available.
	headTrackerColdStale = time.Minute
	// headTrackerClockSkew is the skew tolerated when judging the age of the
	// first alive value a replica learns.
	headTrackerClockSkew = 5 * time.Second
	// headTrackerRecoverAfter is how many consecutive, mutually consistent
	// polls below the regression tolerance it takes to accept a large
	// rollback: the recovery path for a published head that was poisoned
	// (a bogus far-future value that every honest poll now "regresses" from).
	headTrackerRecoverAfter = 3
	// headTrackerFallbackFloorTTL bounds how long the fallback keeps serving
	// the last fresh tracker head as a floor (the same one-minute bound the
	// served-tip regression guard uses), so a floor can never become a wedge.
	headTrackerFallbackFloorTTL = servedTipRegressionTTL
)

// headObservation is one parsed leader poll result.
type headObservation struct {
	Number    int64
	Hash      string
	Timestamp int64 // unix seconds (on-chain)
	Upstream  common.Upstream
	// Raw is the block JSON exactly as returned (full or hashes-only per Full).
	Raw  json.RawMessage
	Full bool
}

type headTrackerDeps struct {
	// poll fetches "latest" through the network's normal routing.
	poll func(ctx context.Context, full bool) (*headObservation, error)
	// blockTime is the network's measured (EMA) block time, 0 when unknown.
	blockTime func() time.Duration
	// verifyChainId reports whether the upstream that served a suspicious
	// jump is on this network's chain.
	verifyChainId func(ctx context.Context, u common.Upstream) (bool, error)
	// majorMove is the jump size (blocks) that needs chain-id verification.
	majorMove func() int64
	// onAccepted runs on the leader for every accepted NEW head (cache and
	// blockstore writes, EMA feed). Must not block for long.
	onAccepted func(ctx context.Context, obs *headObservation)
	// fallbackHead is the head served while the tracker head is stale; used
	// only for the served-lag metric.
	fallbackHead func(ctx context.Context) int64
	now          func() time.Time
}

type headTracker struct {
	projectId string
	networkId string
	label     string
	cfg       *common.EvmHeadTrackerConfig
	ssr       data.SharedStateRegistry
	head      data.CounterInt64SharedVariable
	leaseKey  string
	deps      headTrackerDeps
	logger    *zerolog.Logger

	isLeader atomic.Bool
	// abandonLease (tests) makes the leader stop WITHOUT releasing its lease,
	// simulating a crashed pod whose lease must expire.
	abandonLease atomic.Bool
	// fallback state for the transition counter and the rate-limited WARN.
	inFallback   atomic.Bool
	lastWarnAtMs atomic.Int64

	// Leader-local state (only touched by the poll loop goroutine).
	prev   *headObservation
	prevAt time.Time
	delays []time.Duration
	// regressStreak / regressLast track consecutive consistent regression
	// rejections (poisoned-counter recovery).
	regressStreak int
	regressLast   int64
	// staleStreak counts consecutive same-head polls past the expected next
	// block (staleWait backoff); reset on every new head.
	staleStreak int

	// leaseDeadlineNs is the instant (unix ns, local clock) after which this
	// replica can no longer prove it holds the lease: lastOk + 2·ttl/3. The
	// leader stops polling and never publishes past it (S4).
	leaseDeadlineNs atomic.Int64

	// advancedAtNs is the LOCAL receipt time of the last head advance this
	// replica observed (leader publish or pub/sub delivery). Freshness is
	// measured against it, never against the remote writer's clock (S5).
	// seenHead is the last head value this replica observed through the
	// counter callback.
	advancedAtNs atomic.Int64
	seenHead     atomic.Int64
	seenAlive    atomic.Int64
	// gaps (guarded by gapMu) holds recent intervals between liveness
	// signals on this replica's clock; maxGapNs is their max.
	gapMu    sync.Mutex
	gaps     []time.Duration
	maxGapNs atomic.Int64
	// alive is the leader's liveness counter: unix ms of its last successful
	// poll, published whether or not the head moved (at most once per poll).
	alive data.CounterInt64SharedVariable
	// staleMs is the leader's own staleness window (ms), published so
	// followers need neither a warm EMA nor history of their own.
	staleMs data.CounterInt64SharedVariable
	// lastFreshHead / lastFreshAtNs remember the last head served as fresh,
	// the floor for the fallback path (S7).
	lastFreshHead atomic.Int64
	lastFreshAtNs atomic.Int64

	// procMu serializes poll-result processing (polls themselves overlap).
	procMu sync.Mutex

	// balancer spreads leases across replicas (see headTrackerBalancer).
	balancer *headTrackerBalancer

	// checks / checkGroup back checkUpstreamHead (tip checks).
	checks     sync.Map // upstream id -> *upstreamCheck
	checkGroup singleflight.Group

	stopOnce sync.Once
	stop     context.CancelFunc
	done     chan struct{}
}

func newHeadTracker(projectId, networkId, label string, cfg *common.EvmHeadTrackerConfig, ssr data.SharedStateRegistry, deps headTrackerDeps, logger *zerolog.Logger) *headTracker {
	if deps.now == nil {
		deps.now = time.Now
	}
	if deps.majorMove == nil {
		deps.majorMove = func() int64 { return common.DefaultToleratedBlockHeadRollback }
	}
	lg := logger.With().Str("component", "headTracker").Logger()
	scope := projectId + "/" + networkId
	t := &headTracker{
		projectId: projectId,
		networkId: networkId,
		label:     label,
		cfg:       cfg,
		ssr:       ssr,
		head:      ssr.GetCounterInt64(data.CounterValueSchemaVersion+"/headTracker/"+scope, common.DefaultToleratedBlockHeadRollback),
		alive:     ssr.GetCounterInt64(data.CounterValueSchemaVersion+"/headTrackerAlive/"+scope, 0),
		staleMs:   ssr.GetCounterInt64(data.CounterValueSchemaVersion+"/headTrackerStaleMs/"+scope, 0),
		leaseKey:  "headTracker/" + scope,
		deps:      deps,
		logger:    &lg,
	}
	// Freshness is LIVENESS of the leader, not chain progress. The leader
	// publishes a monotonic "alive" counter after every successful poll
	// (new head or not), and a replica counts the head as fresh while it saw
	// the counter (or the head) change within staleAfter, on its own clock.
	// The first value a replica learns (initial fetch of a possibly
	// hours-old counter after boot) only seeds the comparison.
	//
	// Head advancement alone was the wrong signal: on slow or irregular
	// chains (ethereum missed slots, on-demand blocks on redbelly/
	// rootstock/saga, filecoin null rounds) the head legitimately does not
	// move for many block times while every poll succeeds, and replicas
	// flapped into fallback although the head was correct.
	t.head.OnValue(func(v int64) {
		if prev := t.seenHead.Swap(v); prev > 0 && v != prev {
			t.advancedAtNs.Store(t.deps.now().UnixNano())
		}
	})
	t.alive.OnValue(func(v int64) {
		prev := t.seenAlive.Swap(v)
		if v == prev {
			return
		}
		now := t.deps.now()
		if prev <= 0 {
			// First value this replica learns (startup, or a new counter
			// after failover): its age is only knowable across clocks. Honor
			// it when it is plausibly recent (within the staleness window,
			// tolerating a few seconds of skew), so a starting follower does
			// not wait up to a whole block interval of a slow chain before
			// serving the tracker head.
			if age := now.Sub(time.UnixMilli(v)); age > -headTrackerClockSkew && age <= t.staleAfter() {
				t.advancedAtNs.Store(now.Add(-max(age, 0)).UnixNano())
			}
			return
		}
		t.advancedAtNs.Store(now.UnixNano())
		// Both values were stamped by the leader's clock, so their
		// difference is a pure duration (no cross-host skew): the leader's
		// actual poll cadence.
		t.noteLivenessGap(time.Duration(v-prev) * time.Millisecond)
	})
	t.staleMs.OnValue(func(int64) {})
	return t
}

// Head returns the latest published tracker head, 0 when none.
func (t *headTracker) Head() int64 {
	if t == nil {
		return 0
	}
	return t.head.GetValue()
}

// OnHead registers a callback for every head change seen by this replica.
func (t *headTracker) OnHead(cb func(int64)) {
	if t != nil {
		t.head.OnValue(cb)
	}
}

// IsLeader reports whether this replica currently polls.
func (t *headTracker) IsLeader() bool { return t != nil && t.isLeader.Load() }

// staleAfter is how long a replica may go without seeing a successful
// leader poll (a head or alive counter change) before it falls back:
// max(3 × block time, 3 × the longest recent interval between the leader's
// successful polls, headTrackerMinStale).
//
// The leader's own poll interval is what makes followers correct on slow
// chains: a follower never feeds the block-time EMA (only the leader sees
// block timestamps, and the 60s per-upstream pollers take many minutes to
// warm it on a 12-52s chain), so its blockTime() can sit at the 1s cold
// default while the leader legitimately polls every 12-52s. The interval is
// the difference of two consecutive alive values, both stamped by the
// leader's clock, so no clock skew enters.
func (t *headTracker) staleAfter() time.Duration {
	local := t.localStaleAfter()
	if pub := time.Duration(t.staleMs.GetValue()) * time.Millisecond; pub > 0 {
		// The leader's own window: computed from its WARM block-time EMA and
		// its poll cadence, which a follower cannot know (its EMA is cold,
		// only the leader sees block timestamps).
		return max(local, pub)
	}
	// Nothing published yet (startup before the first leader poll): be
	// conservative rather than fall back on the 5s floor.
	return max(local, headTrackerColdStale)
}

// localStaleAfter is this replica's own estimate: max(3 × block time, 3 ×
// the longest recent interval between successful leader polls,
// headTrackerMinStale).
func (t *headTracker) localStaleAfter() time.Duration {
	return max(headTrackerStaleBlocks*t.blockTime(), headTrackerStaleBlocks*time.Duration(t.maxGapNs.Load()), headTrackerMinStale)
}

// publishedStaleAfter is the window the leader publishes: its local
// estimate, or at least headTrackerColdStale while its own block time is
// still the cold default (a fresh leader right after failover).
func (t *headTracker) publishedStaleAfter() time.Duration {
	d := t.localStaleAfter()
	if t.blockTimeCold() {
		d = max(d, headTrackerColdStale)
	}
	return d
}

// blockTimeCold reports that the cadence is still the cold-start default
// (no measured EMA and no configured interval).
func (t *headTracker) blockTimeCold() bool {
	if t.cfg != nil && t.cfg.Interval != nil {
		return false
	}
	return t.deps.blockTime == nil || t.deps.blockTime() <= 0
}

// noteLivenessGap records one interval between the leader's successful polls;
// the max over the recent window drives staleAfter. Intervals longer than
// headTrackerMaxGap (a previous leader's counter from long ago, a restart)
// are not cadence and are ignored.
func (t *headTracker) noteLivenessGap(gap time.Duration) {
	if gap <= 0 || gap > headTrackerMaxGap {
		return
	}
	t.gapMu.Lock()
	t.gaps = append(t.gaps, gap)
	if len(t.gaps) > headTrackerGapSamples {
		t.gaps = t.gaps[1:]
	}
	var m time.Duration
	for _, g := range t.gaps {
		m = max(m, g)
	}
	t.gapMu.Unlock()
	t.maxGapNs.Store(int64(m))
}

// fresh reports the head when this replica saw it advance within staleAfter
// (local clock), else 0. No side effects.
func (t *headTracker) fresh() int64 {
	v := t.head.GetValue()
	at := t.advancedAtNs.Load()
	if v <= 0 || at == 0 || t.deps.now().Sub(time.Unix(0, at)) > t.staleAfter() {
		return 0
	}
	return v
}

// FallbackFloor is the last head served as fresh, while it is younger than
// headTrackerFallbackFloorTTL, else 0. The fallback path never serves below
// it, so eth_blockNumber does not go backwards when the tracker goes stale.
func (t *headTracker) FallbackFloor() int64 {
	if t == nil {
		return 0
	}
	at := t.lastFreshAtNs.Load()
	if at == 0 || t.deps.now().Sub(time.Unix(0, at)) > headTrackerFallbackFloorTTL {
		return 0
	}
	return t.lastFreshHead.Load()
}

// FreshHead returns the tracker head when it advanced recently enough to be
// served, else 0 (the caller falls back). It records fallback transitions.
func (t *headTracker) FreshHead() int64 {
	if t == nil {
		return 0
	}
	v := t.head.GetValue()
	if f := t.fresh(); f > 0 {
		if t.lastFreshHead.Load() != f {
			t.lastFreshHead.Store(f)
		}
		t.lastFreshAtNs.Store(t.deps.now().UnixNano())
		if t.inFallback.Load() && t.inFallback.Swap(false) {
			telemetry.MetricHeadTrackerFallbackActive.WithLabelValues(t.projectId, t.label).Set(0)
			t.logger.Info().Int64("head", f).Msg("head tracker head is fresh again; serving it as latest")
		}
		return f
	}
	if !t.inFallback.Load() && !t.inFallback.Swap(true) {
		telemetry.MetricHeadTrackerFallbackActive.WithLabelValues(t.projectId, t.label).Set(1)
		telemetry.MetricHeadTrackerFallbackTotal.WithLabelValues(t.projectId, t.label).Inc()
	}
	now := t.deps.now().UnixMilli()
	if last := t.lastWarnAtMs.Load(); now-last >= headTrackerWarnEvery.Milliseconds() && t.lastWarnAtMs.CompareAndSwap(last, now) {
		t.logger.Warn().Int64("head", v).Dur("staleAfter", t.staleAfter()).Bool("leader", t.isLeader.Load()).
			Msg("head tracker head is stale; latest falls back to per-upstream poller heads")
	}
	return 0
}

// blockTime is the poll cadence basis: the configured interval override, else
// the measured EMA, else the cold-start default.
func (t *headTracker) blockTime() time.Duration {
	var ema time.Duration
	if t.deps.blockTime != nil {
		ema = t.deps.blockTime()
	}
	if t.cfg != nil && t.cfg.Interval != nil {
		if d := t.cfg.Interval.Resolve(ema, common.DefaultHeadTrackerColdInterval); d > 0 {
			return d
		}
	}
	if ema > 0 {
		return ema
	}
	return common.DefaultHeadTrackerColdInterval
}

// aligned reports whether waits are aligned to block timestamps. A fixed
// interval override (no multiplier) polls on a plain timer.
func (t *headTracker) aligned() bool {
	return t.cfg == nil || t.cfg.Interval == nil || t.cfg.Interval.BlockTimeMultiplier > 0
}

func (t *headTracker) leaseTtl() time.Duration {
	if t.cfg != nil && t.cfg.LeaseTtl > 0 {
		return t.cfg.LeaseTtl.Duration()
	}
	return common.DefaultHeadTrackerLeaseTtl
}

// Start runs the election + poll loop until ctx ends or Stop is called.
func (t *headTracker) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	t.stop = cancel
	t.done = make(chan struct{})
	telemetry.MetricHeadTrackerIsLeader.WithLabelValues(t.projectId, t.label).Set(0)
	go func() {
		defer close(t.done)
		t.run(ctx)
	}()
	go t.metricsLoop(ctx)
}

// Stop ends the loop and releases the lease (if held) so another replica
// takes over immediately rather than after a TTL.
func (t *headTracker) Stop() {
	if t == nil || t.stop == nil {
		return
	}
	t.stopOnce.Do(func() {
		t.stop()
		<-t.done
	})
}

func (t *headTracker) run(ctx context.Context) {
	t.balancer = headTrackerBalancerFor(t.ssr)
	t.balancer.register(ctx, t)
	defer t.balancer.unregister(t)
	deferredSince := time.Time{}
	for ctx.Err() == nil {
		ttl := t.leaseTtl()
		// Balance: a replica at its share of leases gives others ttl to
		// take a free lease first; past that it takes it anyway.
		if t.balancer.shouldDefer() || (!deferredSince.IsZero() && time.Since(deferredSince) < ttl) {
			if deferredSince.IsZero() {
				deferredSince = time.Now()
			}
			if time.Since(deferredSince) < ttl {
				if !sleepCtx(ctx, ttl/5) {
					return
				}
				continue
			}
		}
		deferredSince = time.Time{}
		lease, err := t.ssr.AcquireLease(ctx, t.leaseKey, ttl)
		if err != nil {
			if errors.Is(err, data.ErrLeaseUnsupported) {
				t.logger.Error().Err(err).Msg("head tracker needs a redis (or memory) sharedState connector; it will not run")
				return
			}
			t.logger.Debug().Err(err).Msg("head tracker lease acquisition failed")
		}
		if lease != nil {
			t.lead(ctx, lease, ttl)
			// Do not immediately re-acquire what was just released (handover,
			// step-down): give other replicas a full TTL first.
			deferredSince = time.Now()
			if !sleepCtx(ctx, ttl/5) {
				return
			}
			continue
		}
		// Follower: check again well within one TTL so takeover after a
		// leader dies costs at most ttl + ttl/5.
		if !sleepCtx(ctx, ttl/5) {
			return
		}
	}
}

// lead holds the lease: a renewer keeps it alive every ttl/3 and ends the
// session when it is lost; the poll loop runs inside the session.
func (t *headTracker) lead(ctx context.Context, lease data.Lease, ttl time.Duration) {
	session, cancel := context.WithCancel(ctx)
	defer cancel()
	hold := ttl * 2 / 3
	t.leaseDeadlineNs.Store(t.deps.now().Add(hold).UnixNano())
	t.isLeader.Store(true)
	telemetry.MetricHeadTrackerIsLeader.WithLabelValues(t.projectId, t.label).Set(1)
	t.logger.Info().Str("instance", t.ssr.InstanceId()).Msg("head tracker acquired leadership")
	defer func() {
		t.isLeader.Store(false)
		telemetry.MetricHeadTrackerIsLeader.WithLabelValues(t.projectId, t.label).Set(0)
		if !t.abandonLease.Load() {
			rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = lease.Release(rctx)
			rcancel()
		}
		t.prev = nil
		t.staleStreak = 0
		t.logger.Info().Msg("head tracker released leadership")
	}()

	// Watchdog (S4): the session ends the moment the hard deadline
	// (last successful renew + 2·ttl/3) passes, even while a renew call is
	// still in flight, so this replica stops polling before another one can
	// legitimately acquire the expired lease.
	go func() {
		defer cancel()
		for {
			d := time.Until(time.Unix(0, t.leaseDeadlineNs.Load()))
			if d <= 0 {
				if session.Err() == nil {
					t.logger.Warn().Msg("head tracker cannot prove it still holds the lease; stepping down")
				}
				return
			}
			if !sleepCtx(session, d) {
				return
			}
		}
	}()
	// Renewer: every ttl/3. The deadline extends from the instant the renew
	// was SENT (conservative: Redis applied the new expiry no earlier).
	go func() {
		defer cancel()
		for sleepCtx(session, ttl/3) {
			sentAt := t.deps.now()
			rctx, rcancel := context.WithDeadline(session, time.Unix(0, t.leaseDeadlineNs.Load()))
			ok, err := lease.Renew(rctx, ttl)
			rcancel()
			if err == nil && !ok {
				t.logger.Warn().Msg("head tracker lease lost to another replica")
				return
			}
			if err == nil {
				t.leaseDeadlineNs.Store(sentAt.Add(hold).UnixNano())
			}
		}
	}()

	// Hand over: while this replica leads more than its share, release this
	// lease once it has been held for a while, but only one network at a time
	// per process (handoverMu) so leadership moves gradually.
	go func() {
		tk := time.NewTicker(headTrackerHandoverCheck)
		defer tk.Stop()
		heldSince := time.Now()
		for {
			select {
			case <-session.Done():
				return
			case <-tk.C:
			}
			if time.Since(heldSince) < headTrackerHandoverMinHold || !t.balancer.overShare() {
				continue
			}
			if !handoverMu.TryLock() {
				continue
			}
			t.logger.Info().Msg("head tracker handing over leadership to balance replicas")
			// Released in lead's deferred cleanup; keep the process-wide
			// handover slot until other replicas had a chance to take it.
			go func() {
				time.Sleep(2 * ttl)
				handoverMu.Unlock()
			}()
			cancel()
			return
		}
	}()
	t.pollLoop(session)
}

// handoverMu allows one lease handover at a time per process.
var handoverMu sync.Mutex

const (
	headTrackerHandoverCheck   = 10 * time.Second
	headTrackerHandoverMinHold = 30 * time.Second
)

// headTrackerMaxInFlight bounds concurrent leader polls per network.
const headTrackerMaxInFlight = 2

// pollLoop schedules polls by the CLOCK, not by the previous poll's return:
// the next poll fires at the scheduled time even if the previous one is still
// running (at most headTrackerMaxInFlight in flight), so poll latency close to
// the block time no longer caps the cadence at 1/(latency + wait). Each poll's
// result is processed (tick) under procMu, so leader-local state stays
// single-threaded; the cache/blockstore write and publish happen inside tick
// (S1 ordering per block) but do not delay the next scheduled poll.
func (t *headTracker) pollLoop(session context.Context) {
	sem := make(chan struct{}, headTrackerMaxInFlight)
	var wg sync.WaitGroup
	defer wg.Wait()
	var failures atomic.Int32
	nextAt := make(chan time.Duration, headTrackerMaxInFlight)
	wait := time.Duration(0)
	for session.Err() == nil {
		if !sleepCtx(session, wait) {
			return
		}
		select {
		case sem <- struct{}{}:
		case <-session.Done():
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			w, err := t.tick(session)
			if err != nil {
				n := failures.Add(1)
				if session.Err() == nil {
					t.logger.Debug().Err(err).Int32("consecutiveFailures", n).Msg("head tracker poll failed")
				}
			} else {
				failures.Store(0)
			}
			select {
			case nextAt <- w:
			default:
			}
		}()
		if failures.Load() >= headTrackerMaxPollFailures {
			t.logger.Warn().Int32("consecutiveFailures", failures.Load()).Msg("head tracker leader stepping down after repeated poll failures")
			return
		}
		// Next poll: as soon as a poll result proposes a time; if the poll
		// is still running when one block time has passed, fire the next one
		// anyway (bounded by the semaphore), so a slow poll never delays the
		// schedule past the expected next block.
		bt := t.blockTime()
		select {
		case w := <-nextAt:
			wait = w
		case <-time.After(max(bt, common.MinHeadTrackerPollWait)):
			wait = 0
		case <-session.Done():
			return
		}
	}
}

// tick performs one leader poll and returns how long to wait before the next.
func (t *headTracker) tick(ctx context.Context) (time.Duration, error) {
	bt := t.blockTime()
	retry := max(common.MinHeadTrackerPollWait, bt/4)
	full := t.cfg != nil && t.cfg.FullBlocks

	start := t.deps.now()
	obs, err := t.deps.poll(ctx, full)
	// Polls may overlap (pollLoop); everything below mutates leader-local
	// state and must run one result at a time.
	t.procMu.Lock()
	defer t.procMu.Unlock()
	end := t.deps.now()
	// Schedule and propagation delay are measured from when the poll was
	// SENT: the answer reflects the chain at that instant, and measuring from
	// the response would fold the poll's own latency (CPU throttling, large
	// fullBlocks payloads) into every wait, capping the cadence at
	// 1/(latency + block time). The returned wait is then relative to `end`.
	now := start
	elapsed := end.Sub(start)
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	telemetry.MetricHeadTrackerPollDuration.WithLabelValues(t.projectId, t.label, outcome).Observe(elapsed.Seconds())
	if err != nil {
		return retry, err
	}
	if obs == nil || obs.Number <= 0 {
		// null "latest": the routed upstream is behind; try again shortly.
		return retry, nil
	}

	current := t.head.GetValue()
	reason := t.reject(ctx, obs, current, now, bt)
	if reason == "regression" && t.recoverFromPoisonedHead(ctx, obs, current) {
		reason = ""
	} else if reason != "regression" {
		t.regressStreak, t.regressLast = 0, 0
	}
	if reason != "" {
		telemetry.MetricHeadTrackerRejectedTotal.WithLabelValues(t.projectId, t.label, reason).Inc()
		t.logger.Warn().Int64("observed", obs.Number).Int64("current", current).Str("reason", reason).
			Str("upstreamId", upstreamIdOf(obs.Upstream)).Msg("head tracker rejected a polled head")
		return retry, nil
	}

	recovering := current-obs.Number > common.DefaultToleratedBlockHeadRollback
	if obs.Number < current && !recovering {
		// A slightly lagging upstream answered: the next block is not out
		// yet. The poll succeeded and the published head is still correct.
		t.publishAlive(ctx)
		return sinceSent(t.staleWait(obs, now, bt), elapsed), nil
	}
	if obs.Number == current {
		t.publishAlive(ctx)
		// Same height. A different hash is a same-height reorg (S2): the
		// cached block and the block store entry for this height are
		// orphaned, so rewrite them with the new canonical block. The
		// published number does not change.
		if p := t.prev; p != nil && p.Number == obs.Number && p.Hash != "" && obs.Hash != "" && p.Hash != obs.Hash {
			t.logger.Info().Int64("number", obs.Number).Str("orphan", p.Hash).Str("canonical", obs.Hash).
				Msg("head tracker observed a same-height reorg; replacing the cached head block")
			if t.deps.onAccepted != nil {
				t.deps.onAccepted(ctx, obs)
			}
			t.prev = obs
		} else if t.prev == nil {
			t.prev, t.prevAt = obs, now
		}
		return sinceSent(t.staleWait(obs, now, bt), elapsed), nil
	}

	// New head. Write the polled block to the cache / block store FIRST
	// (S1), then publish: a follower that learns the new head must find its
	// block already written, or its rewritten "latest" read would miss and
	// go upstream.
	if t.deps.onAccepted != nil {
		t.deps.onAccepted(ctx, obs)
	}
	if !t.holdsLease() || ctx.Err() != nil {
		// The lease may have passed to another replica while writing: never
		// publish without it (S4).
		return retry, ctx.Err()
	}
	t.advancedAtNs.Store(t.deps.now().UnixNano())
	t.head.TryUpdate(ctx, obs.Number)
	t.publishAlive(ctx)
	if obs.Timestamp > 0 {
		delay := now.Sub(time.Unix(obs.Timestamp, 0))
		if delay >= 0 && delay <= 4*max(bt, time.Second) {
			t.delays = append(t.delays, delay)
			if len(t.delays) > headTrackerDelaySamples {
				t.delays = t.delays[1:]
			}
			telemetry.MetricHeadTrackerPropagationDelay.WithLabelValues(t.projectId, t.label).Observe(delay.Seconds())
		}
	}
	t.prev, t.prevAt = obs, now
	t.staleStreak = 0
	return sinceSent(t.newHeadWait(obs, now, bt), elapsed), nil
}

// sinceSent converts a wait measured from when a poll was sent into the
// remaining wait after it returned (elapsed later).
func sinceSent(wait, elapsed time.Duration) time.Duration {
	return max(wait-elapsed, 0)
}

// recoverFromPoisonedHead decides whether a poll that "regresses" more than
// the rollback tolerance below the published head is in fact the truth: the
// published head was poisoned (a bogus far-future value accepted before any
// guard could compare it with anything, or a wrong-chain value) and every
// honest poll now looks like a regression. After headTrackerRecoverAfter
// consecutive regressing polls that are mutually consistent (each at or
// slightly above the previous, i.e. a chain moving forward from far below
// the published head), and whose serving upstream passes the chain-id check,
// the rollback is accepted. Counted as rejected reason="recovered_rollback".
func (t *headTracker) recoverFromPoisonedHead(ctx context.Context, obs *headObservation, current int64) bool {
	consistent := t.regressStreak > 0 && obs.Number >= t.regressLast && obs.Number-t.regressLast <= t.deps.majorMove()
	if consistent {
		t.regressStreak++
	} else {
		t.regressStreak = 1
	}
	t.regressLast = obs.Number
	if t.regressStreak < headTrackerRecoverAfter {
		return false
	}
	// A STUCK node on the right chain also polls consistently far below the
	// published head. Only a head that is current (its block timestamp
	// within max(5 block times, 60s) of now) can be the chain's real tip.
	if obs.Timestamp <= 0 {
		return false
	}
	recent := max(5*t.blockTime(), time.Minute)
	if age := t.deps.now().Sub(time.Unix(obs.Timestamp, 0)); age > recent || age < -headTrackerFutureSlack {
		return false
	}
	if !t.chainIdOk(ctx, obs) {
		return false
	}
	t.regressStreak, t.regressLast = 0, 0
	telemetry.MetricHeadTrackerRejectedTotal.WithLabelValues(t.projectId, t.label, "recovered_rollback").Inc()
	t.logger.Error().Int64("published", current).Int64("observed", obs.Number).Int("consecutivePolls", headTrackerRecoverAfter).
		Msg("head tracker published head was far ahead of every consistent poll; accepting the rollback")
	return true
}

// chainIdOk verifies the serving upstream is on this network's chain.
func (t *headTracker) chainIdOk(ctx context.Context, obs *headObservation) bool {
	if t.deps.verifyChainId == nil || obs.Upstream == nil {
		return true
	}
	vctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ok, err := t.deps.verifyChainId(vctx, obs.Upstream)
	return err == nil && ok
}

// holdsLease reports whether the hard lease deadline has not passed.
func (t *headTracker) holdsLease() bool {
	return t.deps.now().UnixNano() < t.leaseDeadlineNs.Load()
}

// reject applies the agreement-free sanity guards; "" accepts.
func (t *headTracker) reject(ctx context.Context, obs *headObservation, current int64, now time.Time, bt time.Duration) string {
	if current > 0 && current-obs.Number > common.DefaultToleratedBlockHeadRollback {
		return "regression"
	}
	if obs.Timestamp > 0 && time.Unix(obs.Timestamp, 0).After(now.Add(headTrackerFutureSlack)) {
		return "far_future"
	}
	if p := t.prev; p != nil && p.Timestamp > 0 && obs.Timestamp >= p.Timestamp && obs.Number > p.Number && bt > 0 {
		// The chain cannot produce blocks much faster than its measured rate:
		// allow 4× the blocks the timestamps account for (+1s for whole-second
		// timestamps) plus a fixed slack.
		elapsed := time.Duration(obs.Timestamp-p.Timestamp+1) * time.Second
		if allowed := 4*int64(elapsed/bt) + 16; obs.Number-p.Number > allowed {
			return "far_future"
		}
	}
	// The first head this leader accepts is compared with nothing (the
	// counter may be empty, or seeded by another leader), and a major jump
	// could be another chain's height: both need the serving upstream's
	// chain id to match (S3).
	firstAccept := t.prev == nil && (current == 0 || obs.Number > current)
	if (firstAccept || (current > 0 && obs.Number-current > t.deps.majorMove())) && !t.chainIdOk(ctx, obs) {
		return "chain_id"
	}
	return ""
}

func (t *headTracker) meanDelay() time.Duration {
	if len(t.delays) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range t.delays {
		sum += d
	}
	return sum / time.Duration(len(t.delays))
}

// newHeadWait schedules the poll after a new head: at the expected timestamp
// of the next block plus the mean propagation delay, never sooner than half a
// block (or the floor) and never later than one block plus that delay.
func (t *headTracker) newHeadWait(obs *headObservation, now time.Time, bt time.Duration) time.Duration {
	lo := max(common.MinHeadTrackerPollWait, bt/2)
	if !t.aligned() || obs.Timestamp <= 0 {
		return max(common.MinHeadTrackerPollWait, bt)
	}
	md := t.meanDelay()
	target := time.Unix(obs.Timestamp, 0).Add(bt + md)
	return clampDuration(target.Sub(now), lo, bt+md)
}

// staleWait schedules the retry after an unchanged head. While the next
// block is still expected (prev timestamp + block time + delay in the
// future) it waits for it. Once that moment has passed, consecutive
// same-head results back off exponentially instead of retrying at a fixed
// quarter block: on-demand / irregular chains (redbelly ~one block per
// minutes, nibiru, filecoin null rounds) otherwise burn hundreds of polls per
// block, and a cold block time (EMA not warm: on-demand chains rarely
// produce the samples it needs, and samples > 120s are rejected) made the
// retry a flat 500ms.
//
//	warm EMA: base max(500ms, bt/4), doubling, capped at max(bt, min(4·bt, 60s))
//	cold:     base 1s, doubling, capped at 30s
//
// The streak resets on every new head, so a regular chain is unaffected.
func (t *headTracker) staleWait(obs *headObservation, now time.Time, bt time.Duration) time.Duration {
	if !t.aligned() {
		return max(common.MinHeadTrackerPollWait, bt)
	}
	if p := t.prev; p != nil && p.Timestamp > 0 {
		target := time.Unix(p.Timestamp, 0).Add(bt + t.meanDelay())
		if target.After(now) {
			return clampDuration(target.Sub(now), common.MinHeadTrackerPollWait, bt)
		}
	}
	base, ceiling := max(common.MinHeadTrackerPollWait, bt/4), max(bt, min(4*bt, time.Minute))
	if t.blockTimeCold() {
		base, ceiling = time.Second, 30*time.Second
	}
	wait := base << min(t.staleStreak, 16)
	t.staleStreak++
	return min(wait, ceiling)
}

// metricsLoop exports the head and the served lag once a second.
func (t *headTracker) metricsLoop(ctx context.Context) {
	tk := time.NewTicker(time.Second)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
		}
		h := t.head.GetValue()
		if h <= 0 {
			continue
		}
		telemetry.MetricHeadTrackerHeadBlock.WithLabelValues(t.projectId, t.label).Set(float64(h))
		served := t.FreshHead()
		if served == 0 && t.deps.fallbackHead != nil {
			served = t.deps.fallbackHead(ctx)
		}
		lag := h - served
		if served <= 0 || lag < 0 {
			lag = 0
		}
		telemetry.MetricHeadTrackerServedLagBlocks.WithLabelValues(t.projectId, t.label).Set(float64(lag))
	}
}

func clampDuration(d, lo, hi time.Duration) time.Duration {
	if hi < lo {
		hi = lo
	}
	return min(max(d, lo), hi)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-tm.C:
		return true
	}
}

func upstreamIdOf(u common.Upstream) string {
	if u == nil {
		return ""
	}
	return u.Id()
}

// parseHeadObservation extracts number, hash and timestamp from a block.
func parseHeadObservation(raw json.RawMessage) (*headObservation, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return nil, nil
	}
	var b struct {
		Number    string `json:"number"`
		Hash      string `json:"hash"`
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(raw, &b); err != nil {
		return nil, fmt.Errorf("decode latest block: %w", err)
	}
	n, err := common.HexToInt64(b.Number)
	if err != nil || n <= 0 {
		return nil, fmt.Errorf("latest block has invalid number %q", b.Number)
	}
	obs := &headObservation{Number: n, Hash: strings.ToLower(b.Hash), Raw: raw}
	if b.Timestamp != "" {
		if ts, err := common.HexToInt64(b.Timestamp); err == nil {
			obs.Timestamp = ts
		}
	}
	return obs, nil
}

// upstreamCheck is the last tip head check of one upstream on this replica.
type upstreamCheck struct {
	trackedAt int64 // tracker head when the check ran
	head      int64
}

// checkUpstreamHead returns u's head, asking it with eth_blockNumber at most
// once per (upstream, tracker head) on this replica. Concurrent callers for
// the same upstream share one call. The answer is fed into u's shared head
// counter (SuggestLatestBlock), so the poller, the lag metrics and the other
// replicas see it too. Returns the known head on any error.
func (t *headTracker) checkUpstreamHead(ctx context.Context, u common.EvmUpstream, tracked int64, projectId, label string) int64 {
	sp := u.EvmStatePoller()
	id := u.Id()
	if v, ok := t.checks.Load(id); ok {
		if c := v.(*upstreamCheck); c.trackedAt >= tracked {
			return max(c.head, sp.LatestBlock())
		}
	}
	key := fmt.Sprintf("%s@%d", id, tracked)
	v, _, _ := t.checkGroup.Do(key, func() (interface{}, error) {
		if c, ok := t.checks.Load(id); ok && c.(*upstreamCheck).trackedAt >= tracked {
			return c.(*upstreamCheck).head, nil
		}
		telemetry.MetricHeadTrackerTipChecksTotal.WithLabelValues(projectId, label, id).Inc()
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		head, err := fetchUpstreamBlockNumber(cctx, u)
		if err != nil {
			head = 0
		} else if head > sp.LatestBlock() {
			sp.SuggestLatestBlock(head)
		}
		t.checks.Store(id, &upstreamCheck{trackedAt: tracked, head: head})
		return head, nil
	})
	return max(v.(int64), sp.LatestBlock())
}

// fetchUpstreamBlockNumber asks one upstream for eth_blockNumber directly.
func fetchUpstreamBlockNumber(ctx context.Context, u common.EvmUpstream) (int64, error) {
	rq := common.NewNormalizedRequest([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`))
	resp, err := u.Forward(ctx, rq, true, false)
	if resp != nil {
		defer resp.Release()
	}
	if err != nil {
		return 0, err
	}
	jrr, err := resp.JsonRpcResponse(ctx)
	if err != nil {
		return 0, err
	}
	if jrr.Error != nil {
		return 0, jrr.Error
	}
	var hex string
	if err := json.Unmarshal(jrr.GetResultBytes(), &hex); err != nil {
		return 0, err
	}
	return common.HexToInt64(hex)
}

// publishAlive records a successful leader poll in the shared liveness
// counter (unix ms, monotonic), so followers keep the head fresh while the
// chain itself does not move. Only while the lease is provably held.
func (t *headTracker) publishAlive(ctx context.Context) {
	if !t.holdsLease() {
		return
	}
	now := t.deps.now()
	// The leader knows its own poll succeeded: fresh locally at once (its
	// first head may only seed the counter callbacks).
	t.advancedAtNs.Store(now.UnixNano())
	// Publish the leader's own staleness window first (followers apply it
	// to the alive value that follows). Only on a >10% change, so steady
	// state adds no writes.
	sa := t.publishedStaleAfter().Milliseconds()
	if pub := t.staleMs.GetValue(); pub <= 0 || sa > pub*11/10 || sa < pub*9/10 {
		t.staleMs.TryUpdate(ctx, sa)
	}
	t.alive.TryUpdate(ctx, now.UnixMilli())
}
