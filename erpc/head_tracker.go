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
	return &headTracker{
		projectId: projectId,
		networkId: networkId,
		label:     label,
		cfg:       cfg,
		ssr:       ssr,
		head:      ssr.GetCounterInt64(data.CounterValueSchemaVersion+"/headTracker/"+scope, common.DefaultToleratedBlockHeadRollback),
		leaseKey:  "headTracker/" + scope,
		deps:      deps,
		logger:    &lg,
	}
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

// staleAfter is how long the head may go without advancing before replicas
// fall back: max(3 × block time, headTrackerMinStale).
func (t *headTracker) staleAfter() time.Duration {
	return max(headTrackerStaleBlocks*t.blockTime(), headTrackerMinStale)
}

// FreshHead returns the tracker head when it advanced recently enough to be
// served, else 0 (the caller falls back). It records fallback transitions.
func (t *headTracker) FreshHead() int64 {
	if t == nil {
		return 0
	}
	v := t.head.GetValue()
	if v > 0 && !t.head.IsStale(t.staleAfter()) {
		if t.inFallback.Load() && t.inFallback.Swap(false) {
			telemetry.MetricHeadTrackerFallbackActive.WithLabelValues(t.projectId, t.label).Set(0)
			t.logger.Info().Int64("head", v).Msg("head tracker head is fresh again; serving it as latest")
		}
		return v
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
	for ctx.Err() == nil {
		ttl := t.leaseTtl()
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
		t.logger.Info().Msg("head tracker released leadership")
	}()

	go func() {
		defer cancel()
		lastOk := t.deps.now()
		for sleepCtx(session, ttl/3) {
			rctx, rcancel := context.WithTimeout(session, ttl/3)
			ok, err := lease.Renew(rctx, ttl)
			rcancel()
			if err == nil && !ok {
				t.logger.Warn().Msg("head tracker lease lost to another replica")
				return
			}
			if err == nil {
				lastOk = t.deps.now()
				continue
			}
			if t.deps.now().Sub(lastOk) >= ttl*2/3 {
				// Cannot prove we still hold it: stop polling before another
				// replica can legitimately take over.
				t.logger.Warn().Err(err).Msg("head tracker cannot renew its lease; stepping down")
				return
			}
		}
	}()

	failures := 0
	for session.Err() == nil {
		wait, err := t.tick(session)
		if err != nil {
			failures++
			if session.Err() == nil {
				t.logger.Debug().Err(err).Int("consecutiveFailures", failures).Msg("head tracker poll failed")
			}
			if failures >= headTrackerMaxPollFailures {
				t.logger.Warn().Err(err).Int("consecutiveFailures", failures).Msg("head tracker leader stepping down after repeated poll failures")
				return
			}
		} else {
			failures = 0
		}
		if !sleepCtx(session, wait) {
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
	now := t.deps.now()
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	telemetry.MetricHeadTrackerPollDuration.WithLabelValues(t.projectId, t.label, outcome).Observe(now.Sub(start).Seconds())
	if err != nil {
		return retry, err
	}
	if obs == nil || obs.Number <= 0 {
		// null "latest": the routed upstream is behind; try again shortly.
		return retry, nil
	}

	current := t.head.GetValue()
	if reason := t.reject(ctx, obs, current, now, bt); reason != "" {
		telemetry.MetricHeadTrackerRejectedTotal.WithLabelValues(t.projectId, t.label, reason).Inc()
		t.logger.Warn().Int64("observed", obs.Number).Int64("current", current).Str("reason", reason).
			Str("upstreamId", upstreamIdOf(obs.Upstream)).Msg("head tracker rejected a polled head")
		return retry, nil
	}

	if obs.Number <= current {
		// Same (or a slightly lagging) head: the next block is not out yet.
		return t.staleWait(obs, now, bt), nil
	}

	// New head: publish first (followers serve it), then the side effects.
	t.head.TryUpdate(ctx, obs.Number)
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
	if t.deps.onAccepted != nil {
		t.deps.onAccepted(ctx, obs)
	}
	return t.newHeadWait(obs, now, bt), nil
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
	if current > 0 && obs.Number-current > t.deps.majorMove() && t.deps.verifyChainId != nil && obs.Upstream != nil {
		vctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		ok, err := t.deps.verifyChainId(vctx, obs.Upstream)
		cancel()
		if err != nil || !ok {
			return "chain_id"
		}
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

// staleWait schedules the retry after an unchanged head: wait for the expected
// next block if it is still ahead, else retry at a quarter block.
func (t *headTracker) staleWait(obs *headObservation, now time.Time, bt time.Duration) time.Duration {
	retry := max(common.MinHeadTrackerPollWait, bt/4)
	if !t.aligned() {
		return max(common.MinHeadTrackerPollWait, bt)
	}
	p := t.prev
	if p == nil || p.Timestamp <= 0 {
		return retry
	}
	target := time.Unix(p.Timestamp, 0).Add(bt + t.meanDelay())
	if target.After(now) {
		return clampDuration(target.Sub(now), common.MinHeadTrackerPollWait, bt)
	}
	return retry
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
