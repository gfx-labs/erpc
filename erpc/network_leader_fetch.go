package erpc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erpc/erpc/architecture/evm"
	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/data"
	"github.com/erpc/erpc/health"
	"github.com/erpc/erpc/telemetry"
)

// leaderFetch is a network's opt-in leader fetch (evm.chainTracker.leaderFetch):
// the instance that holds the network's lease runs the block clock's head
// fetches as usual and publishes every head its intake passed; the others
// (followers) take each published head through their own intake as that
// upstream's vote, as a push (evm.HeaderLeader), and their clocks fetch
// nothing while the leader's heads stay fresh. A follower whose last
// leader head is older than staleAfter runs its own clock again, and tries
// for the lease on every pass, so a dead leader is replaced within the
// lease TTL. Each head carries the lease's fencing epoch: a follower drops
// heads of an epoch below the highest it saw, a deposed leader's.
//
// Followers that have demand (a running clock, by their reads) send a
// demand message at most every W/2; the leader marks its lanes read on
// it, so its clock runs for readers it does not serve itself.
type leaderFetch struct {
	n          *Network
	conn       data.Connector
	bus        data.BlobPubSub
	leaseKey   string
	channel    string
	instanceId string
	leaseTtl   time.Duration
	minStale   time.Duration

	mu    sync.Mutex
	lease data.Lease

	leading   atomic.Bool
	epoch     atomic.Int64 // own epoch while leading
	maxEpoch  atomic.Int64 // highest epoch seen on the channel
	lastHead  atomic.Int64 // local receive time of the last accepted leader head, unix ns
	demandAt  atomic.Int64 // when this follower last sent demand, unix ns
	stopped   atomic.Bool
	noRelease bool // tests: a crashed leader never releases its lease
}

// leaderMsg is one message on the network's leader channel.
type leaderMsg struct {
	Kind     string          `json:"k"`           // "head" or "demand"
	Epoch    int64           `json:"e,omitempty"` // the leader's fencing epoch (head)
	Upstream string          `json:"u,omitempty"` // whose head (head)
	Header   json.RawMessage `json:"h,omitempty"` // the header as fetched (head)
	From     string          `json:"f,omitempty"` // sender instance, diagnostics
}

// newLeaderFetch is n's leader fetch, nil when it is off or the shared
// state cannot hold a lease or fan out messages (logged).
func newLeaderFetch(n *Network) *leaderFetch {
	if n.cfg == nil || n.cfg.Evm == nil || n.cfg.Evm.ChainTracker == nil {
		return nil
	}
	cfg := n.cfg.Evm.ChainTracker.LeaderFetch
	if cfg == nil || !cfg.Enabled {
		return nil
	}
	if n.upstreamsRegistry == nil || n.upstreamsRegistry.SharedStateRegistry() == nil {
		n.logger.Warn().Msg("evm.chainTracker.leaderFetch is enabled but no shared state is configured; every instance runs its own block clock")
		return nil
	}
	ssr := n.upstreamsRegistry.SharedStateRegistry()
	conn := ssr.Connector()
	bus, ok := conn.(data.BlobPubSub)
	if !ok || !data.SupportsLeases(conn) || !data.SupportsBlobPubSub(conn) {
		n.logger.Warn().Msg("evm.chainTracker.leaderFetch is enabled but the shared state connector cannot grant leases and fan out messages (use redis); every instance runs its own block clock")
		return nil
	}
	ttl, minStale := cfg.LeaseTtl.Duration(), cfg.MinStaleAfter.Duration()
	if ttl <= 0 {
		ttl = common.DefaultLeaderFetchLeaseTtl
	}
	if minStale <= 0 {
		minStale = common.DefaultLeaderFetchMinStaleAfter
	}
	base := fmt.Sprintf("%s/leaderFetch/%s/%s", ssr.ClusterKey(), n.projectId, n.networkId)
	return &leaderFetch{
		n:          n,
		conn:       conn,
		bus:        bus,
		leaseKey:   base + "/lease",
		channel:    base + "/heads",
		instanceId: ssr.InstanceId(),
		leaseTtl:   ttl,
		minStale:   minStale,
	}
}

// staleAfter is how long a follower waits for the leader's next head:
// max(3 x block time, minStaleAfter).
func (l *leaderFetch) staleAfter() time.Duration {
	return max(3*l.n.EvmBlockTime(), l.minStale)
}

// followUntil is until when this instance's clocks follow the leader:
// zero while it leads or the leader's last head is stale.
func (l *leaderFetch) followUntil(now time.Time) time.Time {
	if l == nil || l.leading.Load() || l.stopped.Load() {
		return time.Time{}
	}
	at := l.lastHead.Load()
	if at == 0 {
		return time.Time{}
	}
	until := time.Unix(0, at).Add(l.staleAfter())
	if !now.Before(until) {
		return time.Time{}
	}
	return until
}

// EvmClockLeaderUntil is evm.ChainTracker's view of the leader fetch:
// until when its clock fetches nothing (followUntil).
func (n *Network) EvmClockLeaderUntil() time.Time {
	return n.leader.followUntil(time.Now())
}

// run holds or seeks the lease, reads the channel and sends demand until
// ctx ends.
func (l *leaderFetch) run(ctx context.Context) {
	msgs, stop, err := l.bus.SubscribeBlob(ctx, l.channel)
	if err != nil {
		l.n.logger.Warn().Err(err).Msg("leader fetch cannot subscribe to its channel; every head is fetched locally")
		return
	}
	defer stop()
	defer l.resign()
	tick := time.NewTicker(l.leaseTtl / 3)
	defer tick.Stop()
	l.pass(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case raw := <-msgs:
			l.receive(raw)
		case <-tick.C:
			l.pass(ctx)
		}
	}
}

// pass renews the lease while leading, else tries for it, and sends a
// follower's demand.
func (l *leaderFetch) pass(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, l.leaseTtl/3)
	defer cancel()
	l.mu.Lock()
	lease := l.lease
	l.mu.Unlock()
	if lease != nil {
		ok, err := lease.Renew(cctx, l.leaseTtl)
		if ok {
			return
		}
		if err != nil {
			// Unknown: the lease may still be ours, or expire before the
			// store answers. Stand down: a gap of one pass costs a head,
			// two leaders cost every follower a fencing drop.
			l.n.logger.Warn().Err(err).Msg("leader fetch could not renew its lease; standing down")
		}
		l.setLease(nil, "lost")
	}
	// A follower fed by a fresh leader still tries: SET NX fails at once
	// while the lease is held, and takes it the moment it expires.
	if lease, err := data.AcquireLease(cctx, l.conn, l.leaseKey, l.leaseTtl); err == nil && lease != nil {
		l.setLease(lease, "acquired")
		return
	}
	l.sendDemand(ctx)
}

func (l *leaderFetch) setLease(lease data.Lease, event string) {
	l.mu.Lock()
	l.lease = lease
	l.mu.Unlock()
	leading := lease != nil
	if leading {
		l.epoch.Store(lease.Epoch())
		for {
			cur := l.maxEpoch.Load()
			if lease.Epoch() <= cur || l.maxEpoch.CompareAndSwap(cur, lease.Epoch()) {
				break
			}
		}
	}
	if l.leading.Swap(leading) != leading {
		role := 0.0
		if leading {
			role = 1
		}
		telemetry.MetricLeaderFetchRole.WithLabelValues(l.n.projectId, l.n.Label()).Set(role)
		telemetry.MetricLeaderFetchTransitionsTotal.WithLabelValues(l.n.projectId, l.n.Label(), event).Inc()
		l.n.logger.Info().Str("event", event).Int64("epoch", l.epoch.Load()).Msg("leader fetch role changed")
		// The clocks look at the role again now: a new leader starts
		// fetching, a deposed one stops at its next pass.
		evm.DemandChanged(l.n)
	}
}

// resign gives the lease up as the network stops, so another instance
// takes over at once.
func (l *leaderFetch) resign() {
	l.stopped.Store(true)
	l.mu.Lock()
	lease := l.lease
	l.lease = nil
	l.mu.Unlock()
	l.leading.Store(false)
	if lease != nil && !l.noRelease {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = lease.Release(ctx)
	}
}

// sendDemand tells the leader this follower's clock has demand, at most
// every W/2.
func (l *leaderFetch) sendDemand(ctx context.Context) {
	t := l.n.chainTracker
	if t == nil || l.leading.Load() || !t.ClockRuns() {
		return
	}
	now := time.Now()
	every := health.FreshnessWindow(l.n.EvmBlockTime()) / 2
	if prev := l.demandAt.Load(); prev != 0 && now.Sub(time.Unix(0, prev)) < every {
		return
	}
	l.demandAt.Store(now.UnixNano())
	payload, _ := json.Marshal(leaderMsg{Kind: "demand", From: l.instanceId})
	_ = l.bus.PublishBlob(ctx, l.channel, payload)
}

// published takes a head this instance's intake passed from one of its own
// fetches of upstreamId's latest; the leader publishes it.
func (l *leaderFetch) published(upstreamId string, sh evm.SourceHeader) {
	if l == nil || !l.leading.Load() {
		return
	}
	payload, err := json.Marshal(leaderMsg{Kind: "head", Epoch: l.epoch.Load(), Upstream: upstreamId, Header: sh.Raw(), From: l.instanceId})
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(l.n.appCtx, l.leaseTtl/3)
	defer cancel()
	outcome := "published"
	if err := l.bus.PublishBlob(ctx, l.channel, payload); err != nil {
		outcome = "publish_failed"
	}
	telemetry.MetricLeaderFetchHeadsTotal.WithLabelValues(l.n.projectId, l.n.Label(), outcome).Inc()
}

// receive takes one channel message.
func (l *leaderFetch) receive(raw []byte) {
	var m leaderMsg
	if err := json.Unmarshal(raw, &m); err != nil {
		return
	}
	switch m.Kind {
	case "demand":
		if l.leading.Load() {
			evm.MarkReadAll(l.n)
		}
	case "head":
		l.head(m)
	}
}

// head takes a leader's head: fenced by epoch, then through this
// instance's intake as m.Upstream's vote, as a push.
func (l *leaderFetch) head(m leaderMsg) {
	count := func(outcome string) {
		telemetry.MetricLeaderFetchHeadsTotal.WithLabelValues(l.n.projectId, l.n.Label(), outcome).Inc()
	}
	if l.leading.Load() && m.Epoch == l.epoch.Load() {
		return // our own
	}
	for {
		cur := l.maxEpoch.Load()
		if m.Epoch < cur {
			count("fenced")
			return
		}
		if m.Epoch == cur || l.maxEpoch.CompareAndSwap(cur, m.Epoch) {
			break
		}
	}
	if l.leading.Load() {
		// A newer epoch than ours: another instance holds the lease now.
		l.setLease(nil, "lost")
	}
	in := l.n.intake
	u := in.upstream(m.Upstream)
	if u == nil {
		count("unknown_upstream")
		return
	}
	sh, err := evm.ParseSourceHeader(m.Header)
	if err != nil {
		count("malformed")
		return
	}
	now := time.Now()
	if !in.Polled(m.Upstream, sh, nil, evm.HeaderLeader, now) {
		count("rejected")
		return
	}
	// The head meets the latest part of the upstream's own poll, as a
	// push does: a follower's poller sends no latest fetch of its own
	// while the leader feeds it.
	if p, ok := u.EvmStatePoller().(*evm.EvmStatePoller); ok && !p.IsObjectNull() {
		p.Pushed(now)
	}
	l.lastHead.Store(now.UnixNano())
	count("accepted")
}
