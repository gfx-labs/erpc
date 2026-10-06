package erpc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/data"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// fakeChain is a deterministic chain producing one block per blockTime, with
// timestamps in whole seconds like a real chain.
type fakeChain struct {
	start     time.Time
	blockTime time.Duration
	base      int64
	calls     atomic.Int64
	fail      atomic.Bool
}

func (c *fakeChain) headAt(now time.Time) (int64, int64) {
	n := c.base + int64(now.Sub(c.start)/c.blockTime)
	ts := c.start.Add(time.Duration(n-c.base) * c.blockTime).Unix()
	return n, ts
}

func (c *fakeChain) poll(_ context.Context, _ bool) (*headObservation, error) {
	c.calls.Add(1)
	if c.fail.Load() {
		return nil, fmt.Errorf("upstream down")
	}
	n, ts := c.headAt(time.Now())
	return &headObservation{Number: n, Timestamp: ts, Hash: fmt.Sprintf("0x%064x", n)}, nil
}

func newTestSSR(t *testing.T, ctx context.Context) data.SharedStateRegistry {
	t.Helper()
	r, err := data.NewSharedStateRegistry(ctx, &log.Logger, &common.SharedStateConfig{
		ClusterKey: fmt.Sprintf("ht-%d", time.Now().UnixNano()),
		Connector: &common.ConnectorConfig{Driver: common.DriverMemory,
			Memory: &common.MemoryConnectorConfig{MaxItems: 1000, MaxTotalSize: "10MB"}},
	})
	require.NoError(t, err)
	return r
}

func newTestTracker(ssr data.SharedStateRegistry, cfg *common.EvmHeadTrackerConfig, deps headTrackerDeps) *headTracker {
	if cfg == nil {
		cfg = &common.EvmHeadTrackerConfig{Enabled: true}
	}
	cfg.SetDefaults()
	return newHeadTracker("p", "evm:1", "n", cfg, ssr, deps, &log.Logger)
}

func TestHeadTracker_AdaptiveWait(t *testing.T) {
	ssr := newTestSSR(t, t.Context())
	bt := 2 * time.Second
	ht := newTestTracker(ssr, nil, headTrackerDeps{blockTime: func() time.Duration { return bt }})
	ts := int64(1_700_000_000)
	blockAt := time.Unix(ts, 0)

	// Observed 300ms after the block: next poll at the next block's timestamp.
	w := ht.newHeadWait(&headObservation{Number: 10, Timestamp: ts}, blockAt.Add(300*time.Millisecond), bt)
	require.Equal(t, 1700*time.Millisecond, w)

	// Observed late (1.8s): never sooner than half a block.
	w = ht.newHeadWait(&headObservation{Number: 10, Timestamp: ts}, blockAt.Add(1800*time.Millisecond), bt)
	require.Equal(t, time.Second, w)

	// Mean propagation delay is added to the target (and the cap).
	ht.delays = []time.Duration{time.Second, time.Second}
	w = ht.newHeadWait(&headObservation{Number: 10, Timestamp: ts}, blockAt.Add(300*time.Millisecond), bt)
	require.Equal(t, 2700*time.Millisecond, w)
	ht.delays = nil

	// Stale result before the next block is due: wait for it.
	ht.prev = &headObservation{Number: 10, Timestamp: ts}
	w = ht.staleWait(&headObservation{Number: 10, Timestamp: ts}, blockAt.Add(time.Second), bt)
	require.Equal(t, time.Second, w)
	// Stale after the next block was due: retry at blockTime/4 (floor 500ms).
	w = ht.staleWait(&headObservation{Number: 10, Timestamp: ts}, blockAt.Add(3*time.Second), bt)
	require.Equal(t, 500*time.Millisecond, w)
	w = newTestTracker(ssr, nil, headTrackerDeps{blockTime: func() time.Duration { return 12 * time.Second }}).
		staleWait(&headObservation{Number: 10}, blockAt, 12*time.Second)
	require.Equal(t, 3*time.Second, w, "retry is blockTime/4 on slow chains")

	// Sub-second chains are floored at 500ms.
	w = ht.newHeadWait(&headObservation{Number: 10, Timestamp: ts}, blockAt, 400*time.Millisecond)
	require.Equal(t, common.MinHeadTrackerPollWait, w)
}

func TestHeadTracker_BlockTimeSource(t *testing.T) {
	ssr := newTestSSR(t, t.Context())
	var ema time.Duration
	ht := newTestTracker(ssr, nil, headTrackerDeps{blockTime: func() time.Duration { return ema }})
	require.Equal(t, common.DefaultHeadTrackerColdInterval, ht.blockTime(), "cold start")
	ema = 400 * time.Millisecond
	require.Equal(t, ema, ht.blockTime(), "EMA by default")
	require.Equal(t, headTrackerMinStale, ht.staleAfter())
	ema = 12 * time.Second
	require.Equal(t, 36*time.Second, ht.staleAfter(), "stale = 3 block times")

	ht = newTestTracker(ssr, &common.EvmHeadTrackerConfig{Enabled: true, Interval: &common.BlockTimeAdaptiveDuration{BlockTimeMultiplier: 0.5, Fallback: common.Duration(3 * time.Second)}},
		headTrackerDeps{blockTime: func() time.Duration { return ema }})
	require.Equal(t, 6*time.Second, ht.blockTime())
	ema = 0
	require.Equal(t, 3*time.Second, ht.blockTime(), "override fallback on cold start")
}

func TestHeadTracker_RegressionAndFutureGuards(t *testing.T) {
	ssr := newTestSSR(t, t.Context())
	bt := time.Second
	now := time.Unix(1_700_000_100, 0)
	var verified atomic.Int64
	chainOk := true
	ht := newTestTracker(ssr, nil, headTrackerDeps{
		blockTime: func() time.Duration { return bt },
		verifyChainId: func(context.Context, common.Upstream) (bool, error) {
			verified.Add(1)
			return chainOk, nil
		},
		majorMove: func() int64 { return 60 },
	})
	up := common.NewFakeUpstream("u1")
	ctx := t.Context()

	require.Empty(t, ht.reject(ctx, &headObservation{Number: 1000, Timestamp: now.Unix()}, 0, now, bt))
	require.Equal(t, "regression", ht.reject(ctx, &headObservation{Number: 1000}, 1000+common.DefaultToleratedBlockHeadRollback+1, now, bt))
	require.Empty(t, ht.reject(ctx, &headObservation{Number: 990}, 1000, now, bt), "small regressions are not rejected (the counter ignores them)")
	require.Equal(t, "far_future", ht.reject(ctx, &headObservation{Number: 1001, Timestamp: now.Add(2 * time.Minute).Unix()}, 1000, now, bt))

	ht.prev = &headObservation{Number: 1000, Timestamp: now.Unix() - 10}
	require.Empty(t, ht.reject(ctx, &headObservation{Number: 1010, Timestamp: now.Unix()}, 1000, now, bt))
	require.Equal(t, "far_future", ht.reject(ctx, &headObservation{Number: 1500, Timestamp: now.Unix()}, 1000, now, bt),
		"500 blocks in 10s of a 1s chain is impossible")

	ht.prev = nil
	require.Empty(t, ht.reject(ctx, &headObservation{Number: 1100, Upstream: up}, 1000, now, bt))
	require.Equal(t, int64(1), verified.Load(), "major jump verifies chain id")
	chainOk = false
	require.Equal(t, "chain_id", ht.reject(ctx, &headObservation{Number: 1100, Upstream: up}, 1000, now, bt))
	ht.prev = &headObservation{Number: 1000}
	require.Empty(t, ht.reject(ctx, &headObservation{Number: 1010, Upstream: up}, 1000, now, bt), "small move needs no verification")
}

func TestHeadTracker_TickPublishesAndCallsBack(t *testing.T) {
	ssr := newTestSSR(t, t.Context())
	chain := &fakeChain{start: time.Now().Add(-10 * time.Second), blockTime: time.Second, base: 100}
	var accepted []int64
	var publishedAtWrite int64 = -1
	ht := newTestTracker(ssr, nil, headTrackerDeps{
		poll:      chain.poll,
		blockTime: func() time.Duration { return time.Second },
	})
	ht.deps.onAccepted = func(_ context.Context, o *headObservation) {
		accepted = append(accepted, o.Number)
		publishedAtWrite = ht.Head()
	}
	ht.leaseDeadlineNs.Store(time.Now().Add(time.Hour).UnixNano())
	_, err := ht.tick(t.Context())
	require.NoError(t, err)
	require.GreaterOrEqual(t, ht.Head(), int64(110))
	require.Equal(t, []int64{ht.Head()}, accepted)
	require.Zero(t, publishedAtWrite, "S1: the block is written BEFORE the head is published")
	require.Equal(t, ht.Head(), ht.FreshHead())

	// Same head again: no second callback.
	ht.head.TryUpdate(t.Context(), ht.Head()+1)
	before := len(accepted)
	_, err = ht.tick(t.Context())
	require.NoError(t, err)
	require.Len(t, accepted, before)

	chain.fail.Store(true)
	w, err := ht.tick(t.Context())
	require.Error(t, err)
	require.Equal(t, common.MinHeadTrackerPollWait, w, "errors retry at max(500ms, blockTime/4), no backoff to the slow poller")
}

func TestHeadTracker_FallbackWhenStale(t *testing.T) {
	ssr := newTestSSR(t, t.Context())
	ht := newTestTracker(ssr, nil, headTrackerDeps{blockTime: func() time.Duration { return 100 * time.Millisecond }})
	require.Zero(t, ht.FreshHead(), "no head yet")
	require.True(t, ht.inFallback.Load())
	ht.head.TryUpdate(t.Context(), 42)
	require.Zero(t, ht.FreshHead(), "the first value a replica learns is not proof of freshness (could be an hours-old counter)")
	ht.head.TryUpdate(t.Context(), 43)
	require.Equal(t, int64(43), ht.FreshHead())
	require.False(t, ht.inFallback.Load())
}

// S5: freshness is the LOCAL receipt time of an advance, never the remote
// writer's timestamp, so a skewed clock on the leader cannot make followers
// treat a stale head as fresh (or a fresh one as stale).
func TestHeadTracker_FreshnessUsesLocalClock(t *testing.T) {
	ssr := newTestSSR(t, t.Context())
	now := time.Now()
	ht := newTestTracker(ssr, nil, headTrackerDeps{
		blockTime: func() time.Duration { return time.Second },
		now:       func() time.Time { return now },
	})
	ht.head.TryUpdate(t.Context(), 100)
	ht.head.TryUpdate(t.Context(), 101)
	require.Equal(t, int64(101), ht.FreshHead())
	// Local time moves past the staleness window with no advance: stale,
	// whatever timestamps the shared counter carries.
	now = now.Add(headTrackerMinStale + time.Second)
	require.Zero(t, ht.FreshHead())
	// S7: the fallback floor is the last fresh head, for a bounded time.
	require.Equal(t, int64(101), ht.FallbackFloor())
	now = now.Add(headTrackerFallbackFloorTTL)
	require.Zero(t, ht.FallbackFloor(), "the floor fails open after its TTL")
}

// S2: same height, different hash: the cached head block is rewritten.
func TestHeadTracker_SameHeightReorgRewritesBlock(t *testing.T) {
	ssr := newTestSSR(t, t.Context())
	hash := "0xaa"
	var written []string
	ht := newTestTracker(ssr, nil, headTrackerDeps{
		blockTime: func() time.Duration { return time.Second },
		poll: func(context.Context, bool) (*headObservation, error) {
			return &headObservation{Number: 500, Hash: hash, Timestamp: time.Now().Unix()}, nil
		},
		onAccepted: func(_ context.Context, o *headObservation) { written = append(written, o.Hash) },
	})
	ht.leaseDeadlineNs.Store(time.Now().Add(time.Hour).UnixNano())
	_, err := ht.tick(t.Context())
	require.NoError(t, err)
	_, err = ht.tick(t.Context())
	require.NoError(t, err)
	require.Equal(t, []string{"0xaa"}, written, "same block twice: written once")
	hash = "0xbb"
	_, err = ht.tick(t.Context())
	require.NoError(t, err)
	require.Equal(t, []string{"0xaa", "0xbb"}, written, "reorged head block is rewritten")
	require.Equal(t, int64(500), ht.Head())
}

// S3: the first head is verified (chain id), and a poisoned published head
// is recovered from after headTrackerRecoverAfter consistent polls.
func TestHeadTracker_BogusFirstHeadAndRecovery(t *testing.T) {
	ssr := newTestSSR(t, t.Context())
	up := common.NewFakeUpstream("u1")
	wrongChain := true
	n := int64(1000)
	ht := newTestTracker(ssr, nil, headTrackerDeps{
		blockTime: func() time.Duration { return time.Second },
		poll: func(context.Context, bool) (*headObservation, error) {
			return &headObservation{Number: n, Upstream: up, Timestamp: time.Now().Unix()}, nil
		},
		verifyChainId: func(context.Context, common.Upstream) (bool, error) { return !wrongChain, nil },
	})
	ht.leaseDeadlineNs.Store(time.Now().Add(time.Hour).UnixNano())
	_, _ = ht.tick(t.Context())
	require.Zero(t, ht.Head(), "a first head from a wrong-chain upstream is not published")

	// A poisoned counter (another chain's height published earlier).
	wrongChain = false
	ht.head.TryUpdate(t.Context(), 50_000_000)
	for i := 0; i < headTrackerRecoverAfter-1; i++ {
		_, _ = ht.tick(t.Context())
		require.Equal(t, int64(50_000_000), ht.Head(), "a single regressing poll is not enough")
		n++
	}
	_, _ = ht.tick(t.Context())
	require.Equal(t, n, ht.Head(), "consistent polls recover from the poisoned head")
}

// S4: a leader past its hard lease deadline never publishes.
func TestHeadTracker_NoPublishPastLeaseDeadline(t *testing.T) {
	ssr := newTestSSR(t, t.Context())
	ht := newTestTracker(ssr, nil, headTrackerDeps{
		blockTime: func() time.Duration { return time.Second },
		poll: func(context.Context, bool) (*headObservation, error) {
			return &headObservation{Number: 77, Timestamp: time.Now().Unix()}, nil
		},
	})
	ht.leaseDeadlineNs.Store(time.Now().Add(-time.Millisecond).UnixNano())
	_, _ = ht.tick(t.Context())
	require.Zero(t, ht.Head())
}

// Several replicas share one shared state: exactly one polls, ~1 call per
// block, and when the leader stops another takes over within ~TTL + 1 block
// and the head keeps advancing.
func TestHeadTracker_ElectionAndFailover(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ssr := newTestSSR(t, ctx)
	bt := time.Second
	chain := &fakeChain{start: time.Now(), blockTime: bt, base: 1000}

	const replicas = 4
	trackers := make([]*headTracker, replicas)
	var mu sync.Mutex
	pollsBy := map[int]int{}
	for i := 0; i < replicas; i++ {
		i := i
		cfg := &common.EvmHeadTrackerConfig{Enabled: true, LeaseTtl: common.Duration(time.Second)}
		trackers[i] = newTestTracker(ssr, cfg, headTrackerDeps{
			blockTime: func() time.Duration { return bt },
			poll: func(ctx context.Context, full bool) (*headObservation, error) {
				mu.Lock()
				pollsBy[i]++
				mu.Unlock()
				return chain.poll(ctx, full)
			},
		})
		trackers[i].Start(ctx)
	}
	defer func() {
		for _, tr := range trackers {
			tr.Stop()
		}
	}()

	leader := func() int {
		idx := -1
		for i, tr := range trackers {
			if tr.IsLeader() {
				require.Equal(t, -1, idx, "two leaders at once")
				idx = i
			}
		}
		return idx
	}
	require.Eventually(t, func() bool { return leader() >= 0 }, 3*time.Second, 10*time.Millisecond)

	// Sample leadership continuously over the window: never two at once.
	stopSampling := make(chan struct{})
	var maxLeaders atomic.Int32
	go func() {
		tk := time.NewTicker(5 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stopSampling:
				return
			case <-tk.C:
				var c int32
				for _, tr := range trackers {
					if tr.IsLeader() {
						c++
					}
				}
				if c > maxLeaders.Load() {
					maxLeaders.Store(c)
				}
			}
		}
	}()
	defer close(stopSampling)

	window := 5 * time.Second
	startCalls := chain.calls.Load()
	time.Sleep(window)
	blocks := float64(window / bt)
	calls := float64(chain.calls.Load() - startCalls)
	t.Logf("calls per block across %d replicas: %.2f", replicas, calls/blocks)
	require.LessOrEqual(t, calls/blocks, 2.0, "about one poll per block in total")
	require.GreaterOrEqual(t, calls/blocks, 0.8, "the leader keeps up with every block")
	mu.Lock()
	pollers := 0
	for _, n := range pollsBy {
		if n > 0 {
			pollers++
		}
	}
	mu.Unlock()
	require.Equal(t, 1, pollers, "only the leader polls")
	require.LessOrEqual(t, maxLeaders.Load(), int32(1), "never two concurrent leaders")
	expected, _ := chain.headAt(time.Now())
	for _, tr := range trackers {
		require.GreaterOrEqual(t, tr.Head(), expected-2, "every replica sees the head within ~1 block")
	}

	// Kill the leader without releasing (simulated crash: lease must expire).
	old := leader()
	crashed := trackers[old]
	crashed.abandonLease.Store(true)
	headBefore := crashed.Head()
	killedAt := time.Now()
	// Stop its loop but keep the lease held so takeover depends on TTL.
	crashed.Stop()
	require.Eventually(t, func() bool {
		l := leader()
		return l >= 0 && l != old
	}, 3*time.Second, 10*time.Millisecond)
	took := time.Since(killedAt)
	t.Logf("takeover after %s", took)
	require.LessOrEqual(t, took, time.Second+500*time.Millisecond, "within TTL + a renew check")
	require.Eventually(t, func() bool {
		for i, tr := range trackers {
			if i != old && tr.Head() <= headBefore+1 {
				return false
			}
		}
		return true
	}, 3*time.Second, 10*time.Millisecond, "head keeps advancing after failover (within ~TTL + 1 block)")
}
