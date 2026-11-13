package erpc

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/data"
	"github.com/erpc/erpc/test/chainsim"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// leaderNode is one eRPC instance of a leader fetch cluster: its own
// server and network, the cluster's shared state.
type leaderNode struct {
	srv    *HttpServer
	addr   string
	cancel context.CancelFunc
	nw     *Network
	erpc   *ERPC
	ctx    context.Context
}

// startSharedERPCServer is startTestERPCServer on a given shared-state
// registry, so instances share it as replicas share redis.
func startSharedERPCServer(t *testing.T, cfg *common.Config, ssr data.SharedStateRegistry) *leaderNode {
	t.Helper()
	logger := log.Logger
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, cfg.SetDefaults(&common.DefaultOptions{}))
	e, err := NewERPC(ctx, &logger, ssr, nil, nil, cfg)
	stopERPCAtEnd(t, e, ctx)
	require.NoError(t, err)
	e.Bootstrap(ctx)
	srv, err := NewHttpServer(ctx, &logger, cfg.Server, cfg.HealthCheck, cfg.Admin, e)
	require.NoError(t, err)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.serverV4.Serve(lis) }()
	n := &leaderNode{srv: srv, addr: lis.Addr().String(), cancel: cancel, erpc: e, ctx: ctx}
	require.Eventually(t, func() bool {
		nw, err := e.GetNetwork(context.Background(), "test_ws", "evm:123")
		if err != nil || nw == nil || nw.chainTracker == nil {
			return false
		}
		n.nw = nw
		return true
	}, 15*time.Second, 20*time.Millisecond, "network initializes")
	return n
}

// kill stops the instance at once, as a crash: its lease is not released.
func (n *leaderNode) kill() {
	if n.nw.leader != nil {
		n.nw.leader.noRelease = true
	}
	_ = n.srv.Shutdown(&zerolog.Logger{})
	n.cancel()
}

func (n *leaderNode) read(t *testing.T) {
	resp, err := http.Post(fmt.Sprintf("http://%s/test_ws/evm/123", n.addr), "application/json",
		bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)))
	if err == nil {
		resp.Body.Close()
	}
}

func (n *leaderNode) view() uint64 {
	if n.nw == nil || n.nw.chainTracker == nil {
		return 0
	}
	h, ok := n.nw.chainTracker.View().Head()
	if !ok {
		return 0
	}
	return h.Number
}

// leaderCluster is size instances on one memory shared state, each with
// the same two http vendors of a 300ms chain; leader: leader fetch on.
type leaderCluster struct {
	chain   *chainsim.Chain
	vendors []*chainsim.Vendor
	nodes   []*leaderNode
}

func newLeaderCluster(t *testing.T, size int, leader bool) *leaderCluster {
	chain := chainsim.NewChain(chainsim.Config{ChainID: 123, BlockTime: 300 * time.Millisecond, Seed: 11, History: 20})
	t.Cleanup(chain.Close)
	c := &leaderCluster{chain: chain}
	for _, id := range []string{"http-a", "http-b"} {
		v, err := chain.AddVendor(id, "vendor-"+id, false)
		require.NoError(t, err)
		c.vendors = append(c.vendors, v)
	}
	logger := log.Logger
	ssr, err := data.NewSharedStateRegistry(context.Background(), &logger, &common.SharedStateConfig{
		ClusterKey: "leader-test",
		Connector: &common.ConnectorConfig{
			Driver: "memory",
			Memory: &common.MemoryConnectorConfig{MaxItems: 100_000, MaxTotalSize: "1GB"},
		},
	})
	require.NoError(t, err)
	for i := 0; i < size; i++ {
		cfg := sourcesConfig(func(nw *common.NetworkConfig) {
			if leader {
				nw.Evm.ChainTracker = &common.EvmChainTrackerConfig{LeaderFetch: &common.EvmLeaderFetchConfig{
					Enabled:       true,
					LeaseTtl:      common.Duration(600 * time.Millisecond),
					MinStaleAfter: common.Duration(time.Second),
				}}
			}
		}, sourceUpstream(c.vendors[0].ID, c.vendors[0].URL(), common.SubscriptionSourceOff),
			sourceUpstream(c.vendors[1].ID, c.vendors[1].URL(), common.SubscriptionSourceOff))
		cfg.Server.WaitBeforeShutdown = common.Duration(0).Ptr()
		cfg.Server.WebSocket = &common.WebSocketServerConfig{}
		n := startSharedERPCServer(t, cfg, ssr)
		c.nodes = append(c.nodes, n)
		t.Cleanup(func() {
			_ = n.srv.Shutdown(&zerolog.Logger{})
			n.cancel()
		})
	}
	return c
}

// readAll reads latest on every live instance every 50ms until ctx ends.
func (c *leaderCluster) readAll(t *testing.T, ctx context.Context, live func(i int) bool) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i, n := range c.nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tick := time.NewTicker(50 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					if live(i) {
						n.read(t)
					}
				}
			}
		}()
	}
	return &wg
}

// latestCalls are the vendors' eth_getBlockByNumber(latest) calls in
// [from, to].
func (c *leaderCluster) latestCalls(from, to time.Time) int {
	total := 0
	for _, v := range c.vendors {
		total += len(v.LatestCalls(from, to))
	}
	return total
}

// leaders are the indexes of the live instances that lead.
func (c *leaderCluster) leaders(live func(i int) bool) []int {
	var out []int
	for i, n := range c.nodes {
		if live(i) && n.nw.leader != nil && n.nw.leader.leading.Load() {
			out = append(out, i)
		}
	}
	return out
}

// measure runs reads on every instance for d after a warm-up and returns
// the vendors' latest calls per new block.
func (c *leaderCluster) measure(t *testing.T, d time.Duration) (perBlock float64, calls int, blocks uint64) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := c.readAll(t, ctx, func(int) bool { return true })
	time.Sleep(2 * time.Second) // warm-up: the clocks fit the chain's phase
	from, start := time.Now(), c.chain.Tip().Number
	time.Sleep(d)
	to, end := time.Now(), c.chain.Tip().Number
	cancel()
	wg.Wait()
	calls, blocks = c.latestCalls(from, to), end-start
	require.Positive(t, blocks)
	return float64(calls) / float64(blocks), calls, blocks
}

// With the leader fetch on, N instances on one shared state spend about
// what one instance spends on latest fetches: the leader's clock, ~1 per
// block, and every follower's view still follows the chain.
func TestLeaderFetch_ClusterFetchesLatestAboutOncePerBlock(t *testing.T) {
	const size = 4
	c := newLeaderCluster(t, size, true)
	c.chain.Start()
	require.Eventually(t, func() bool { return len(c.leaders(func(int) bool { return true })) == 1 }, 10*time.Second, 20*time.Millisecond,
		"exactly one instance leads")

	perBlock, calls, blocks := c.measure(t, 6*time.Second)
	t.Logf("leader fetch: %d instances, %d latest calls over %d blocks: %.2f per block", size, calls, blocks, perBlock)
	require.Len(t, c.leaders(func(int) bool { return true }), 1, "still exactly one leader")
	require.LessOrEqual(t, perBlock, 2.0, "the cluster fetches latest about once per block, not once per instance")

	tip := c.chain.Tip().Number
	for i, n := range c.nodes {
		require.Eventually(t, func() bool { return n.view()+3 >= tip }, 5*time.Second, 20*time.Millisecond,
			"instance %d's view follows the chain (view %d, tip %d)", i, n.view(), tip)
	}
}

// Disabled, every instance runs its own clock: the cluster's latest fetches
// grow with its size, and no instance holds a leader fetch.
func TestLeaderFetch_DisabledEveryInstanceFetches(t *testing.T) {
	const size = 3
	c := newLeaderCluster(t, size, false)
	for _, n := range c.nodes {
		require.Nil(t, n.nw.leader, "no leader fetch when disabled")
		require.True(t, n.nw.EvmClockLeaderUntil().IsZero())
	}
	c.chain.Start()
	perBlock, calls, blocks := c.measure(t, 4*time.Second)
	t.Logf("disabled: %d instances, %d latest calls over %d blocks: %.2f per block", size, calls, blocks, perBlock)
	require.Greater(t, perBlock, float64(size)*0.5, "every instance fetches latest on its own")
}

// A leader that dies without releasing its lease is replaced within the
// lease TTL plus a pass, and the followers' views keep following the chain:
// they run their own clocks once the leader's heads are staleAfter old,
// until the new leader's heads arrive.
func TestLeaderFetch_LeaderDeathHandsOver(t *testing.T) {
	const size = 3
	c := newLeaderCluster(t, size, true)
	c.chain.Start()
	var dead sync.Map
	live := func(i int) bool { _, d := dead.Load(i); return !d }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := c.readAll(t, ctx, live)
	defer wg.Wait()
	defer cancel()

	require.Eventually(t, func() bool { return len(c.leaders(live)) == 1 }, 10*time.Second, 20*time.Millisecond, "one leader")
	old := c.leaders(live)[0]
	oldEpoch := c.nodes[old].nw.leader.epoch.Load()
	time.Sleep(time.Second)

	dead.Store(old, true)
	c.nodes[old].kill()
	killed := time.Now()

	// Lease TTL 600ms, passes every 200ms: a new leader within ~1s.
	var next int
	require.Eventually(t, func() bool {
		ls := c.leaders(live)
		if len(ls) != 1 {
			return false
		}
		next = ls[0]
		return true
	}, 3*time.Second, 10*time.Millisecond, "another instance takes the lease")
	handover := time.Since(killed)
	t.Logf("handover after %s", handover)
	require.Less(t, handover, 2*time.Second, "handover within the lease TTL plus a pass, with slack")
	require.Greater(t, c.nodes[next].nw.leader.epoch.Load(), oldEpoch, "the new leader's epoch fences the old one")

	// Every live follower keeps following the chain.
	time.Sleep(2 * time.Second)
	tip := c.chain.Tip().Number
	for i, n := range c.nodes {
		if !live(i) {
			continue
		}
		require.Eventually(t, func() bool { return n.view()+3 >= tip }, 5*time.Second, 20*time.Millisecond,
			"instance %d's view follows the chain after the handover (view %d, tip %d)", i, n.view(), tip)
	}
}

// A follower whose leader stops publishing (alive, lease held, heads lost)
// runs its own clock again once the last head is staleAfter old.
func TestLeaderFetch_StaleLeaderFallsBack(t *testing.T) {
	c := newLeaderCluster(t, 2, true)
	c.chain.Start()
	all := func(int) bool { return true }
	ctx, cancel := context.WithCancel(context.Background())
	wg := c.readAll(t, ctx, all)
	defer wg.Wait()
	defer cancel()
	require.Eventually(t, func() bool { return len(c.leaders(all)) == 1 }, 10*time.Second, 20*time.Millisecond, "one leader")
	follower := c.nodes[1-c.leaders(all)[0]]
	require.Eventually(t, func() bool { return !follower.nw.EvmClockLeaderUntil().IsZero() }, 5*time.Second, 20*time.Millisecond,
		"the follower follows the leader's heads")

	// The follower stops hearing the leader: its clock must take over
	// within staleAfter (max(3 x 300ms, 1s)) and its view keep moving.
	l := follower.nw.leader
	l.maxEpoch.Store(1 << 40) // every head of the current leader is now fenced
	require.Eventually(t, func() bool { return follower.nw.EvmClockLeaderUntil().IsZero() }, 2*time.Second, 20*time.Millisecond,
		"the follower stops following a stale leader")
	at := follower.view()
	require.Eventually(t, func() bool { return follower.view() >= at+3 }, 5*time.Second, 20*time.Millisecond,
		"the follower's own clock moves its view")
}
