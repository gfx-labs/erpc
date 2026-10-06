package erpc

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/util"
	"github.com/stretchr/testify/require"
)



// timedChain is a JSON-RPC EVM node whose head advances on the wall clock
// (one block per blockTime) with real unix timestamps, so the head tracker's
// block-time alignment is exercised as on a live chain. It counts calls per
// method and per "latest" tag, and can serve a frozen head for eth_blockNumber
// (a stale poller view) while getBlockByNumber stays live.
type timedChain struct {
	start     time.Time
	blockTime time.Duration
	base      int64
	srv       *httptest.Server

	mu    sync.Mutex
	calls map[string]int
	// latestCalls counts eth_getBlockByNumber("latest", *) calls.
	latestCalls atomic.Int64
	// byNumberCalls / byHashCalls count explicit-number and by-hash reads.
	byNumberCalls atomic.Int64
	byHashCalls   atomic.Int64
	// frozenBlockNumber, when > 0, is what eth_blockNumber returns.
	frozenBlockNumber atomic.Int64
}

func newTimedChain(blockTime time.Duration, base int64) *timedChain {
	c := &timedChain{start: time.Now(), blockTime: blockTime, base: base, calls: map[string]int{}}
	c.srv = httptest.NewServer(http.HandlerFunc(c.serve))
	return c
}

func (c *timedChain) Close()      { c.srv.Close() }
func (c *timedChain) URL() string { return c.srv.URL }

func (c *timedChain) head() int64 { return c.base + int64(time.Since(c.start)/c.blockTime) }

func (c *timedChain) Calls(method string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[method]
}

func timedHash(n int64) string { return fmt.Sprintf("0x%064x", n) }

func (c *timedChain) block(n int64, full bool) interface{} {
	if n > c.head() || n < 0 {
		return nil
	}
	ts := c.start.Add(time.Duration(n-c.base) * c.blockTime).Unix()
	tx := fmt.Sprintf("0x%064x", n+1<<40)
	var txs interface{} = []string{tx}
	if full {
		txs = []map[string]interface{}{{
			"hash": tx, "blockHash": timedHash(n), "blockNumber": fmt.Sprintf("0x%x", n),
			"transactionIndex": "0x0", "from": "0x0000000000000000000000000000000000000001",
			"to": "0x0000000000000000000000000000000000000002", "value": "0x0", "input": "0x",
			"nonce": "0x0", "gas": "0x5208", "gasPrice": "0x1", "type": "0x0",
			"v": "0x1b", "r": "0x1", "s": "0x1",
		}}
	}
	return map[string]interface{}{
		"number": fmt.Sprintf("0x%x", n), "hash": timedHash(n), "parentHash": timedHash(n - 1),
		"timestamp": fmt.Sprintf("0x%x", ts), "gasLimit": "0x1c9c380", "gasUsed": "0x5208",
		"miner": "0x0000000000000000000000000000000000000000", "extraData": "0x",
		"logsBloom": "0x" + strings.Repeat("0", 512), "transactions": txs, "uncles": []string{},
	}
}

func (c *timedChain) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var req struct {
		Id     json.RawMessage   `json:"id"`
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	_ = json.Unmarshal(body, &req)
	c.mu.Lock()
	c.calls[req.Method]++
	c.mu.Unlock()
	var result interface{}
	var rpcErr interface{}
	switch req.Method {
	case "eth_chainId":
		result = "0x7b"
	case "net_version":
		result = "123"
	case "eth_syncing":
		result = false
	case "eth_blockNumber":
		h := c.head()
		if f := c.frozenBlockNumber.Load(); f > 0 {
			h = f
		}
		result = fmt.Sprintf("0x%x", h)
	case "eth_call":
		var tag string
		if len(req.Params) > 1 {
			_ = json.Unmarshal(req.Params[1], &tag)
		}
		result = "0x" + strings.Repeat("0", 63) + "1"
		if tag != "latest" {
			// Echo a different value for interpolated calls so a test can
			// tell whether "latest" reached the upstream untouched.
			result = "0x" + strings.Repeat("0", 63) + "2"
		}
	case "eth_getBlockByNumber", "eth_getBlockByHash":
		var ref string
		var full bool
		_ = json.Unmarshal(req.Params[0], &ref)
		if len(req.Params) > 1 {
			_ = json.Unmarshal(req.Params[1], &full)
		}
		var n int64
		switch {
		case req.Method == "eth_getBlockByHash":
			c.byHashCalls.Add(1)
			n, _ = strconv.ParseInt(strings.TrimPrefix(ref, "0x"), 16, 64)
		case ref == "latest":
			c.latestCalls.Add(1)
			n = c.head()
		case ref == "finalized" || ref == "safe":
			n = c.head() - 64
		default:
			c.byNumberCalls.Add(1)
			n, _ = strconv.ParseInt(strings.TrimPrefix(ref, "0x"), 16, 64)
		}
		result = c.block(n, full)
	default:
		rpcErr = map[string]interface{}{"code": -32601, "message": "the method " + req.Method + " does not exist/is not available"}
	}
	resp := map[string]interface{}{"jsonrpc": "2.0", "id": req.Id}
	if rpcErr != nil {
		resp["error"] = rpcErr
	} else {
		resp["result"] = result
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// headTrackerTestConfig builds one replica's config: shared state and the
// JSON-RPC cache in the same Redis, a slow (60s) state poller, and the head
// tracker on or off.
func headTrackerTestConfig(redisAddr, cluster, upstreamURL string, ht *common.EvmHeadTrackerConfig, cache bool) *common.Config {
	rc := func() *common.RedisConnectorConfig {
		c := &common.RedisConnectorConfig{Addr: redisAddr, ConnPoolSize: 8}
		// The test fixture builds the sharedState registry before
		// cfg.SetDefaults runs, so defaults are applied here.
		_ = c.SetDefaults()
		return c
	}
	cfg := &common.Config{
		Server: &common.ServerConfig{ListenV4: util.BoolPtr(true)},
		Database: &common.DatabaseConfig{
			SharedState: &common.SharedStateConfig{
				ClusterKey: cluster,
				Connector:  &common.ConnectorConfig{Id: "ss", Driver: common.DriverRedis, Redis: rc()},
			},
		},
		Projects: []*common.ProjectConfig{{
			Id: "test_project",
			Networks: []*common.NetworkConfig{{
				Architecture: common.ArchitectureEvm,
				Evm:          &common.EvmNetworkConfig{ChainId: 123, HeadTracker: ht},
			}},
			Upstreams: []*common.UpstreamConfig{{
				Id:       "timed",
				Type:     common.UpstreamTypeEvm,
				Endpoint: upstreamURL,
				Evm:      &common.EvmUpstreamConfig{ChainId: 123, StatePollerInterval: common.Duration(60 * time.Second)},
			}},
		}},
		RateLimiters: &common.RateLimiterConfig{},
	}
	if cache {
		cfg.Database.EvmJsonRpcCache = &common.CacheConfig{
			Connectors: []*common.ConnectorConfig{{Id: "cache", Driver: common.DriverRedis, Redis: rc()}},
			Policies: []*common.CachePolicyConfig{
				{Network: "*", Method: "*", Finality: common.DataFinalityStateUnfinalized, Connector: "cache", TTL: common.FixedDuration(time.Minute)},
				{Network: "*", Method: "*", Finality: common.DataFinalityStateFinalized, Connector: "cache", TTL: common.FixedDuration(time.Minute)},
				{Network: "*", Method: "*", Finality: common.DataFinalityStateUnknown, Connector: "cache", TTL: common.FixedDuration(time.Minute)},
			},
		}
	}
	return cfg
}

type htReplica struct {
	send     func(string, map[string]string, map[string]string) (int, map[string]string, string)
	shutdown func()
	erpc     *ERPC
}

func (r htReplica) network(t *testing.T) *Network { return instanceNetwork(t, r.erpc) }

func (r htReplica) blockNumber(t *testing.T) int64 {
	res := doRpc(t, r.send, "eth_blockNumber", "[]")
	var s string
	require.NoError(t, json.Unmarshal(res.Result, &s))
	n, err := common.HexToInt64(s)
	require.NoError(t, err)
	return n
}

func startHTReplicas(t *testing.T, n int, mk func() *common.Config) []htReplica {
	t.Helper()
	out := make([]htReplica, n)
	for i := range out {
		send, _, _, shutdown, e := createServerTestFixtures(mk(), t)
		out[i] = htReplica{send: send, shutdown: shutdown, erpc: e}
		t.Cleanup(shutdown)
		// Materialize the network (and its tracker) on every replica.
		_ = out[i].network(t)
	}
	return out
}

func trackerOf(t *testing.T, r htReplica) *headTracker {
	ht := r.network(t).headTracker
	require.NotNil(t, ht)
	return ht
}

// N replicas, one fast chain, 60s state pollers: eth_blockNumber on every
// replica tracks the chain within about one block while the upstream sees
// about one "latest" poll per block in total.
func TestHeadTracker_E2E_MultiPodTracksFastChain(t *testing.T) {
	mr := miniredis.RunT(t)
	chain := newTimedChain(time.Second, 5_000)
	defer chain.Close()
	cluster := fmt.Sprintf("ht-e2e-%d", time.Now().UnixNano())
	ht := func() *common.EvmHeadTrackerConfig {
		return &common.EvmHeadTrackerConfig{Enabled: true, LeaseTtl: common.Duration(2 * time.Second)}
	}
	reps := startHTReplicas(t, 3, func() *common.Config { return headTrackerTestConfig(mr.Addr(), cluster, chain.URL(), ht(), false) })

	require.Eventually(t, func() bool {
		for _, r := range reps {
			if trackerOf(t, r).FreshHead() < chain.head()-1 {
				return false
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond, "every replica follows the tracker head")

	leaders := 0
	for _, r := range reps {
		if trackerOf(t, r).IsLeader() {
			leaders++
		}
	}
	require.Equal(t, 1, leaders, "exactly one replica polls")

	window := 6 * time.Second
	startLatest := chain.latestCalls.Load()
	startBN := chain.Calls("eth_blockNumber")
	deadline := time.Now().Add(window)
	maxLag := int64(0)
	for time.Now().Before(deadline) {
		for _, r := range reps {
			lag := chain.head() - r.blockNumber(t)
			maxLag = max(maxLag, lag)
		}
		time.Sleep(100 * time.Millisecond)
	}
	latestPerBlock := float64(chain.latestCalls.Load()-startLatest) / window.Seconds()
	t.Logf("3 replicas: %.2f latest polls per block, %d eth_blockNumber upstream calls, max lag %d blocks",
		latestPerBlock, chain.Calls("eth_blockNumber")-startBN, maxLag)
	require.LessOrEqual(t, maxLag, int64(1), "eth_blockNumber within ~1 block of the chain on every replica")
	require.LessOrEqual(t, latestPerBlock, 1.5, "about one poll per block across the fleet")
	require.Zero(t, chain.Calls("eth_blockNumber")-startBN, "client eth_blockNumber is answered locally")
}

// Kill the leader: a follower takes over within about TTL + 1 block and the
// head keeps advancing.
func TestHeadTracker_E2E_LeaderFailover(t *testing.T) {
	mr := miniredis.RunT(t)
	// miniredis expires keys only when time is advanced.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		tk := time.NewTicker(50 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				mr.FastForward(50 * time.Millisecond)
			}
		}
	}()
	chain := newTimedChain(time.Second, 9_000)
	defer chain.Close()
	cluster := fmt.Sprintf("ht-fo-%d", time.Now().UnixNano())
	ttl := 2 * time.Second
	reps := startHTReplicas(t, 2, func() *common.Config {
		return headTrackerTestConfig(mr.Addr(), cluster, chain.URL(), &common.EvmHeadTrackerConfig{Enabled: true, LeaseTtl: common.Duration(ttl)}, false)
	})
	var leader, follower htReplica
	require.Eventually(t, func() bool {
		for i, r := range reps {
			if trackerOf(t, r).IsLeader() {
				leader, follower = r, reps[1-i]
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool { return follower.blockNumber(t) >= chain.head()-1 }, 10*time.Second, 50*time.Millisecond)

	// Crash: the leader stops without releasing its lease.
	lt := trackerOf(t, leader)
	lt.abandonLease.Store(true)
	lt.Stop()
	killed := time.Now()
	ft := trackerOf(t, follower)
	require.Eventually(t, ft.IsLeader, 3*ttl, 20*time.Millisecond)
	took := time.Since(killed)
	t.Logf("takeover after %s (ttl %s)", took, ttl)
	require.LessOrEqual(t, took, ttl+time.Second+500*time.Millisecond)
	require.Eventually(t, func() bool { return follower.blockNumber(t) >= chain.head()-1 }, ttl+3*time.Second, 50*time.Millisecond,
		"the new leader keeps the head advancing")
}

// After one leader poll, another replica sharing the cache serves
// getBlockByNumber(latest), (<hex>) and getBlockByHash with zero upstream
// calls; a fullBlocks poll also fills the hashes-only variant.
func TestHeadTracker_E2E_FollowerServesPolledBlockFromCache(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprintf("fullBlocks=%v", full), func(t *testing.T) {
			mr := miniredis.RunT(t)
			chain := newTimedChain(2*time.Second, 7_000)
			defer chain.Close()
			cluster := fmt.Sprintf("ht-cache-%d", time.Now().UnixNano())
			reps := startHTReplicas(t, 2, func() *common.Config {
				return headTrackerTestConfig(mr.Addr(), cluster, chain.URL(),
					&common.EvmHeadTrackerConfig{Enabled: true, FullBlocks: full, LeaseTtl: common.Duration(2 * time.Second)}, true)
			})
			var follower htReplica
			require.Eventually(t, func() bool {
				for i, r := range reps {
					if trackerOf(t, r).IsLeader() {
						follower = reps[1-i]
						return true
					}
				}
				return false
			}, 10*time.Second, 20*time.Millisecond)

			// Wait for a fresh head whose block the leader has written.
			var head int64
			require.Eventually(t, func() bool {
				head = trackerOf(t, follower).FreshHead()
				return head > 0 && head == chain.head()
			}, 10*time.Second, 20*time.Millisecond)
			hexHead := fmt.Sprintf("0x%x", head)
			time.Sleep(200 * time.Millisecond) // async cache write

			before := chain.byNumberCalls.Load() + chain.byHashCalls.Load()
			beforeLatest := chain.latestCalls.Load()
			if head != chain.head() {
				t.Skip("chain advanced during the setup; block-time too short for this host")
			}
			for _, variant := range []bool{false, true} {
				if variant && !full {
					continue // a hashes-only poll never fabricates a full block
				}
				p := fmt.Sprintf(`["latest",%v]`, variant)
				r := doRpc(t, follower.send, "eth_getBlockByNumber", p)
				require.Contains(t, string(r.Result), timedHash(head), "latest resolves to the tracker head")
				r = doRpc(t, follower.send, "eth_getBlockByNumber", fmt.Sprintf(`["%s",%v]`, hexHead, variant))
				require.Contains(t, string(r.Result), timedHash(head))
				r = doRpc(t, follower.send, "eth_getBlockByHash", fmt.Sprintf(`["%s",%v]`, timedHash(head), variant))
				require.Contains(t, string(r.Result), hexHead)
			}
			require.Equal(t, before, chain.byNumberCalls.Load()+chain.byHashCalls.Load(), "follower reads made no upstream call")
			// The leader may poll latest once more meanwhile; the follower never does.
			require.LessOrEqual(t, chain.latestCalls.Load()-beforeLatest, int64(1))
		})
	}
}

// With the tracker OFF (premium-style config: no servedTip, skipInterpolation,
// enforceHighestBlock=false), eth_blockNumber, "latest" eth_call and
// getBlockByNumber("latest") reach the upstream untouched even while the
// state poller's view is stale, and no tracker runs.
func TestHeadTracker_E2E_PremiumPassthroughWhileDisabled(t *testing.T) {
	mr := miniredis.RunT(t)
	chain := newTimedChain(500*time.Millisecond, 3_000)
	defer chain.Close()
	cfg := headTrackerTestConfig(mr.Addr(), fmt.Sprintf("ht-prem-%d", time.Now().UnixNano()), chain.URL(), nil, false)
	cfg.Projects[0].Networks[0].DirectiveDefaults = &common.DirectiveDefaultsConfig{
		SkipInterpolation:   util.BoolPtr(true),
		EnforceHighestBlock: util.BoolPtr(false),
	}
	reps := startHTReplicas(t, 1, func() *common.Config { return cfg })
	r := reps[0]
	require.Nil(t, r.network(t).headTracker, "tracker is inert when not enabled")

	// Let the poller seed, then let the chain run ahead of it (60s interval).
	require.Eventually(t, func() bool { return r.network(t).EvmHighestLatestBlockNumber(t.Context()) > 0 }, 5*time.Second, 20*time.Millisecond)
	time.Sleep(2 * time.Second)
	stale := r.network(t).EvmHighestLatestBlockNumber(t.Context())
	require.Less(t, stale, chain.head()-1, "poller view is stale")

	before := chain.Calls("eth_blockNumber")
	got := r.blockNumber(t)
	require.GreaterOrEqual(t, got, chain.head()-1, "eth_blockNumber is the live upstream answer, not the stale poller value")
	require.Greater(t, chain.Calls("eth_blockNumber"), before, "eth_blockNumber went upstream")

	res := doRpc(t, r.send, "eth_call", `[{"to":"0x0000000000000000000000000000000000000002","data":"0x"},"latest"]`)
	require.Equal(t, `"0x`+strings.Repeat("0", 63)+`1"`, string(res.Result), `"latest" reached the upstream as the tag`)

	beforeLatest := chain.latestCalls.Load()
	res = doRpc(t, r.send, "eth_getBlockByNumber", `["latest",false]`)
	require.Greater(t, chain.latestCalls.Load(), beforeLatest, "getBlockByNumber(latest) went upstream as the tag")
	var blk struct{ Number string }
	require.NoError(t, json.Unmarshal(res.Result, &blk))
	n, _ := common.HexToInt64(blk.Number)
	require.Greater(t, n, stale, "latest block is live, not the stale poller head")
}
