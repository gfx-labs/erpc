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
	"github.com/erpc/erpc/blockstore"
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
	// lagBlocks makes this node see the chain that many blocks late: a
	// lagging upstream that returns null (or an error) for the head block.
	lagBlocks atomic.Int64
	// failData makes eth_call / eth_getLogs fail with a retryable server
	// error, so data traffic fails over to the other upstreams.
	failData atomic.Bool
	// pinned, when > 0, freezes the chain at that height (deterministic
	// assertions on one head block).
	pinned atomic.Int64
}

func newTimedChain(blockTime time.Duration, base int64) *timedChain {
	c := &timedChain{start: time.Now(), blockTime: blockTime, base: base, calls: map[string]int{}}
	c.srv = httptest.NewServer(http.HandlerFunc(c.serve))
	return c
}

func (c *timedChain) Close()      { c.srv.Close() }
func (c *timedChain) URL() string { return c.srv.URL }

func (c *timedChain) head() int64 {
	if p := c.pinned.Load(); p > 0 {
		return p - c.lagBlocks.Load()
	}
	return c.base + int64(time.Since(c.start)/c.blockTime) - c.lagBlocks.Load()
}

// view returns a node sharing the same chain (start, base, block time) but
// with its own server and counters, so several upstreams of one network can
// be observed separately.
func (c *timedChain) view() *timedChain {
	v := &timedChain{start: c.start, blockTime: c.blockTime, base: c.base, calls: map[string]int{}}
	v.srv = httptest.NewServer(http.HandlerFunc(v.serve))
	return v
}

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
	if c.failData.Load() && (req.Method == "eth_call" || req.Method == "eth_getLogs") {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"temporarily unavailable"}}`))
		return
	}
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
		if strings.HasPrefix(tag, "0x") {
			if n, err := strconv.ParseInt(tag[2:], 16, 64); err == nil && n > c.head() {
				rpcErr = map[string]interface{}{"code": -32000, "message": "header not found"}
				break
			}
		}
		result = "0x" + strings.Repeat("0", 63) + "1"
		if tag != "latest" {
			// Echo a different value for interpolated calls so a test can
			// tell whether "latest" reached the upstream untouched.
			result = "0x" + strings.Repeat("0", 63) + "2"
		}
	case "eth_getLogs":
		var flt map[string]interface{}
		_ = json.Unmarshal(req.Params[0], &flt)
		to, _ := flt["toBlock"].(string)
		if strings.HasPrefix(to, "0x") {
			if n, err := strconv.ParseInt(to[2:], 16, 64); err == nil && n > c.head() {
				rpcErr = map[string]interface{}{"code": -32000, "message": "block range extends beyond current head block"}
				break
			}
		}
		result = []interface{}{}
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

	// Count concurrent leaders on every tick of the run, not one snapshot.
	stopSampling := make(chan struct{})
	var maxLeaders, minLeaders atomic.Int32
	minLeaders.Store(99)
	trackers := make([]*headTracker, len(reps))
	for i, r := range reps {
		trackers[i] = trackerOf(t, r)
	}
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
				maxLeaders.Store(max(maxLeaders.Load(), c))
				minLeaders.Store(min(minLeaders.Load(), c))
			}
		}
	}()

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
	close(stopSampling)
	require.Equal(t, int32(1), maxLeaders.Load(), "never more than one polling replica")
	require.Equal(t, int32(1), minLeaders.Load(), "always exactly one polling replica")
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

			// Freeze the chain, then wait until the follower serves that head
			// as fresh. The leader writes the block BEFORE publishing (S1), so
			// no sleep is needed: seeing the head implies the block is cached.
			require.Eventually(t, func() bool { return trackerOf(t, follower).FreshHead() > 0 }, 10*time.Second, 20*time.Millisecond)
			head := chain.head()
			chain.pinned.Store(head)
			require.Eventually(t, func() bool { return trackerOf(t, follower).FreshHead() == head }, 5*time.Second, 5*time.Millisecond)
			hexHead := fmt.Sprintf("0x%x", head)

			before := chain.byNumberCalls.Load() + chain.byHashCalls.Load()
			beforeLatest := chain.latestCalls.Load()
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
			require.LessOrEqual(t, chain.latestCalls.Load()-beforeLatest, int64(2))
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

// Regression (review blocker): with the tracker on, "latest" interpolates to
// the tracker head, which is ahead of every upstream's 60s poller view. The
// block-availability gates must then NOT force-poll each upstream's head per
// request (that brings back per-upstream per-block polling). 3 replicas × 3
// upstreams, 1s blocks, a steady stream of eth_getLogs(toBlock=latest),
// eth_call(latest) and getBlockByNumber(latest): total upstream
// latest/blockNumber polls stay ≈ 1 per block (the leader only) plus the
// pollers' own ticks. One upstream lags two blocks and answers null/errors
// for the head: requests still succeed by failing over.
func TestHeadTracker_E2E_NoPerUpstreamPollingUnderLatestTraffic(t *testing.T) {
	mr := miniredis.RunT(t)
	chain := newTimedChain(time.Second, 20_000)
	defer chain.Close()
	ups := []*timedChain{chain, chain.view(), chain.view()}
	defer ups[1].Close()
	defer ups[2].Close()
	// u0 (first in order) answers the leader's latest polls, so its known
	// head stays current through response enrichment. Make it fail data
	// methods so that traffic lands on u1/u2, whose 60s poller views are
	// stale: exactly the upstreams the availability gate used to force-poll.
	ups[0].failData.Store(true)
	ups[2].lagBlocks.Store(2)

	cluster := fmt.Sprintf("ht-nopoll-%d", time.Now().UnixNano())
	mk := func() *common.Config {
		cfg := headTrackerTestConfig(mr.Addr(), cluster, chain.URL(),
			&common.EvmHeadTrackerConfig{Enabled: true, LeaseTtl: common.Duration(2 * time.Second)}, false)
		prj := cfg.Projects[0]
		base := prj.Upstreams[0]
		prj.Upstreams = nil
		for i, u := range ups {
			c := *base
			evmCfg := *base.Evm
			c.Evm = &evmCfg
			c.Id = fmt.Sprintf("u%d", i)
			c.Endpoint = u.URL()
			if i == 1 {
				// A head-relative serving range: the network-level gate
				// (checkUpstreamBlockAvailability) used to skip this upstream
				// for every tracked block and fire handleBlockSkip's poll.
				zero := int64(0)
				evmCfg.BlockAvailability = &common.EvmBlockAvailabilityConfig{
					Upper: &common.EvmAvailabilityBoundConfig{LatestBlockMinus: &zero},
				}
			}
			prj.Upstreams = append(prj.Upstreams, &c)
		}
		return cfg
	}
	reps := startHTReplicas(t, 3, mk)
	require.Eventually(t, func() bool {
		for _, r := range reps {
			if trackerOf(t, r).FreshHead() < chain.head()-1 {
				return false
			}
		}
		return true
	}, 15*time.Second, 50*time.Millisecond)

	headPolls := func() int64 {
		var n int64
		for _, u := range ups {
			n += u.latestCalls.Load() + int64(u.Calls("eth_blockNumber"))
		}
		return n
	}
	start := headPolls()
	const blocks = 30
	deadline := time.Now().Add(blocks * chain.blockTime)
	var reqs, fails int
	for time.Now().Before(deadline) {
		for _, r := range reps {
			for _, q := range []struct{ m, p string }{
				{"eth_getLogs", `[{"fromBlock":"latest","toBlock":"latest"}]`},
				{"eth_call", `[{"to":"0x0000000000000000000000000000000000000002","data":"0x"},"latest"]`},
				{"eth_getBlockByNumber", `["latest",false]`},
			} {
				reqs++
				code, _, body := r.send(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":%s}`, q.m, q.p), nil, nil)
				var resp rpcResp
				_ = json.Unmarshal([]byte(body), &resp)
				if code != 200 || len(resp.Error) > 0 || string(resp.Result) == "null" {
					fails++
					t.Logf("%s failed: %d %s", q.m, code, body)
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	polls := headPolls() - start
	t.Logf("%d requests over %d blocks: %d upstream latest/blockNumber polls (%.2f per block), %d failures",
		reqs, blocks, polls, float64(polls)/blocks, fails)
	require.Zero(t, fails, "requests succeed even though one upstream lags the head")
	// Leader: ~1/block. Slack for a stale retry or two and the 60s pollers'
	// ticks (3 upstreams × 3 replicas, at most one tick each in the window).
	require.LessOrEqual(t, float64(polls), blocks*1.3+9, "no per-upstream per-request head polling")
}

// The hashes-only block derived from a fullBlocks poll must equal what an
// upstream returns for eth_getBlockByNumber(n, false): same fields and values,
// transactions as hashes in order (key order is irrelevant on the wire).
func TestHeadTracker_DerivedHashesOnlyBlockMatchesUpstream(t *testing.T) {
	type tx = map[string]interface{}
	hashes := []string{timedHash(1 << 41), timedHash(1<<41 + 1), timedHash(1<<41 + 2)}
	base := map[string]interface{}{
		"number": "0x10", "hash": timedHash(16), "parentHash": timedHash(15), "timestamp": "0x65",
		"gasLimit": "0x1c9c380", "gasUsed": "0x5208", "miner": "0x0000000000000000000000000000000000000000",
		"extraData": "0x", "logsBloom": "0x" + strings.Repeat("0", 512), "uncles": []string{},
		"baseFeePerGas": "0x7", "withdrawals": []interface{}{}, "size": "0x220",
	}
	full := map[string]interface{}{}
	hashOnly := map[string]interface{}{}
	for k, v := range base {
		full[k], hashOnly[k] = v, v
	}
	var txs []tx
	for i, h := range hashes {
		txs = append(txs, tx{"hash": h, "blockHash": timedHash(16), "blockNumber": "0x10",
			"transactionIndex": fmt.Sprintf("0x%x", i), "from": "0x0000000000000000000000000000000000000001",
			"input": "0x", "nonce": fmt.Sprintf("0x%x", i), "type": "0x2", "value": "0x0"})
	}
	full["transactions"] = txs
	hashOnly["transactions"] = hashes
	for name, pair := range map[string][2]interface{}{
		"three txs":  {full, hashOnly},
		"timedChain": {newTimedChain(time.Second, 1).block(1, true), newTimedChain(time.Second, 1).block(1, false)},
	} {
		fullRaw, _ := json.Marshal(pair[0])
		wantRaw, _ := json.Marshal(pair[1])
		got, err := (&blockstore.BlockRecord{Block: fullRaw}).BlockJSON(false)
		require.NoError(t, err, name)
		require.JSONEq(t, string(wantRaw), string(got), name)
	}
}

// Shared state (Redis) goes away: no replica can hold or renew the lease, so
// none polls; all fall back to the poller heads with the floor keeping
// eth_blockNumber monotonic; no polling storm.
func TestHeadTracker_E2E_RedisOutageFallsBackWithoutPolling(t *testing.T) {
	mr := miniredis.RunT(t)
	chain := newTimedChain(time.Second, 40_000)
	defer chain.Close()
	cluster := fmt.Sprintf("ht-outage-%d", time.Now().UnixNano())
	reps := startHTReplicas(t, 3, func() *common.Config {
		return headTrackerTestConfig(mr.Addr(), cluster, chain.URL(),
			&common.EvmHeadTrackerConfig{Enabled: true, LeaseTtl: common.Duration(time.Second)}, false)
	})
	require.Eventually(t, func() bool {
		for _, r := range reps {
			if trackerOf(t, r).FreshHead() < chain.head()-1 {
				return false
			}
		}
		return true
	}, 15*time.Second, 50*time.Millisecond)
	last := map[int]int64{}
	for i, r := range reps {
		last[i] = r.blockNumber(t)
	}

	mr.Close()
	// Every replica steps down within ~2/3 TTL and enters fallback after
	// the staleness window.
	require.Eventually(t, func() bool {
		for _, r := range reps {
			if trackerOf(t, r).IsLeader() || trackerOf(t, r).FreshHead() != 0 {
				return false
			}
		}
		return true
	}, 15*time.Second, 50*time.Millisecond, "no leader and every replica in fallback")

	// Client eth_blockNumber requests go upstream in fallback (as without the
	// tracker), so count only background head polls: getBlockByNumber(latest).
	start := chain.latestCalls.Load()
	window := 5 * time.Second
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		for i, r := range reps {
			bn := r.blockNumber(t)
			require.GreaterOrEqual(t, bn, last[i], "eth_blockNumber never goes backwards in fallback")
			last[i] = bn
		}
		time.Sleep(100 * time.Millisecond)
	}
	polls := chain.latestCalls.Load() - start
	t.Logf("redis outage: %d upstream head polls in %s across 3 replicas", polls, window)
	require.LessOrEqual(t, polls, int64(3), "no polling storm: only the 60s pollers may tick")
}
