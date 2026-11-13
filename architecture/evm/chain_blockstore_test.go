package evm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erpc/erpc/architecture/evm/logfilter"
	"github.com/erpc/erpc/blockstore"
	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/data"
	"github.com/erpc/erpc/indexer"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// fleetUpstream counts receipts sweeps across every simulated replica: it
// is the upstream they all share.
type fleetUpstream struct {
	mu       sync.Mutex
	receipts map[string]json.RawMessage // block hash -> receipts
	calls    map[string]*atomic.Int32
	delay    time.Duration
}

func (u *fleetUpstream) count(hash string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	if c := u.calls[hash]; c != nil {
		return int(c.Load())
	}
	return 0
}

type fleetUpstreamId struct{ common.Upstream }

func (fleetUpstreamId) Id() string { return "up-1" }

// replicaNet is one eRPC replica's network: its own scheduler and
// trackers, sweeping the shared fleetUpstream.
type replicaNet struct {
	common.Network
	up *fleetUpstream
}

func (*replicaNet) ProjectId() string             { return "test" }
func (*replicaNet) Label() string                 { return "evm:1" }
func (*replicaNet) Id() string                    { return "evm:1" }
func (*replicaNet) Config() *common.NetworkConfig { return nil }
func (*replicaNet) Logger() *zerolog.Logger       { l := zerolog.Nop(); return &l }

func (n *replicaNet) SweepUpstreams(ctx context.Context, req *common.NormalizedRequest, _ string, check func(*common.NormalizedResponse) error) (*common.NormalizedResponse, error) {
	jrq, err := req.JsonRpcRequest()
	if err != nil {
		return nil, err
	}
	hash, _ := jrq.Params[0].(string)
	hash = strings.ToLower(hash)
	n.up.mu.Lock()
	c := n.up.calls[jrq.Method+hash]
	if c == nil {
		c = &atomic.Int32{}
		n.up.calls[jrq.Method+hash] = c
	}
	body := n.up.receipts[hash]
	n.up.mu.Unlock()
	c.Add(1)
	if n.up.delay > 0 {
		time.Sleep(n.up.delay)
	}
	if body == nil {
		body = json.RawMessage("null")
	}
	resp := common.NewNormalizedResponse().WithJsonRpcResponse(
		common.MustNewJsonRpcResponseFromBytes([]byte(`1`), body, nil)).SetUpstream(fleetUpstreamId{})
	return resp, check(resp)
}

// blockFixture is a header with a real logsBloom and receipts whose logs
// recompute it.
func blockFixture(n uint64, parent string, txs int) (hash string, header, receipts json.RawMessage) {
	hash = testHash("e", n)
	bloom := make([]byte, logfilter.BloomLen)
	var rs []map[string]any
	var hashes []string
	for i := range txs {
		tx := fmt.Sprintf("0x%064x", n*1000+uint64(i))
		addr := fmt.Sprintf("0x%040x", 0xa0+i)
		topic := fmt.Sprintf("0x%064x", 0xf0)
		a, _ := common.HexToBytes(addr)
		tp, _ := common.HexToBytes(topic)
		logfilter.BloomAdd(bloom, a)
		logfilter.BloomAdd(bloom, tp)
		hashes = append(hashes, tx)
		rs = append(rs, map[string]any{
			"blockHash": hash, "blockNumber": fmt.Sprintf("0x%x", n), "transactionHash": tx,
			"logsBloom": "0x" + strings.Repeat("0", 512),
			"logs": []map[string]any{{
				"address": addr, "topics": []string{topic}, "data": "0x",
				"blockHash": hash, "blockNumber": fmt.Sprintf("0x%x", n), "transactionHash": tx,
				"transactionIndex": fmt.Sprintf("0x%x", i), "logIndex": fmt.Sprintf("0x%x", i), "removed": false,
			}},
		})
	}
	header, _ = json.Marshal(map[string]any{
		"number": fmt.Sprintf("0x%x", n), "hash": hash, "parentHash": parent,
		"timestamp": fmt.Sprintf("0x%x", 1_700_000_000+n), "logsBloom": fmt.Sprintf("0x%x", bloom),
		"transactionsRoot": testHash("d", n), "transactions": hashes,
	})
	receipts, _ = json.Marshal(rs)
	return hash, header, receipts
}

func fleetConnector(t *testing.T) data.Connector {
	lg := zerolog.Nop()
	c, err := data.NewMemoryConnector(t.Context(), &lg, "fleet", &common.MemoryConnectorConfig{MaxItems: 10_000, MaxTotalSize: "64MB"})
	require.NoError(t, err)
	return c
}

// newReplica is one replica: a network registered with its own trackers,
// attached to the shared store over conn.
func newReplica(t *testing.T, up *fleetUpstream, conn data.Connector, opt blockstore.Options, logsFill int64) (*replicaNet, *ChainTracker) {
	n := &replicaNet{up: up}
	tr := StartChainTrackers(t.Context(), n)
	SetChainStore(n, blockstore.New(conn, opt), logsFill)
	t.Cleanup(func() { SetChainStore(n, nil, 0) })
	return n, tr
}

func fleetOpts() blockstore.Options {
	return blockstore.Options{Namespace: "t", ProjectId: "test", Network: "evm:1", TTL: time.Minute, PeerWait: 2 * time.Second}
}

// Two replicas share one connector: each block's receipts cost one
// upstream fetch, whichever replica asks first, and the other is served
// from the store (or waits on the lease, then reads it).
func TestChainStore_TwoReplicasOneReceiptsFetchPerBlock(t *testing.T) {
	up := &fleetUpstream{receipts: map[string]json.RawMessage{}, calls: map[string]*atomic.Int32{}, delay: 50 * time.Millisecond}
	conn := fleetConnector(t)
	_, a := newReplica(t, up, conn, fleetOpts(), 0)
	_, b := newReplica(t, up, conn, fleetOpts(), 0)

	parent := testHash("e", 0)
	var hashes []string
	for n := uint64(1); n <= 4; n++ {
		hash, hdr, rec := blockFixture(n, parent, 3)
		up.receipts[hash] = rec
		for _, tr := range []*ChainTracker{a, b} {
			tr.Observe("src", hdr, true)
		}
		parent = hash
		hashes = append(hashes, hash)
	}
	a.Sync(t.Context())
	b.Sync(t.Context())

	for _, hash := range hashes {
		var wg sync.WaitGroup
		for _, tr := range []*ChainTracker{a, b} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				r, ok := tr.resolveReceipts(ctx, hash, -1)
				require.True(t, ok)
				require.Len(t, r, 3)
			}()
		}
		wg.Wait()
		require.Equal(t, 1, up.count("eth_getBlockReceipts"+hash), "block %s: one upstream receipts fetch for the fleet", hash)
	}
}

// A store whose payload is corrupt (a log dropped) fails validation and
// the replica falls back to the upstream, which then overwrites it.
func TestChainStore_InvalidStoredReceiptsFallBack(t *testing.T) {
	up := &fleetUpstream{receipts: map[string]json.RawMessage{}, calls: map[string]*atomic.Int32{}}
	conn := fleetConnector(t)
	opt := fleetOpts()
	_, a := newReplica(t, up, conn, opt, 0)
	hash, hdr, rec := blockFixture(1, testHash("e", 0), 3)
	up.receipts[hash] = rec
	a.Observe("src", hdr, true)
	a.Sync(t.Context())

	var rs []map[string]any
	require.NoError(t, json.Unmarshal(rec, &rs))
	rs[1]["logs"] = []any{}
	bad, _ := json.Marshal(rs)
	store := blockstore.New(conn, opt)
	require.NoError(t, store.Put(t.Context(), blockstore.KindReceipts, hash, bad, 0))
	require.Eventually(t, func() bool { _, ok := store.Get(t.Context(), blockstore.KindReceipts, hash); return ok }, time.Second, 5*time.Millisecond)

	r, ok := a.resolveReceipts(t.Context(), hash, -1)
	require.True(t, ok)
	require.Len(t, r, 3)
	require.Equal(t, 1, up.count("eth_getBlockReceipts"+hash), "invalid stored receipts are refetched upstream")
	require.Eventually(t, func() bool {
		got, ok := store.Get(t.Context(), blockstore.KindReceipts, hash)
		return ok && string(got) == string(rec)
	}, time.Second, 5*time.Millisecond, "the checked answer replaces the bad entry")
}

// Without a store nothing changes: every replica fetches.
func TestChainStore_DisabledEachReplicaFetches(t *testing.T) {
	up := &fleetUpstream{receipts: map[string]json.RawMessage{}, calls: map[string]*atomic.Int32{}}
	a := StartChainTrackers(t.Context(), &replicaNet{up: up})
	b := StartChainTrackers(t.Context(), &replicaNet{up: up})
	hash, hdr, rec := blockFixture(1, testHash("e", 0), 2)
	up.receipts[hash] = rec
	for _, tr := range []*ChainTracker{a, b} {
		tr.Observe("src", hdr, true)
		tr.Sync(t.Context())
		_, ok := tr.resolveReceipts(t.Context(), hash, -1)
		require.True(t, ok)
	}
	require.Equal(t, 2, up.count("eth_getBlockReceipts"+hash))
}

// A replica whose peer holds the lease and never stores falls back to its
// own fetch once PeerWait ends (fail-open).
func TestChainStore_LeaseHolderStallsFailOpen(t *testing.T) {
	up := &fleetUpstream{receipts: map[string]json.RawMessage{}, calls: map[string]*atomic.Int32{}}
	conn := fleetConnector(t)
	opt := fleetOpts()
	opt.PeerWait = 200 * time.Millisecond
	_, a := newReplica(t, up, conn, opt, 0)
	hash, hdr, rec := blockFixture(1, testHash("e", 0), 1)
	up.receipts[hash] = rec
	a.Observe("src", hdr, true)
	a.Sync(t.Context())

	release, ok := blockstore.New(conn, opt).Lease(t.Context(), blockstore.KindReceipts, hash)
	require.True(t, ok)
	defer release()
	start := time.Now()
	_, got := a.resolveReceipts(t.Context(), hash, -1)
	require.True(t, got)
	require.GreaterOrEqual(t, time.Since(start), 150*time.Millisecond, "it waited for the peer")
	require.Equal(t, 1, up.count("eth_getBlockReceipts"+hash))
}

// Logs fill serves a small range at or below the head from held or
// stored receipts, with any filter, and declines on any gap.
func TestChainStore_LogsFill(t *testing.T) {
	up := &fleetUpstream{receipts: map[string]json.RawMessage{}, calls: map[string]*atomic.Int32{}}
	conn := fleetConnector(t)
	opt := fleetOpts()
	na, a := newReplica(t, up, conn, opt, 4)
	nb, b := newReplica(t, up, conn, opt, 4)

	parent := testHash("e", 0)
	var hashes []string
	for n := uint64(1); n <= 3; n++ {
		hash, hdr, rec := blockFixture(n, parent, 2)
		up.receipts[hash] = rec
		a.Observe("src", hdr, true)
		b.Observe("src", hdr, true)
		parent = hash
		hashes = append(hashes, hash)
	}
	a.Sync(t.Context())
	b.Sync(t.Context())
	head, ok := a.View().Head()
	require.True(t, ok)
	require.Equal(t, uint64(3), head.Number)

	f := func(from, to int, extra map[string]any) *logfilter.LogsFilter {
		m := map[string]any{"fromBlock": fmt.Sprintf("0x%x", from), "toBlock": fmt.Sprintf("0x%x", to)}
		for k, v := range extra {
			m[k] = v
		}
		return logsFillFilter(m)
	}
	// Nothing held or stored yet: a gap.
	_, ok = LogsFill(t.Context(), na, a, f(1, 3, nil))
	require.False(t, ok)

	// Replica a fetches receipts; replica b serves from the store.
	for _, h := range hashes {
		_, ok := a.resolveReceipts(t.Context(), h, -1)
		require.True(t, ok)
	}
	require.Eventually(t, func() bool {
		_, ok := LogsFill(t.Context(), nb, b, f(1, 3, nil))
		return ok
	}, time.Second, 10*time.Millisecond)
	logs, ok := LogsFill(t.Context(), nb, b, f(1, 3, nil))
	require.True(t, ok)
	require.Len(t, logs, 6)
	logs, ok = LogsFill(t.Context(), nb, b, f(2, 3, map[string]any{"address": fmt.Sprintf("0x%040x", 0xa1)}))
	require.True(t, ok)
	require.Len(t, logs, 2)
	for _, h := range hashes {
		require.Equal(t, 1, up.count("eth_getBlockReceipts"+h), "b served from a's fetch")
	}

	_, ok = LogsFill(t.Context(), nb, b, f(1, 4, nil))
	require.False(t, ok, "above the head")
	_, ok = LogsFill(t.Context(), nb, b, f(0, 8, nil))
	require.False(t, ok, "wider than maxRange")
	require.Nil(t, logsFillFilter(map[string]any{"fromBlock": "latest", "toBlock": "0x3"}), "tags are not served")
}

// Finalized pins are promoted into the historical store with their
// height index; reads check header against index and receipts against
// header.
func TestChainStore_HistoricalPromotion(t *testing.T) {
	up := &fleetUpstream{receipts: map[string]json.RawMessage{}, calls: map[string]*atomic.Int32{}}
	conn := fleetConnector(t)
	opt := fleetOpts()
	opt.HistoricalTTL = time.Hour
	na, a := newReplica(t, up, conn, opt, 0)
	parent := testHash("e", 0)
	var hashes []string
	for n := uint64(1); n <= 3; n++ {
		hash, hdr, rec := blockFixture(n, parent, 1)
		up.receipts[hash] = rec
		a.Observe("src", hdr, true)
		parent = hash
		hashes = append(hashes, hash)
	}
	a.Sync(t.Context())
	_, ok := a.resolveReceipts(t.Context(), hashes[0], -1)
	require.True(t, ok)

	require.Equal(t, 2, PromoteFinalized(t.Context(), na, a, 2))
	require.Equal(t, 0, PromoteFinalized(t.Context(), na, a, 2), "promotion is incremental")
	require.Eventually(t, func() bool {
		hdr, rec, ok := HistoricalBlock(t.Context(), na, 1)
		return ok && hdr != nil && rec != nil
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		hdr, rec, ok := HistoricalBlock(t.Context(), na, 2)
		return ok && hdr != nil && rec == nil
	}, time.Second, 10*time.Millisecond, "block 2's receipts were never fetched: header only")
	_, _, ok = HistoricalBlock(t.Context(), na, 3)
	require.False(t, ok, "not finalized")
}

var _ = indexer.Cursor{}

// The eth_getLogs network hook answers a fill hit from the store, keeps
// the request id, and leaves a gap, a tag range or a skip-cache request to
// the normal path.
func TestChainStore_GetLogsHook(t *testing.T) {
	up := &fleetUpstream{receipts: map[string]json.RawMessage{}, calls: map[string]*atomic.Int32{}}
	conn := fleetConnector(t)
	na, a := newReplica(t, up, conn, fleetOpts(), 4)
	hash, hdr, rec := blockFixture(1, testHash("e", 0), 2)
	up.receipts[hash] = rec
	a.Observe("src", hdr, true)
	a.Sync(t.Context())

	req := func(body string) *common.NormalizedRequest {
		return common.NewNormalizedRequest([]byte(body))
	}
	const hit = `{"jsonrpc":"2.0","id":42,"method":"eth_getLogs","params":[{"fromBlock":"0x1","toBlock":"0x1"}]}`
	handled, _, err := HandleNetworkPreForward(t.Context(), na, nil, req(hit))
	require.NoError(t, err)
	require.False(t, handled, "receipts not held: the request goes upstream")

	_, ok := a.resolveReceipts(t.Context(), hash, -1)
	require.True(t, ok)
	handled, resp, err := HandleNetworkPreForward(t.Context(), na, nil, req(hit))
	require.NoError(t, err)
	require.True(t, handled)
	require.True(t, resp.FromCache())
	jrr, err := resp.JsonRpcResponse(t.Context())
	require.NoError(t, err)
	var logs []json.RawMessage
	require.NoError(t, json.Unmarshal(jrr.GetResultBytes(), &logs))
	require.Len(t, logs, 2)
	require.EqualValues(t, 42, jrr.ID())

	handled, _, _ = HandleNetworkPreForward(t.Context(), na, nil, req(`{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{"fromBlock":"latest","toBlock":"latest"}]}`))
	require.False(t, handled, "a tag range is not filled")
	skip := req(hit)
	skip.SetDirectives(&common.RequestDirectives{SkipCacheRead: "true"})
	handled, _, _ = HandleNetworkPreForward(t.Context(), na, nil, skip)
	require.False(t, handled, "skip-cache-read asks upstream")
}
