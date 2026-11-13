package blockstore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/erpc/erpc/architecture/evm/logfilter"
	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/data"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func h32(tag string, n int) string {
	s := fmt.Sprintf("%s%x", tag, n)
	return "0x" + strings.Repeat("0", 64-len(s)) + s
}

const (
	addrA  = "0x00000000000000000000000000000000000000aa"
	topicT = "0x00000000000000000000000000000000000000000000000000000000000000f1"
)

// fixture builds a block with txs transactions, one log of addrA/topicT
// each, its header (with the right bloom) and receipts.
func fixture(t *testing.T, n, txs int) (header, receipts []byte) {
	t.Helper()
	hash := h32("b", n)
	bloom := make([]byte, logfilter.BloomLen)
	var rs []map[string]any
	var hashes []string
	for i := range txs {
		tx := h32("7", n*100+i)
		hashes = append(hashes, tx)
		addr := addrA
		if i > 0 {
			addr = fmt.Sprintf("0x%040x", 0xa0+i)
		}
		a, _ := common.HexToBytes(addr)
		tp, _ := common.HexToBytes(topicT)
		logfilter.BloomAdd(bloom, a)
		logfilter.BloomAdd(bloom, tp)
		rs = append(rs, map[string]any{
			"blockHash": hash, "blockNumber": fmt.Sprintf("0x%x", n), "transactionHash": tx,
			"logs": []map[string]any{{
				"address": addr, "topics": []string{topicT}, "data": "0x",
				"blockHash": hash, "blockNumber": fmt.Sprintf("0x%x", n), "transactionHash": tx,
				"logIndex": fmt.Sprintf("0x%x", i), "removed": false,
			}},
		})
	}
	if hashes == nil {
		hashes = []string{}
	}
	if rs == nil {
		rs = []map[string]any{}
	}
	hdr, _ := json.Marshal(map[string]any{
		"number": fmt.Sprintf("0x%x", n), "hash": hash, "parentHash": h32("b", n-1),
		"logsBloom": "0x" + fmt.Sprintf("%x", bloom), "transactions": hashes,
	})
	rec, _ := json.Marshal(rs)
	return hdr, rec
}

func TestValidateReceipts(t *testing.T) {
	hdr, rec := fixture(t, 5, 3)
	h, err := ParseHeader(hdr)
	require.NoError(t, err)
	require.NoError(t, ValidateReceipts(h, rec, true))

	var rs []map[string]any
	require.NoError(t, json.Unmarshal(rec, &rs))

	t.Run("bloom recompute catches a dropped log", func(t *testing.T) {
		cp := clone(t, rs)
		cp[1]["logs"] = []any{}
		cp[2]["logs"].([]any)[0].(map[string]any)["logIndex"] = "0x1"
		require.ErrorIs(t, ValidateReceipts(h, marshal(t, cp), true), ErrInvalid)
	})
	t.Run("bloom off accepts the same", func(t *testing.T) {
		cp := clone(t, rs)
		cp[0]["logs"].([]any)[0].(map[string]any)["topics"] = []string{h32("e", 1)}
		require.Error(t, ValidateReceipts(h, marshal(t, cp), true))
		require.NoError(t, ValidateReceipts(h, marshal(t, cp), false))
	})
	t.Run("duplicate transaction", func(t *testing.T) {
		cp := clone(t, rs)
		cp[2]["transactionHash"] = cp[0]["transactionHash"]
		require.ErrorIs(t, ValidateReceipts(h, marshal(t, cp), false), ErrInvalid)
	})
	t.Run("missing transaction", func(t *testing.T) {
		require.ErrorIs(t, ValidateReceipts(h, marshal(t, clone(t, rs)[:2]), false), ErrInvalid)
	})
	t.Run("other block", func(t *testing.T) {
		cp := clone(t, rs)
		cp[0]["blockHash"] = h32("c", 5)
		require.ErrorIs(t, ValidateReceipts(h, marshal(t, cp), false), ErrInvalid)
	})
	t.Run("null and empty", func(t *testing.T) {
		require.Error(t, ValidateReceipts(h, []byte("null"), true))
		require.Error(t, ValidateReceipts(h, []byte("[]"), true))
		eh, er := fixture(t, 6, 0)
		e, err := ParseHeader(eh)
		require.NoError(t, err)
		require.NoError(t, ValidateReceipts(e, er, true), "an empty block has empty receipts")
	})
	t.Run("extra system receipt allowed", func(t *testing.T) {
		cp := clone(t, rs)
		cp = append(cp, map[string]any{"blockHash": h32("b", 5), "blockNumber": "0x5", "transactionHash": h32("9", 1), "logs": []any{}})
		require.NoError(t, ValidateReceipts(h, marshal(t, cp), true))
	})
}

func TestParseHeader_RejectsDuplicatedTransactions(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{
		"number": "0x10", "hash": h32("b", 16), "parentHash": h32("b", 15),
		"transactions": []string{h32("a", 1), h32("a", 2), h32("a", 1)},
	})
	_, err := ParseHeader(raw)
	require.ErrorIs(t, err, ErrInvalid)
}

func TestHeaderLinks(t *testing.T) {
	a, _ := fixture(t, 5, 1)
	b, _ := fixture(t, 6, 1)
	ha, _ := ParseHeader(a)
	hb, _ := ParseHeader(b)
	require.True(t, hb.LinksTo(ha))
	require.False(t, ha.LinksTo(hb))
}

func TestBlockLogsMatch(t *testing.T) {
	_, rec := fixture(t, 5, 3)
	bl, err := LogsOf(5, h32("b", 5), rec)
	require.NoError(t, err)
	require.Len(t, bl.Logs, 3)
	f := logfilter.Parse([]any{map[string]any{"topics": []any{topicT}}})
	require.Len(t, bl.Match(f), 3)
	f = logfilter.Parse([]any{map[string]any{"address": addrA, "topics": []any{topicT}}})
	require.Len(t, bl.Match(f), 1)
	f = logfilter.Parse([]any{map[string]any{"address": "0x00000000000000000000000000000000000000bb"}})
	require.Empty(t, bl.Match(f))
}

func memConn(t *testing.T) data.Connector {
	lg := zerolog.Nop()
	c, err := data.NewMemoryConnector(t.Context(), &lg, "mem", &common.MemoryConnectorConfig{MaxItems: 1000, MaxTotalSize: "64MB"})
	require.NoError(t, err)
	return c
}

func TestStore_PutGetAndIndex(t *testing.T) {
	conn := memConn(t)
	s := New(conn, Options{ProjectId: "p", Network: "evm:1", TTL: time.Minute, HistoricalTTL: time.Hour})
	ctx := context.Background()
	_, ok := s.Get(ctx, KindReceipts, h32("b", 1))
	require.False(t, ok)
	require.NoError(t, s.Put(ctx, KindReceipts, strings.ToUpper(h32("b", 1)), []byte("[]"), 0))
	waitFor(t, func() bool { _, ok := s.Get(ctx, KindReceipts, h32("b", 1)); return ok })

	require.NoError(t, s.PutFinalized(ctx, 7, h32("b", 7)))
	waitFor(t, func() bool { _, ok := s.FinalizedHash(ctx, 7); return ok })
	require.NoError(t, s.PutFinalized(ctx, 7, h32("b", 7)), "same hash is idempotent")
	require.ErrorIs(t, s.PutFinalized(ctx, 7, h32("c", 7)), ErrIndexConflict)
	got, _ := s.FinalizedHash(ctx, 7)
	require.Equal(t, h32("b", 7), got, "the index never replaces")

	small := New(conn, Options{Network: "evm:1", MaxPayloadBytes: 4})
	require.ErrorIs(t, small.Put(ctx, KindHeader, h32("b", 2), []byte("12345"), 0), ErrTooLarge)
}

func TestStore_LeaseCoalesces(t *testing.T) {
	conn := memConn(t)
	opt := Options{Network: "evm:1", TTL: time.Minute, PeerWait: 2 * time.Second}
	a, b := New(conn, opt), New(conn, opt)
	ctx := context.Background()
	rel, ok := a.Lease(ctx, KindReceipts, h32("b", 1))
	require.True(t, ok)
	_, ok2 := b.Lease(ctx, KindReceipts, h32("b", 1))
	require.False(t, ok2, "a held lease is not granted twice")

	var wg sync.WaitGroup
	var got []byte
	wg.Add(1)
	go func() {
		defer wg.Done()
		got, _ = b.AwaitPeer(ctx, KindReceipts, h32("b", 1))
	}()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, a.Put(ctx, KindReceipts, h32("b", 1), []byte(`["x"]`), 0))
	rel()
	wg.Wait()
	require.Equal(t, `["x"]`, string(got), "the waiter takes the holder's payload")

	off := New(conn, Options{Network: "evm:1"})
	_, ok = off.Lease(ctx, KindReceipts, h32("b", 2))
	require.False(t, ok, "peerWait 0 disables coordination")
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	require.Eventually(t, cond, time.Second, 5*time.Millisecond)
}

func clone(t *testing.T, rs []map[string]any) []map[string]any {
	var out []map[string]any
	require.NoError(t, json.Unmarshal(marshal(t, rs), &out))
	return out
}

func marshal(t *testing.T, v any) []byte {
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}
