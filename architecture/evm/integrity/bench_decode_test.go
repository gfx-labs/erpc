package integrity

import (
	"context"
	"testing"
)

// Real Ethereum mainnet block 0x192f5c2 (264 txs: legacy, EIP-1559, blob and
// EIP-7702) and its receipts, run through the full chain-1 check set exactly as
// the request path does. Every recompute must run to "pass", so the numbers
// measure the decode + verify cost of a real full block, not an early skip.

func benchMainnetFixture(b *testing.B, name string) realFixture {
	b.Helper()
	fx := loadRealFixtures(&testing.T{}, "mainnet")[name]
	if fx.Method == "" {
		b.Fatalf("fixture %s missing", name)
	}
	return fx
}

func BenchmarkValidateMainnetBlockFull(b *testing.B) {
	benchValidateFixture(b, "block-full", LevelIntrinsic, "transactionsRootRecompute", "blockHashRecompute")
}

func BenchmarkValidateMainnetReceipts(b *testing.B) {
	benchValidateFixture(b, "receipts", LevelIntrinsic)
}

func benchValidateFixture(b *testing.B, name string, level Level, mustPass ...string) {
	fx := benchMainnetFixture(b, name)
	t := &testing.T{}
	res := validateFixture(t, 1, level, fx)
	if res.Err != nil {
		b.Fatalf("honest mainnet %s rejected by %s: %v", name, res.RejectedCheckID, res.Err)
	}
	for _, id := range mustPass {
		if oc := outcomeOf(res, id); oc != "pass" {
			b.Fatalf("%s: %s = %q, want pass", name, id, oc)
		}
	}
	b.SetBytes(int64(len(fx.Result)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = validateFixture(t, 1, level, fx)
	}
}

// Per-accessor cost on the same block: the shared decode every block check
// reads from.
func BenchmarkDecodeMainnetBlockHeaderAndTxs(b *testing.B) {
	fx := benchMainnetFixture(b, "block-full")
	b.SetBytes(int64(len(fx.Result)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		d := newDecoded(MethodGetBlockByNumber, fx.Result)
		_ = d.Header()
		_ = d.Transactions()
	}
}

func BenchmarkCheckMainnetTransactionsRootRecompute(b *testing.B) {
	benchCheck(b, "block-full", MethodGetBlockByNumber, "transactionsRootRecompute")
}

func BenchmarkCheckMainnetBlockHashRecompute(b *testing.B) {
	benchCheck(b, "block-full", MethodGetBlockByNumber, "blockHashRecompute")
}

func benchCheck(b *testing.B, name, method, id string) {
	fx := benchMainnetFixture(b, name)
	c := checkByID(id)
	if c == nil {
		b.Fatalf("no check %s", id)
	}
	cfg := CheckConfig{Params: map[string]string{"chainId": "1"}}
	if v := c.Run(context.Background(), newDecoded(method, fx.Result), cfg); v != nil {
		b.Fatalf("%s did not pass: %+v", id, v)
	}
	b.SetBytes(int64(len(fx.Result)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Run(context.Background(), newDecoded(method, fx.Result), cfg)
	}
}

func checkByID(id string) *Check {
	for _, list := range registry {
		for _, c := range list {
			if c.ID == id {
				return c
			}
		}
	}
	return nil
}
