package evm

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/erpc/erpc/architecture/evm/logfilter"
	"github.com/erpc/erpc/blockstore"
	"github.com/erpc/erpc/common"
)

// chainStore is one network's fleet-shared block store as the chain
// tracker uses it (blockstore package). Tracker fetches of headers by hash
// and of receipts read through it first, and write to it once their checks
// pass, so N replicas cost about one upstream fetch per block. Every miss,
// invalid entry or store error falls through to the tracker's own sweep.
type chainStore struct {
	store    *blockstore.Store
	logsFill int64 // logs fill's maxRange; 0 = off

	// promoted is the highest finalized height promoted to the historical
	// store; promoting runs on one goroutine at a time.
	promoteMu sync.Mutex
	promoted  int64
}

var chainStores sync.Map // common.Network -> *chainStore

// SetChainStore attaches s to n's trackers, logsFill being the logs fill's
// maxRange (0 off). Attach before the network serves; nil detaches.
func SetChainStore(n common.Network, s *blockstore.Store, logsFill int64) {
	if s == nil {
		chainStores.Delete(n)
		return
	}
	chainStores.Store(n, &chainStore{store: s, logsFill: logsFill})
}

func chainStoreOf(n common.Network) *chainStore {
	if n == nil {
		return nil
	}
	if v, ok := chainStores.Load(n); ok {
		return v.(*chainStore)
	}
	return nil
}

// storeKind is the payload kind a tracker fetch shares, "" when none:
// headers by hash and receipts by hash. By-number and logs fetches are not
// shared: a number is not a block, and logs from eth_getLogs prove less
// than receipts.
func storeKind(spec fetchSpec) blockstore.Kind {
	if spec.fresh || spec.upstream != "" {
		// A refetch after a wrong answer or one aimed at a named upstream
		// asks that upstream, not the fleet.
		return ""
	}
	switch spec.method {
	case "eth_getBlockByHash":
		return blockstore.KindHeader
	case "eth_getBlockReceipts":
		return blockstore.KindReceipts
	}
	return ""
}

// storeHash is the block hash of a by-hash fetch's params.
func storeHash(spec fetchSpec) string {
	var params []any
	if err := common.SonicCfg.UnmarshalFromString(spec.params, &params); err != nil || len(params) == 0 {
		return ""
	}
	s, _ := params[0].(string)
	key, ok := hashKey(s)
	if !ok {
		return ""
	}
	return key
}

// readThrough is a tracker fetch through the shared store: the stored
// payload when one decodes and passes the reader's checks; else, under the
// block's lease, the sweep, whose checked answer is then stored; else,
// while another replica holds the lease, its stored payload once it lands
// within PeerWait; else the sweep. ok false: the store is not involved.
func (s *fetchScheduler) readThrough(ctx context.Context, r *fetchRequest, sweep func() fetchResult) (res fetchResult, ok bool) {
	defer func() {
		// A panic in the store path never fails the fetch it fronts.
		if rec := recover(); rec != nil {
			s.network.Logger().Error().Interface("panic", rec).Msg("blockstore read-through panicked")
			res, ok = fetchResult{}, false
		}
	}()
	cs := chainStoreOf(s.network)
	if cs == nil {
		return fetchResult{}, false
	}
	kind := storeKind(r.spec)
	hash := ""
	if kind != "" {
		hash = storeHash(r.spec)
	}
	if hash == "" {
		return fetchResult{}, false
	}
	if res, ok := s.fromStore(ctx, cs, r.spec, kind, hash, nil); ok {
		return res, true
	}
	release, leased := cs.store.Lease(ctx, kind, hash)
	if !leased {
		if raw, ok := cs.store.AwaitPeer(ctx, kind, hash); ok {
			if res, ok := s.fromStore(ctx, cs, r.spec, kind, hash, raw); ok {
				return res, true
			}
		}
		res = sweep()
		s.toStore(cs, kind, hash, res)
		return res, true
	}
	defer release()
	// The holder before this one may have stored it meanwhile.
	if res, ok := s.fromStore(ctx, cs, r.spec, kind, hash, nil); ok {
		return res, true
	}
	res = sweep()
	s.toStore(cs, kind, hash, res)
	return res, true
}

// fromStore decodes kind's stored payload of hash (raw, else read now)
// with spec's decode, the same answer check a sweep runs, after the
// blockstore's own validation of receipts against the held header.
func (s *fetchScheduler) fromStore(ctx context.Context, cs *chainStore, spec fetchSpec, kind blockstore.Kind, hash string, raw []byte) (fetchResult, bool) {
	if raw == nil {
		var ok bool
		if raw, ok = cs.store.Get(ctx, kind, hash); !ok {
			return fetchResult{}, false
		}
	}
	if kind == blockstore.KindReceipts {
		if err := s.validateStored(hash, raw); err != nil {
			cs.store.CountInvalid(kind)
			return fetchResult{}, false
		}
	}
	v, err := spec.decode(bytes.Clone(raw), nil)
	if err != nil {
		cs.store.CountInvalid(kind)
		return fetchResult{}, false
	}
	return fetchResult{val: v}, true
}

// validateStored checks stored receipts against the held header of hash
// (bloom recompute, hashes, tx dedupe); with no held header, the decode's
// own check is all there is, as for a cached answer.
func (s *fetchScheduler) validateStored(hash string, raw []byte) error {
	h := s.heldHeaderAny(hash)
	if h == nil {
		return nil
	}
	bh, err := blockstore.ParseHeader(h.raw)
	if err != nil {
		return nil
	}
	return blockstore.ValidateReceipts(bh, raw, !s.bloom.prof().checkOff)
}

// heldHeaderAny is the header of hash any tracker of the network holds.
func (s *fetchScheduler) heldHeaderAny(hash string) *chainHeader {
	nt := registeredTrackers(s.network)
	if nt == nil {
		return nil
	}
	var h [32]byte
	if !decodeHex32(hash, &h) {
		return nil
	}
	for _, t := range nt.trackers() {
		if ch := t.heldHeader(h); ch != nil {
			return ch
		}
	}
	return nil
}

// toStore shares a sweep's checked answer. A header is shared once its
// hash recomputes, or at least decodes for the hash asked; receipts once
// they passed the decode and blockstore.ValidateReceipts against a held
// header, never without one: unchecked receipts must not reach the fleet.
func (s *fetchScheduler) toStore(cs *chainStore, kind blockstore.Kind, hash string, res fetchResult) {
	if res.err != nil || res.val == nil || res.upstream == "" {
		return
	}
	var raw []byte
	switch v := res.val.(type) {
	case *chainHeader:
		if kind != blockstore.KindHeader || v.hashHex != hash {
			return
		}
		raw = v.raw
	case *heldReceipts:
		if kind != blockstore.KindReceipts || v.fromLogs || v.bloom == bloomMismatch {
			return
		}
		h := v.h
		if h == nil {
			h = s.heldHeaderAny(hash)
		}
		if h == nil {
			return
		}
		bh, err := blockstore.ParseHeader(h.raw)
		if err != nil || blockstore.ValidateReceipts(bh, v.raw, !s.bloom.prof().checkOff) != nil {
			cs.store.CountInvalid(kind)
			return
		}
		raw = v.raw
	default:
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), 5*time.Second)
	defer cancel()
	_ = cs.store.Put(ctx, kind, hash, raw, 0)
}

// LogsFill answers an eth_getLogs over an explicit hex range of at most
// the fill's maxRange blocks, at or below the view's Head, from each
// block's complete receipts: held by a tracker, else in the shared store.
// Every block of the range must be pinned and its receipts checked; any
// gap leaves ok false and the request goes upstream as before.
func LogsFill(ctx context.Context, n common.Network, t *ChainTracker, f *logfilter.LogsFilter) (logs []json.RawMessage, ok bool) {
	cs := chainStoreOf(n)
	if cs == nil || cs.logsFill <= 0 || t == nil || f == nil || f.BlockHash != "" {
		return nil, false
	}
	from, to, concrete := f.ConcreteRange()
	if !concrete || to-from+1 > cs.logsFill {
		return nil, false
	}
	state := t.state.Load()
	vh := state.viewHead()
	if vh == nil || uint64(to) > vh.number {
		cs.store.CountLogsFill("above_head")
		return nil, false
	}
	gate := t.fetch.bloom
	out := []json.RawMessage{}
	for num := from; num <= to; num++ {
		h := state.pinAt(uint64(num))
		if h == nil {
			cs.store.CountLogsFill("unpinned")
			return nil, false
		}
		// A header that proves no log matches needs no receipts; trust in
		// the bloom is the logs gate's, so only the proofs that need none.
		if gate.emptySkips(h) || (!gate.prof().checkOff && h.hashRecomputes() && !f.MayMatch(h.bloom)) {
			continue
		}
		r := t.heldReceiptsOf(h.hash)
		if r == nil || r.fromLogs {
			r = storedReceipts(ctx, cs, h, !gate.prof().checkOff)
		}
		if r == nil {
			cs.store.CountLogsFill("gap")
			return nil, false
		}
		out = append(out, r.match(f)...)
	}
	cs.store.CountLogsFill("served")
	return out, true
}

// storedReceipts is block h's receipts from the shared store, validated
// against h.
func storedReceipts(ctx context.Context, cs *chainStore, h *chainHeader, checkBloom bool) *heldReceipts {
	raw, ok := cs.store.Get(ctx, blockstore.KindReceipts, h.hashHex)
	if !ok {
		return nil
	}
	bh, err := blockstore.ParseHeader(h.raw)
	if err != nil || blockstore.ValidateReceipts(bh, raw, checkBloom) != nil {
		cs.store.CountInvalid(blockstore.KindReceipts)
		return nil
	}
	return indexReceipts(h.hash, raw)
}

// logsFillResponse is LogsFill's answer to req, or nil when it has none.
func logsFillResponse(ctx context.Context, n common.Network, nrq *common.NormalizedRequest, f *logfilter.LogsFilter) *common.NormalizedResponse {
	cs := chainStoreOf(n)
	if cs == nil || cs.logsFill <= 0 {
		return nil
	}
	if d := nrq.Directives(); d != nil && (d.UseUpstream != "" || nrq.ShouldSkipCacheRead("")) {
		// A client that names upstreams or skips the cache asks them, not us.
		return nil
	}
	nt := registeredTrackers(n)
	if nt == nil {
		return nil
	}
	t := lookupChainTracker(n, "")
	logs, ok := LogsFill(ctx, n, t, f)
	if !ok {
		return nil
	}
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, l := range logs {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(l)
	}
	buf.WriteByte(']')
	jrr, err := common.NewJsonRpcResponseFromBytes(nil, buf.Bytes(), nil)
	if err != nil {
		return nil
	}
	if jrq, err := nrq.JsonRpcRequest(ctx); err == nil {
		_ = jrr.SetID(jrq.ID)
	}
	return common.NewNormalizedResponse().WithRequest(nrq).WithJsonRpcResponse(jrr).SetFromCache(true)
}

// PromoteFinalized copies the shared headers and receipts of t's pins
// from the last promoted height up to finalized into the historical store
// with its TTL, and indexes each height's hash. Only blocks the tracker
// pins (hash-linked to its head) at or below finalized are promoted, and
// only payloads already shared: it fetches nothing.
func PromoteFinalized(ctx context.Context, n common.Network, t *ChainTracker, finalized int64) int {
	cs := chainStoreOf(n)
	if cs == nil || !cs.store.Historical() || t == nil || finalized <= 0 {
		return 0
	}
	if !cs.promoteMu.TryLock() {
		return 0
	}
	defer cs.promoteMu.Unlock()
	state := t.state.Load()
	if len(state.pins) == 0 {
		return 0
	}
	lo := max(cs.promoted+1, int64(state.base))
	hi := min(finalized, int64(state.head().number))
	ttl := cs.store.Options().HistoricalTTL
	done := 0
	for num := lo; num <= hi; num++ {
		h := state.pinAt(uint64(num))
		if h == nil {
			break
		}
		if err := cs.store.PutFinalized(ctx, num, h.hashHex); err != nil {
			cs.store.CountPromote("conflict")
			cs.promoted = num
			continue
		}
		_ = cs.store.Put(ctx, blockstore.KindHeader, h.hashHex, h.raw, ttl)
		if raw, ok := cs.store.Get(ctx, blockstore.KindReceipts, h.hashHex); ok {
			_ = cs.store.Put(ctx, blockstore.KindReceipts, h.hashHex, raw, ttl)
		} else if r := t.heldReceiptsOf(h.hash); r != nil && !r.fromLogs {
			_ = cs.store.Put(ctx, blockstore.KindReceipts, h.hashHex, r.raw, ttl)
		}
		cs.promoted = num
		done++
	}
	if done > 0 {
		cs.store.CountPromote("ok")
	}
	return done
}

// HistoricalBlock is a finalized block's header and receipts (nil when
// not stored) at height num from the historical store, checked against
// each other and the index: for catch-up readers below the window.
func HistoricalBlock(ctx context.Context, n common.Network, num int64) (header, receipts json.RawMessage, ok bool) {
	cs := chainStoreOf(n)
	if cs == nil || !cs.store.Historical() {
		return nil, nil, false
	}
	hash, ok := cs.store.FinalizedHash(ctx, num)
	if !ok {
		return nil, nil, false
	}
	raw, ok := cs.store.Get(ctx, blockstore.KindHeader, hash)
	if !ok {
		return nil, nil, false
	}
	bh, err := blockstore.ParseHeader(raw)
	if err != nil || bh.Hash != hash || bh.Number != num {
		cs.store.CountInvalid(blockstore.KindHeader)
		return nil, nil, false
	}
	if rr, ok := cs.store.Get(ctx, blockstore.KindReceipts, hash); ok {
		if blockstore.ValidateReceipts(bh, rr, true) == nil {
			receipts = rr
		} else {
			cs.store.CountInvalid(blockstore.KindReceipts)
		}
	}
	return raw, receipts, true
}

// logsFillFilter is the filter of an eth_getLogs that LogsFill may serve:
// explicit hex bounds both, no tags, no blockHash.
func logsFillFilter(filter map[string]any) *logfilter.LogsFilter {
	for _, k := range [...]string{"fromBlock", "toBlock"} {
		s, _ := filter[k].(string)
		if !strings.HasPrefix(s, "0x") && !strings.HasPrefix(s, "0X") {
			return nil
		}
	}
	return logfilter.Parse([]any{filter})
}

// logsFillFilterOf is logsFillFilter of jrq's filter, nil without one.
func logsFillFilterOf(jrq *common.JsonRpcRequest) *logfilter.LogsFilter {
	jrq.RLock()
	defer jrq.RUnlock()
	if len(jrq.Params) < 1 {
		return nil
	}
	filter, ok := jrq.Params[0].(map[string]any)
	if !ok {
		return nil
	}
	return logsFillFilter(filter)
}
