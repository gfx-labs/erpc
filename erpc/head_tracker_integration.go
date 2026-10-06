package erpc

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/erpc/erpc/blockstore"
	"github.com/erpc/erpc/common"
	"go.opentelemetry.io/otel/trace"
)

// initHeadTracker starts the fleet head tracker for an EVM network that opts
// in (evm.headTracker.enabled). Networks without it get no tracker at all:
// n.headTracker stays nil and every integration point below is a no-op.
func (nr *NetworksRegistry) initHeadTracker(network *Network, nwCfg *common.NetworkConfig) error {
	if nwCfg.Architecture != common.ArchitectureEvm || !nwCfg.Evm.HeadTrackerEnabled() {
		return nil
	}
	if nr.upstreamsRegistry == nil || nr.upstreamsRegistry.SharedStateRegistry() == nil {
		return fmt.Errorf("evm.headTracker requires a sharedState registry")
	}
	cfg := nwCfg.Evm.HeadTracker
	cfg.SetDefaults()
	if err := cfg.Validate(); err != nil {
		return err
	}
	ht := newHeadTracker(network.projectId, network.networkId, network.Label(), cfg,
		nr.upstreamsRegistry.SharedStateRegistry(), network.headTrackerDeps(), network.logger)
	network.headTracker = ht
	if c := network.blockStore; c != nil {
		// Every replica's block store follows the published head at once
		// instead of waiting for its next tick.
		ht.OnHead(func(int64) { c.Kick() })
	}
	ht.Start(nr.appCtx)
	return nil
}

// headTrackerPollKey marks the leader's own poll so the "latest" rewrite and
// the local eth_blockNumber answer never apply to it.
type headTrackerPollKey struct{}

func (n *Network) headTrackerDeps() headTrackerDeps {
	return headTrackerDeps{
		poll:      n.headTrackerPoll,
		blockTime: n.EvmBlockTime,
		verifyChainId: func(ctx context.Context, u common.Upstream) (bool, error) {
			eu, ok := u.(common.EvmUpstream)
			if !ok || n.cfg == nil || n.cfg.Evm == nil || n.cfg.Evm.ChainId == 0 {
				return true, nil
			}
			got, err := eu.EvmGetChainId(ctx)
			if err != nil {
				return false, err
			}
			return got == strconv.FormatInt(n.cfg.Evm.ChainId, 10), nil
		},
		majorMove:  n.headTrackerMajorMove,
		onAccepted: n.onHeadTrackerAccepted,
		fallbackHead: func(ctx context.Context) int64 {
			return n.evmPollerLatestBlockNumber(ctx, trace.SpanFromContext(ctx))
		},
	}
}

// headTrackerMajorMove mirrors the state poller's major-head-move threshold:
// one minute of chain progress, clamped to [2, rollback tolerance] blocks.
func (n *Network) headTrackerMajorMove() int64 {
	bt := n.EvmBlockTime()
	if bt <= 0 {
		return common.DefaultToleratedBlockHeadRollback
	}
	return min(max(int64(time.Minute/bt), 2), common.DefaultToleratedBlockHeadRollback)
}

// headTrackerPoll fetches "latest" through the network's normal forwarding:
// selection policy, failover, retry and integrity checks all apply (the
// request is deliberately NOT IsInternal, which would skip integrity). It
// skips cache reads (freshness) and the multiplexer/block-store serve path
// (a client's in-flight or cached "latest" must not stand in for the poll),
// and keeps "latest" as the tag on the wire.
func (n *Network) headTrackerPoll(ctx context.Context, full bool) (*headObservation, error) {
	jrq := common.NewJsonRpcRequest("eth_getBlockByNumber", []interface{}{"latest", full})
	if err := jrq.SetID(1); err != nil {
		return nil, err
	}
	rq := common.NewNormalizedRequestFromJsonRpcRequest(jrq)
	rq.SetDirectives(&common.RequestDirectives{SkipCacheRead: "true", SkipInterpolation: true, RetryEmpty: true})
	pctx := context.WithValue(withBlockStoreBypass(ctx), headTrackerPollKey{}, true)
	resp, err := n.Forward(pctx, rq)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("empty response")
	}
	defer resp.Release()
	jrr, err := resp.JsonRpcResponse(ctx)
	if err != nil {
		return nil, err
	}
	if jrr.Error != nil {
		return nil, jrr.Error
	}
	raw := append(json.RawMessage(nil), jrr.GetResultBytes()...)
	obs, err := parseHeadObservation(raw)
	if err != nil || obs == nil {
		return obs, err
	}
	obs.Full = full
	obs.Upstream = resp.Upstream()
	return obs, nil
}

// onHeadTrackerAccepted runs on the leader for each new head. The response
// already passed the network's integrity checks inside Forward, so it is
// written under its concrete keys for every replica to reuse:
//   - the block-time EMA gets the on-chain timestamp (the slow pollers alone
//     would sample it once a minute);
//   - the JSON-RPC cache gets eth_getBlockByNumber(<hex>, full) and
//     eth_getBlockByHash(<hash>, full), plus the hashes-only form derived
//     from a full block, so one call fills both variants;
//   - the block store adopts the header (and body when full) as canonical.
//
// Nothing is fabricated: a hashes-only poll never writes a full-block entry.
func (n *Network) onHeadTrackerAccepted(ctx context.Context, obs *headObservation) {
	if n.metricsTracker != nil && obs.Timestamp > 0 {
		n.metricsTracker.ObserveNetworkHead(n.networkId, n.Label(), obs.Number, obs.Timestamp)
	}
	variants := map[bool]json.RawMessage{obs.Full: obs.Raw}
	if obs.Full {
		if hashes, err := (&blockstore.BlockRecord{Block: obs.Raw}).BlockJSON(false); err == nil {
			variants[false] = hashes
		}
	}
	if c := n.blockStore; c != nil {
		c.AdoptBlock(ctx, obs.Raw, obs.Full, true, false)
	}
	if n.cacheDal == nil || n.cacheDal.IsObjectNull() {
		return
	}
	hexNum := fmt.Sprintf("0x%x", obs.Number)
	for full, raw := range variants {
		n.headTrackerCacheSet(ctx, "eth_getBlockByNumber", []interface{}{hexNum, full}, raw, obs.Upstream)
		if obs.Hash != "" {
			n.headTrackerCacheSet(ctx, "eth_getBlockByHash", []interface{}{obs.Hash, full}, raw, obs.Upstream)
		}
	}
}

func (n *Network) headTrackerCacheSet(ctx context.Context, method string, params []interface{}, result json.RawMessage, ups common.Upstream) {
	jrq := common.NewJsonRpcRequest(method, params)
	_ = jrq.SetID(1)
	rq := common.NewNormalizedRequestFromJsonRpcRequest(jrq)
	rq.SetNetwork(n)
	jrr, err := common.NewJsonRpcResponseFromBytes([]byte("1"), result, nil)
	if err != nil {
		return
	}
	resp := common.NewNormalizedResponse().WithRequest(rq).WithJsonRpcResponse(jrr)
	if ups != nil {
		// Finality of the entry is judged against the serving upstream's view.
		resp.SetUpstream(ups)
	}
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := n.cacheDal.Set(sctx, rq, resp); err != nil {
		n.logger.Debug().Err(err).Str("method", method).Msg("head tracker could not cache the polled block")
	}
}

// tryServeHeadTracker answers from the tracker head without any upstream call
// (the venn head replacer), and rewrites eth_getBlockByNumber("latest") to the
// concrete head so it is served by the block store or the cache entries the
// leader wrote. Only when the tracker is enabled and fresh, and never for the
// leader's own poll or a request pinned to an upstream subset.
func (n *Network) tryServeHeadTracker(ctx context.Context, req *common.NormalizedRequest, method string) (*common.NormalizedResponse, bool) {
	ht := n.headTracker
	if ht == nil || ctx.Value(headTrackerPollKey{}) != nil {
		return nil, false
	}
	dirs := req.Directives()
	if dirs != nil && (dirs.UseUpstream != "" || dirs.SkipInterpolation) {
		return nil, false
	}
	switch method {
	case "eth_blockNumber":
		head := ht.FreshHead()
		if head <= 0 {
			return nil, false
		}
		jrr, err := common.NewJsonRpcResponse(req.ID(), fmt.Sprintf("0x%x", head), nil)
		if err != nil {
			return nil, false
		}
		resp := common.NewNormalizedResponse().WithRequest(req).WithJsonRpcResponse(jrr)
		resp.SetEvmBlockNumber(head)
		return resp, true
	case "eth_getBlockByNumber":
		n.rewriteLatestToTrackerHead(ctx, req)
	}
	return nil, false
}

// rewriteLatestToTrackerHead turns eth_getBlockByNumber("latest", x) into
// eth_getBlockByNumber(<tracker head>, x). The block was observed by the
// leader through normal routing; an upstream that has not seen it yet is
// skipped by missing-data retry like any explicit-number read near the head.
func (n *Network) rewriteLatestToTrackerHead(ctx context.Context, req *common.NormalizedRequest) {
	jrq, err := req.JsonRpcRequest(ctx)
	if err != nil {
		return
	}
	jrq.RLock()
	isLatest := len(jrq.Params) == 2
	if isLatest {
		s, ok := jrq.Params[0].(string)
		isLatest = ok && strings.EqualFold(s, "latest")
	}
	jrq.RUnlock()
	if !isLatest {
		return
	}
	head := n.headTracker.FreshHead()
	if head <= 0 {
		return
	}
	jrq.Lock()
	params := append([]interface{}(nil), jrq.Params...)
	params[0] = fmt.Sprintf("0x%x", head)
	jrq.Params = params
	jrq.Unlock()
	jrq.InvalidateCacheHash()
	// The tag-derived block reference and finality were memoized earlier
	// (project-level metrics); re-derive them for the concrete number so the
	// cache lookup uses the same key and policy the leader's write did.
	req.SetEvmBlockNumber(head)
	req.SetEvmBlockRef(strconv.FormatInt(head, 10))
	req.SetFinality(n.GetFinality(ctx, req, nil))
}
