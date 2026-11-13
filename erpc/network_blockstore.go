package erpc

import (
	"fmt"
	"time"

	"github.com/erpc/erpc/architecture/evm"
	"github.com/erpc/erpc/blockstore"
	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/data"
)

// initBlockStore attaches the fleet-shared block store (evm.blockStore) to
// network's chain trackers, and starts historical promotion when that is
// on. Disabled or absent: nothing changes.
func (nr *NetworksRegistry) initBlockStore(network *Network, nwCfg *common.NetworkConfig) error {
	if nwCfg == nil || nwCfg.Evm == nil || nwCfg.Evm.BlockStore == nil || !nwCfg.Evm.BlockStore.Enabled {
		return nil
	}
	cfg := *nwCfg.Evm.BlockStore
	cfg.SetDefaults()
	var conn data.Connector
	if nr.evmJsonRpcCache != nil {
		conn = nr.evmJsonRpcCache.Connector(cfg.ConnectorId)
	}
	if conn == nil {
		return fmt.Errorf("evm.blockStore.connectorId %q is not a connector of database.evmJsonRpcCache", cfg.ConnectorId)
	}
	return attachBlockStore(network, conn, &cfg)
}

// attachBlockStore attaches a store over conn to network per cfg (with
// defaults set).
func attachBlockStore(network *Network, conn data.Connector, cfg *common.EvmBlockStoreConfig) error {
	opt := blockstore.Options{
		Namespace:       cfg.Namespace,
		ProjectId:       network.projectId,
		Network:         network.networkId,
		TTL:             cfg.TTL.Duration(),
		MaxPayloadBytes: cfg.MaxPayloadBytes,
		LeaseTTL:        10 * time.Second,
	}
	if cfg.PeerWait != nil {
		opt.PeerWait = cfg.PeerWait.Duration()
	}
	if h := cfg.Historical; h != nil && h.Enabled {
		opt.HistoricalTTL = h.TTL.Duration()
	}
	var logsFill int64
	if lf := cfg.LogsFill; lf != nil && lf.Enabled {
		logsFill = lf.MaxRange
	}
	store := blockstore.New(conn, opt)
	evm.SetChainStore(network, store, logsFill)
	if store.Historical() && network.appCtx != nil {
		network.bg.Go(func() { network.promoteFinalizedLoop(network) })
	}
	return nil
}

// promoteFinalizedLoop promotes newly finalized pins to the historical
// store, once per block time (at most every 2s, at least every 30s), until
// the network's ctx ends.
func (n *Network) promoteFinalizedLoop(_ *Network) {
	ctx := n.appCtx
	for {
		bt := n.EvmBlockTime()
		wait := min(max(bt, 2*time.Second), 30*time.Second)
		select {
		case <-ctx.Done():
			evm.SetChainStore(n, nil, 0)
			return
		case <-time.After(wait):
		}
		if n.chainTracker == nil {
			continue
		}
		fin := common.EvmHeadOf(n, ctx, nil).Finalized
		evm.PromoteFinalized(ctx, n, n.chainTracker, fin)
	}
}
