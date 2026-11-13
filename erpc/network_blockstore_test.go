package erpc

import (
	"testing"

	"github.com/erpc/erpc/common"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// An enabled block store must name a declared connector; disabled or
// absent, nothing is attached and nothing is required.
func TestInitBlockStore_ConnectorRequired(t *testing.T) {
	lg := zerolog.Nop()
	nr := &NetworksRegistry{logger: &lg}
	nw := &Network{networkId: "evm:1", projectId: "p"}
	require.NoError(t, nr.initBlockStore(nw, &common.NetworkConfig{Evm: &common.EvmNetworkConfig{ChainId: 1}}))
	require.NoError(t, nr.initBlockStore(nw, &common.NetworkConfig{Evm: &common.EvmNetworkConfig{ChainId: 1, BlockStore: &common.EvmBlockStoreConfig{ConnectorId: "x"}}}))
	err := nr.initBlockStore(nw, &common.NetworkConfig{Evm: &common.EvmNetworkConfig{ChainId: 1, BlockStore: &common.EvmBlockStoreConfig{Enabled: true, ConnectorId: "missing"}}})
	require.ErrorContains(t, err, "missing")
}
