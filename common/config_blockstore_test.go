package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEvmBlockStoreConfig_Defaults(t *testing.T) {
	c := &EvmBlockStoreConfig{Enabled: true, ConnectorId: "redis", LogsFill: &EvmBlockStoreLogsFillConfig{Enabled: true}, Historical: &EvmBlockStoreHistoricalConfig{Enabled: true}}
	c.SetDefaults()
	require.Equal(t, "default", c.Namespace)
	require.Equal(t, Duration(10*time.Minute), c.TTL)
	require.Equal(t, Duration(time.Second), *c.PeerWait)
	require.EqualValues(t, 32<<20, c.MaxPayloadBytes)
	require.EqualValues(t, 10, c.LogsFill.MaxRange)
	require.Equal(t, Duration(24*time.Hour), c.Historical.TTL)
	require.NoError(t, c.Validate())

	zero := Duration(0)
	c2 := &EvmBlockStoreConfig{PeerWait: &zero}
	c2.SetDefaults()
	require.Equal(t, Duration(0), *c2.PeerWait, "an explicit 0 turns coordination off")
}

func TestEvmBlockStoreConfig_Validate(t *testing.T) {
	var nilCfg *EvmBlockStoreConfig
	require.NoError(t, nilCfg.Validate())
	require.NoError(t, (&EvmBlockStoreConfig{}).Validate(), "disabled is never checked")
	require.Error(t, (&EvmBlockStoreConfig{Enabled: true}).Validate(), "connectorId required")
	big := Duration(time.Minute)
	require.Error(t, (&EvmBlockStoreConfig{Enabled: true, ConnectorId: "r", PeerWait: &big}).Validate())
	require.Error(t, (&EvmBlockStoreConfig{Enabled: true, ConnectorId: "r", LogsFill: &EvmBlockStoreLogsFillConfig{Enabled: true, MaxRange: 5000}}).Validate())
}

func TestEvmNetworkConfig_BlockStoreDefaultsOnlyWhenSet(t *testing.T) {
	e := &EvmNetworkConfig{ChainId: 1}
	require.NoError(t, e.SetDefaults())
	require.Nil(t, e.BlockStore, "absent stays absent: zero behaviour change")
	e = &EvmNetworkConfig{ChainId: 1, BlockStore: &EvmBlockStoreConfig{Enabled: true, ConnectorId: "r"}}
	require.NoError(t, e.SetDefaults())
	require.Equal(t, "default", e.BlockStore.Namespace)
	require.NoError(t, e.Validate())
}
