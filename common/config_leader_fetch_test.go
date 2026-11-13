package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestEvmLeaderFetchConfig(t *testing.T) {
	t.Run("unset stays nil: leader fetch off", func(t *testing.T) {
		c := &EvmChainTrackerConfig{}
		c.SetDefaults()
		require.Nil(t, c.LeaderFetch)
	})

	t.Run("defaults fill leaseTtl and minStaleAfter", func(t *testing.T) {
		c := &EvmChainTrackerConfig{LeaderFetch: &EvmLeaderFetchConfig{Enabled: true}}
		c.SetDefaults()
		require.True(t, c.LeaderFetch.Enabled)
		require.Equal(t, DefaultLeaderFetchLeaseTtl, c.LeaderFetch.LeaseTtl.Duration())
		require.Equal(t, DefaultLeaderFetchMinStaleAfter, c.LeaderFetch.MinStaleAfter.Duration())
	})

	t.Run("yaml", func(t *testing.T) {
		var c EvmChainTrackerConfig
		require.NoError(t, yaml.Unmarshal([]byte("leaderFetch:\n  enabled: true\n  leaseTtl: 2s\n  minStaleAfter: 10s\n"), &c))
		require.True(t, c.LeaderFetch.Enabled)
		require.Equal(t, 2*time.Second, c.LeaderFetch.LeaseTtl.Duration())
		require.Equal(t, 10*time.Second, c.LeaderFetch.MinStaleAfter.Duration())
	})

	t.Run("validation", func(t *testing.T) {
		e := baseValidEvmNetworkConfig()
		e.ChainTracker = &EvmChainTrackerConfig{LeaderFetch: &EvmLeaderFetchConfig{Enabled: true, LeaseTtl: Duration(time.Second)}}
		require.NoError(t, e.Validate())
		e.ChainTracker.LeaderFetch.LeaseTtl = Duration(10 * time.Millisecond)
		require.ErrorContains(t, e.Validate(), "leaseTtl")
		e.ChainTracker.LeaderFetch.LeaseTtl = Duration(time.Second)
		e.ChainTracker.LeaderFetch.MinStaleAfter = Duration(-1)
		require.ErrorContains(t, e.Validate(), "minStaleAfter")
	})
}
