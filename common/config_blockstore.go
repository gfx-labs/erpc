package common

import (
	"fmt"
	"time"
)

// EvmBlockStoreConfig configures the fleet-shared block store of an EVM
// network. The chain tracker's header and receipts fetches read through it
// by block hash and write to it once their checks pass, so N replicas cost
// about one upstream fetch per block. Every miss, invalid entry or store
// error falls back to the tracker's own upstream fetch.
type EvmBlockStoreConfig struct {
	// Enabled turns the store on. Default false.
	Enabled bool `yaml:"enabled,omitempty" json:"enabled"`
	// ConnectorId names a connector declared under
	// database.evmJsonRpcCache.connectors (redis for a fleet; memory shares
	// within one process only). Required when enabled.
	ConnectorId string `yaml:"connectorId,omitempty" json:"connectorId,omitempty"`
	// Namespace isolates deployments sharing one connector. Default "default".
	Namespace string `yaml:"namespace,omitempty" json:"namespace,omitempty"`
	// TTL is the lifetime of entries for blocks above finalized. Default 10m.
	TTL Duration `yaml:"ttl,omitempty" json:"ttl,omitempty" tstype:"Duration"`
	// PeerWait is how long a replica waits for another replica that is
	// already fetching the same block before fetching itself. 0 turns
	// coordination off (each replica still reads the store first). Default 1s.
	PeerWait *Duration `yaml:"peerWait,omitempty" json:"peerWait,omitempty" tstype:"Duration"`
	// MaxPayloadBytes skips sharing any one header or receipts payload
	// larger than this. Default 32MB.
	MaxPayloadBytes int64 `yaml:"maxPayloadBytes,omitempty" json:"maxPayloadBytes,omitempty"`
	// LogsFill configures serving small eth_getLogs ranges from held or
	// stored receipts.
	LogsFill *EvmBlockStoreLogsFillConfig `yaml:"logsFill,omitempty" json:"logsFill,omitempty"`
	// Historical configures keeping finalized blocks and receipts.
	Historical *EvmBlockStoreHistoricalConfig `yaml:"historical,omitempty" json:"historical,omitempty"`
}

// EvmBlockStoreLogsFillConfig: an eth_getLogs with an explicit hex range of
// at most MaxRange blocks, every one pinned by the chain tracker, is
// answered by filtering each block's complete receipts locally. Any block
// whose receipts are neither held nor stored sends the request upstream.
type EvmBlockStoreLogsFillConfig struct {
	// Enabled opts in. Default false.
	Enabled bool `yaml:"enabled,omitempty" json:"enabled"`
	// MaxRange is the widest range (toBlock-fromBlock+1) served. Default 10.
	MaxRange int64 `yaml:"maxRange,omitempty" json:"maxRange,omitempty"`
}

// EvmBlockStoreHistoricalConfig: blocks that become finalized are kept
// with a finalized height->hash index for HistoricalTTL.
type EvmBlockStoreHistoricalConfig struct {
	// Enabled opts in. Default false.
	Enabled bool `yaml:"enabled,omitempty" json:"enabled"`
	// TTL is the lifetime of finalized entries. Default 24h.
	TTL Duration `yaml:"ttl,omitempty" json:"ttl,omitempty" tstype:"Duration"`
}

// SetDefaults fills unset fields: namespace "default", ttl 10m, peerWait
// 1s, maxPayloadBytes 32MB, logsFill.maxRange 10, historical.ttl 24h.
func (c *EvmBlockStoreConfig) SetDefaults() {
	if c == nil {
		return
	}
	if c.Namespace == "" {
		c.Namespace = "default"
	}
	if c.TTL == 0 {
		c.TTL = Duration(10 * time.Minute)
	}
	if c.PeerWait == nil {
		c.PeerWait = Duration(time.Second).Ptr()
	}
	if c.MaxPayloadBytes == 0 {
		c.MaxPayloadBytes = 32 << 20
	}
	if c.LogsFill != nil {
		cp := *c.LogsFill
		c.LogsFill = &cp
		if c.LogsFill.MaxRange == 0 {
			c.LogsFill.MaxRange = 10
		}
	}
	if c.Historical != nil {
		cp := *c.Historical
		c.Historical = &cp
		if c.Historical.TTL == 0 {
			c.Historical.TTL = Duration(24 * time.Hour)
		}
	}
}

// Validate checks an enabled store.
func (c *EvmBlockStoreConfig) Validate() error {
	if c == nil || !c.Enabled {
		return nil
	}
	if c.ConnectorId == "" {
		return fmt.Errorf("network.*.evm.blockStore.connectorId is required when blockStore is enabled")
	}
	if c.TTL < 0 {
		return fmt.Errorf("network.*.evm.blockStore.ttl must be >= 0")
	}
	if c.PeerWait != nil && (*c.PeerWait < 0 || *c.PeerWait > Duration(10*time.Second)) {
		return fmt.Errorf("network.*.evm.blockStore.peerWait must be within 0..10s")
	}
	if c.MaxPayloadBytes < 0 {
		return fmt.Errorf("network.*.evm.blockStore.maxPayloadBytes must be >= 0")
	}
	if lf := c.LogsFill; lf != nil && lf.Enabled && (lf.MaxRange < 1 || lf.MaxRange > 1000) {
		return fmt.Errorf("network.*.evm.blockStore.logsFill.maxRange must be within 1..1000")
	}
	if h := c.Historical; h != nil && h.Enabled && h.TTL < 0 {
		return fmt.Errorf("network.*.evm.blockStore.historical.ttl must be >= 0")
	}
	return nil
}
