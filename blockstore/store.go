// Package blockstore shares chain tracker payloads across eRPC replicas
// through a data.Connector (normally Redis): block headers and complete
// block receipts keyed by block hash, plus a finalized height->hash index
// for historical reads. A payload never makes a block canonical on its own:
// the chain tracker decides canonicality, and every payload read from the
// store is validated again against the header the reader holds before it is
// used. Every failure of the store (miss, invalid payload, connector error,
// lease timeout) falls back to the caller's normal upstream path.
package blockstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/data"
	"github.com/erpc/erpc/telemetry"
	"github.com/prometheus/client_golang/prometheus"
)

// Kind names one hash-addressed, immutable payload of a block.
type Kind string

const (
	// KindHeader is eth_getBlockByHash(hash, false) as the tracker holds it.
	KindHeader Kind = "header"
	// KindReceipts is eth_getBlockReceipts(hash): every receipt of the block.
	KindReceipts Kind = "receipts"
)

// ErrTooLarge is returned by Put for a payload above MaxPayloadBytes.
var ErrTooLarge = errors.New("blockstore: payload exceeds maxPayloadBytes")

// ErrIndexConflict is returned by PutFinalized when another hash is
// already indexed at the height: the index never replaces an entry.
var ErrIndexConflict = errors.New("blockstore: conflicting finalized index entry")

// MetricOps counts store operations by kind (header, receipts, index,
// logs), op (get, put, lease, logs_fill) and outcome.
var MetricOps = telemetry.DefineCounter(prometheus.CounterOpts{
	Namespace: "erpc",
	Name:      "blockstore_ops_total",
	Help:      "Blockstore operations by kind (header, receipts, index, logs), op (get, put, lease, logs_fill, promote) and outcome.",
}, []string{"project", "network", "kind", "op", "outcome"})

// Options configure one network's Store.
type Options struct {
	Namespace string
	ProjectId string
	// Network is the network label used for keys and metrics.
	Network string
	// TTL is the lifetime of tip payloads (blocks above finalized).
	TTL time.Duration
	// HistoricalTTL is the lifetime of finalized payloads and index entries
	// written by promotion; 0 disables the historical store.
	HistoricalTTL time.Duration
	// PeerWait bounds how long a replica waits for another replica that
	// holds a block's fetch lease before it fetches itself. 0 disables
	// cross-replica coalescing (every replica reads the store, then fetches).
	PeerWait time.Duration
	// LeaseTTL is the expiry of a fetch lease, so a replica that dies while
	// holding one blocks the others at most this long.
	LeaseTTL time.Duration
	// MaxPayloadBytes rejects larger payloads on Put and Get; 0 = no cap.
	MaxPayloadBytes int64
}

// Store is one network's view of the shared store.
type Store struct {
	conn      data.Connector
	opt       Options
	partition string
}

// New returns a Store over conn; nil when conn is nil.
func New(conn data.Connector, opt Options) *Store {
	if conn == nil {
		return nil
	}
	ns := opt.Namespace
	if ns == "" {
		ns = "default"
	}
	return &Store{
		conn:      conn,
		opt:       opt,
		partition: fmt.Sprintf("blockstore/%s/%s/%s", ns, opt.ProjectId, opt.Network),
	}
}

// Options returns the store's options.
func (s *Store) Options() Options { return s.opt }

// Historical reports whether finalized payloads are kept.
func (s *Store) Historical() bool { return s != nil && s.opt.HistoricalTTL > 0 }

func (s *Store) count(kind, op, outcome string) {
	MetricOps.WithLabelValues(s.opt.ProjectId, s.opt.Network, kind, op, outcome).Inc()
}

// NormHash lowercases a 0x-prefixed hash.
func NormHash(h string) string { return strings.ToLower(strings.TrimSpace(h)) }

func payloadKey(kind Kind, hash string) string { return string(kind) + ":" + NormHash(hash) }

func indexKey(n int64) string { return "f:" + strconv.FormatInt(n, 10) }

// Get reads kind's payload of hash. ok is false on a miss, an oversized
// value or any connector error; the payload still has to be validated by
// the caller against the header it holds.
func (s *Store) Get(ctx context.Context, kind Kind, hash string) ([]byte, bool) {
	if s == nil {
		return nil, false
	}
	v, err := s.conn.Get(ctx, data.ConnectorMainIndex, s.partition, payloadKey(kind, hash), nil)
	switch {
	case err != nil && common.HasErrorCode(err, common.ErrCodeRecordNotFound):
		s.count(string(kind), "get", "miss")
		return nil, false
	case err != nil:
		s.count(string(kind), "get", "error")
		return nil, false
	case len(v) == 0:
		s.count(string(kind), "get", "miss")
		return nil, false
	case s.opt.MaxPayloadBytes > 0 && int64(len(v)) > s.opt.MaxPayloadBytes:
		s.count(string(kind), "get", "too_large")
		return nil, false
	}
	s.count(string(kind), "get", "hit")
	// Connectors may hand out a view of their own memory.
	return append([]byte(nil), v...), true
}

// CountInvalid records that a payload read from the store failed the
// reader's validation.
func (s *Store) CountInvalid(kind Kind) {
	if s != nil {
		s.count(string(kind), "get", "invalid")
	}
}

// Put writes kind's payload of hash with ttl (0 = the tip TTL). The caller
// validated raw before.
func (s *Store) Put(ctx context.Context, kind Kind, hash string, raw []byte, ttl time.Duration) error {
	if s == nil {
		return nil
	}
	if s.opt.MaxPayloadBytes > 0 && int64(len(raw)) > s.opt.MaxPayloadBytes {
		s.count(string(kind), "put", "too_large")
		return ErrTooLarge
	}
	if ttl <= 0 {
		ttl = s.opt.TTL
	}
	if err := s.conn.Set(ctx, s.partition, payloadKey(kind, hash), raw, &ttl); err != nil {
		s.count(string(kind), "put", "error")
		return err
	}
	s.count(string(kind), "put", "ok")
	return nil
}

// leaseTry bounds one lease attempt: the connector is asked once, not
// waited on, since a holder's result is picked up by AwaitPeer polling.
var leaseTry = 100 * time.Millisecond

// awaitPoll is how often AwaitPeer reads the store.
var awaitPoll = 25 * time.Millisecond

// Lease tries once to take the fetch lease of kind's payload of hash.
// acquired is false when another replica holds it, coordination is off
// (PeerWait 0) or the connector cannot lock; release is always safe to
// call. The lease only coalesces fetches, it never gates correctness: a
// caller without it waits for the holder with AwaitPeer and then fetches
// on its own.
func (s *Store) Lease(ctx context.Context, kind Kind, hash string) (release func(), acquired bool) {
	noop := func() {}
	if s == nil || s.opt.PeerWait <= 0 {
		return noop, false
	}
	lctx, cancel := context.WithTimeout(ctx, min(leaseTry, s.opt.PeerWait))
	defer cancel()
	ttl := s.opt.LeaseTTL
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	lock, err := s.conn.Lock(lctx, s.partition+"/lease/"+payloadKey(kind, hash), ttl)
	if err != nil || lock == nil || lock.IsNil() {
		if lctx.Err() != nil {
			s.count(string(kind), "lease", "waited")
		} else {
			s.count(string(kind), "lease", "error")
		}
		return noop, false
	}
	s.count(string(kind), "lease", "acquired")
	return func() {
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = lock.Unlock(uctx)
	}, true
}

// AwaitPeer polls the store for kind's payload of hash for up to PeerWait,
// while another replica holds its lease; ok false when the wait ended
// without one.
func (s *Store) AwaitPeer(ctx context.Context, kind Kind, hash string) ([]byte, bool) {
	if s == nil || s.opt.PeerWait <= 0 {
		return nil, false
	}
	deadline := time.Now().Add(s.opt.PeerWait)
	t := time.NewTicker(awaitPoll)
	defer t.Stop()
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, false
		case <-t.C:
		}
		if v, ok := s.Get(ctx, kind, hash); ok {
			s.count(string(kind), "lease", "peer_hit")
			return v, true
		}
	}
	s.count(string(kind), "lease", "peer_timeout")
	return nil, false
}

// FinalizedHash is the hash indexed at finalized height n.
func (s *Store) FinalizedHash(ctx context.Context, n int64) (string, bool) {
	if !s.Historical() || n < 0 {
		return "", false
	}
	v, err := s.conn.Get(ctx, data.ConnectorMainIndex, s.partition, indexKey(n), nil)
	if err != nil || len(v) == 0 {
		if err == nil || common.HasErrorCode(err, common.ErrCodeRecordNotFound) {
			s.count("index", "get", "miss")
		} else {
			s.count("index", "get", "error")
		}
		return "", false
	}
	s.count("index", "get", "hit")
	return NormHash(string(v)), true
}

// PutFinalized indexes hash at finalized height n. An entry that already
// names another hash stays and ErrIndexConflict is returned: a finalized
// height never changes, so a conflict means one writer saw a wrong chain.
func (s *Store) PutFinalized(ctx context.Context, n int64, hash string) error {
	if !s.Historical() {
		return nil
	}
	hash = NormHash(hash)
	if cur, ok := s.FinalizedHash(ctx, n); ok {
		if cur != hash {
			s.count("index", "put", "conflict")
			return ErrIndexConflict
		}
		return nil
	}
	ttl := s.opt.HistoricalTTL
	if err := s.conn.Set(ctx, s.partition, indexKey(n), []byte(hash), &ttl); err != nil {
		s.count("index", "put", "error")
		return err
	}
	s.count("index", "put", "ok")
	return nil
}

// CountLogsFill records one logs fill decision (served, gap, unpinned,
// above_head).
func (s *Store) CountLogsFill(outcome string) {
	if s != nil {
		s.count("logs", "logs_fill", outcome)
	}
}

// CountPromote records one historical promotion pass (ok, conflict).
func (s *Store) CountPromote(outcome string) {
	if s != nil {
		s.count("index", "promote", outcome)
	}
}
