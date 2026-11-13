package data

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrLeaseUnsupported is returned by AcquireLease when the shared-state
// connector cannot provide an exclusive, expiring lease. Callers must then
// not assume leadership: two instances could both believe they lead.
var ErrLeaseUnsupported = errors.New("shared state connector does not support leases")

// Lease is an exclusive, expiring right held by one instance: a random-token
// lease (SET NX PX plus compare-and-renew), not a mutex. Losing it is seen at
// the next Renew; a holder that dies stops renewing, so another instance
// takes it at most one TTL later.
//
// Epoch is the lease's fencing token: every successful acquisition of a key
// gets a strictly higher epoch than any before it. Whatever a holder
// publishes carries its epoch, so readers drop writes of a deposed holder
// that has not yet noticed it lost the lease.
type Lease interface {
	Epoch() int64
	// Renew extends the lease by ttl. false with a nil error: the lease was
	// lost (expired, and possibly taken by another instance).
	Renew(ctx context.Context, ttl time.Duration) (bool, error)
	// Release gives the lease up early, so another instance takes over at
	// once. Releasing a lost lease is a no-op.
	Release(ctx context.Context) error
}

// LeaseConnector is the optional capability of a connector that can grant
// leases. AcquireLease tries once: (nil, nil) when another instance holds
// key now.
type LeaseConnector interface {
	AcquireLease(ctx context.Context, key string, ttl time.Duration) (Lease, error)
}

// AcquireLease takes the lease key on connector c for ttl, once.
func AcquireLease(ctx context.Context, c Connector, key string, ttl time.Duration) (Lease, error) {
	if ttl < time.Millisecond {
		return nil, fmt.Errorf("lease ttl must be positive")
	}
	lc, ok := c.(LeaseConnector)
	if !ok {
		return nil, ErrLeaseUnsupported
	}
	return lc.AcquireLease(ctx, key, ttl)
}

// SupportsLeases reports whether c can grant leases.
func SupportsLeases(c Connector) bool {
	if f, ok := c.(*FailsafeConnector); ok {
		_, ok := f.wrapped.(LeaseConnector)
		return ok
	}
	_, ok := c.(LeaseConnector)
	return ok
}

func leaseToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generate lease token: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}

// ----- memory -----

// memoryLeases are one memory connector's leases: instances that share a
// connector (one process, tests) share its leases.
type memoryLeases struct {
	mu     sync.Mutex
	m      map[string]*memoryLeaseState
	epochs map[string]int64
}

type memoryLeaseState struct {
	token   string
	expires time.Time
}

type memoryLease struct {
	owner *memoryLeases
	key   string
	token string
	epoch int64
}

func (m *MemoryConnector) AcquireLease(_ context.Context, key string, ttl time.Duration) (Lease, error) {
	token, err := leaseToken()
	if err != nil {
		return nil, err
	}
	l := &m.leases
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.m == nil {
		l.m, l.epochs = map[string]*memoryLeaseState{}, map[string]int64{}
	}
	if st, ok := l.m[key]; ok && time.Now().Before(st.expires) {
		return nil, nil
	}
	l.epochs[key]++
	l.m[key] = &memoryLeaseState{token: token, expires: time.Now().Add(ttl)}
	return &memoryLease{owner: l, key: key, token: token, epoch: l.epochs[key]}, nil
}

func (l *memoryLease) Epoch() int64 { return l.epoch }

func (l *memoryLease) Renew(_ context.Context, ttl time.Duration) (bool, error) {
	l.owner.mu.Lock()
	defer l.owner.mu.Unlock()
	st, ok := l.owner.m[l.key]
	if !ok || st.token != l.token || time.Now().After(st.expires) {
		return false, nil
	}
	st.expires = time.Now().Add(ttl)
	return true, nil
}

func (l *memoryLease) Release(context.Context) error {
	l.owner.mu.Lock()
	defer l.owner.mu.Unlock()
	if st, ok := l.owner.m[l.key]; ok && st.token == l.token {
		delete(l.owner.m, l.key)
	}
	return nil
}

// ----- redis -----

type redisLease struct {
	conn  *RedisConnector
	key   string
	token string
	epoch int64
}

// AcquireLease is SET NX PX of a random token; on success the key's epoch
// counter (key + "/epoch", never expiring) is incremented in the same
// script, so epochs only grow across holders.
func (r *RedisConnector) AcquireLease(ctx context.Context, key string, ttl time.Duration) (Lease, error) {
	if err := r.checkReady(); err != nil {
		return nil, err
	}
	token, err := leaseToken()
	if err != nil {
		return nil, err
	}
	const script = `if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then return redis.call('INCR', KEYS[2]) else return 0 end`
	epoch, err := r.client.Eval(ctx, script, []string{key, key + "/epoch"}, token, ttl.Milliseconds()).Int64()
	if err != nil {
		return nil, fmt.Errorf("acquire lease %s: %w", key, err)
	}
	if epoch == 0 {
		return nil, nil
	}
	return &redisLease{conn: r, key: key, token: token, epoch: epoch}, nil
}

func (l *redisLease) Epoch() int64 { return l.epoch }

func (l *redisLease) Renew(ctx context.Context, ttl time.Duration) (bool, error) {
	if err := l.conn.checkReady(); err != nil {
		return false, err
	}
	const script = `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end`
	n, err := l.conn.client.Eval(ctx, script, []string{l.key}, l.token, ttl.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("renew lease %s: %w", l.key, err)
	}
	return n == 1, nil
}

func (l *redisLease) Release(ctx context.Context) error {
	if err := l.conn.checkReady(); err != nil {
		return err
	}
	const script = `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end`
	if err := l.conn.client.Eval(ctx, script, []string{l.key}, l.token).Err(); err != nil {
		return fmt.Errorf("release lease %s: %w", l.key, err)
	}
	return nil
}

// ----- failsafe -----

// AcquireLease forwards to the wrapped connector: a lease is no cache read
// or write, so no failsafe policy applies.
func (f *FailsafeConnector) AcquireLease(ctx context.Context, key string, ttl time.Duration) (Lease, error) {
	return AcquireLease(ctx, f.wrapped, key, ttl)
}
