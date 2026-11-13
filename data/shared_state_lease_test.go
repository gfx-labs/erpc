package data

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/erpc/erpc/common"
	"github.com/erpc/erpc/util"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func init() {
	util.ConfigureTestLogger()
}

func newMemoryForLease(t *testing.T) *MemoryConnector {
	c, err := NewMemoryConnector(context.Background(), &log.Logger, "m", &common.MemoryConnectorConfig{MaxItems: 100, MaxTotalSize: "1MB"})
	require.NoError(t, err)
	return c
}

func newRedisForLease(t *testing.T) (*RedisConnector, *miniredis.Miniredis) {
	m, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(m.Close)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := &common.RedisConnectorConfig{Addr: m.Addr(), ConnPoolSize: 5, InitTimeout: common.Duration(2 * time.Second), GetTimeout: common.Duration(time.Second), SetTimeout: common.Duration(time.Second)}
	require.NoError(t, cfg.SetDefaults())
	c, err := NewRedisConnector(ctx, &log.Logger, "r", cfg)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return c.checkReady() == nil }, 5*time.Second, 10*time.Millisecond)
	return c, m
}

// One holder at a time; a lost or released lease frees the key; every new
// holder's epoch is above every earlier one.
func testLeaseSemantics(t *testing.T, c Connector, elapse func(time.Duration)) {
	ctx := context.Background()
	a, err := AcquireLease(ctx, c, "k", 200*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, a)
	b, err := AcquireLease(ctx, c, "k", 200*time.Millisecond)
	require.NoError(t, err)
	require.Nil(t, b, "held: no second holder")

	ok, err := a.Renew(ctx, 200*time.Millisecond)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, a.Release(ctx))
	b, err = AcquireLease(ctx, c, "k", 100*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, b, "released: free")
	require.Greater(t, b.Epoch(), a.Epoch(), "epochs grow")

	ok, err = a.Renew(ctx, time.Second)
	require.NoError(t, err)
	require.False(t, ok, "a released lease cannot be renewed")

	elapse(150 * time.Millisecond)
	require.Eventually(t, func() bool {
		d, err := AcquireLease(ctx, c, "k", time.Second)
		if err != nil || d == nil {
			return false
		}
		require.Greater(t, d.Epoch(), b.Epoch(), "a lease taken after expiry fences the expired one")
		ok, _ := b.Renew(ctx, time.Second)
		require.False(t, ok, "the expired holder lost it")
		return true
	}, 3*time.Second, 20*time.Millisecond, "an expired lease is free")
}

func TestLease_Memory(t *testing.T) { testLeaseSemantics(t, newMemoryForLease(t), time.Sleep) }

func TestLease_Redis(t *testing.T) {
	c, m := newRedisForLease(t)
	testLeaseSemantics(t, c, m.FastForward)
}

func TestLease_UnsupportedConnector(t *testing.T) {
	_, err := AcquireLease(context.Background(), &MockConnector{}, "k", time.Second)
	require.ErrorIs(t, err, ErrLeaseUnsupported)
}

func testBlobFanOut(t *testing.T, c BlobPubSub) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s1, stop1, err := c.SubscribeBlob(ctx, "ch")
	require.NoError(t, err)
	defer stop1()
	s2, stop2, err := c.SubscribeBlob(ctx, "ch")
	require.NoError(t, err)
	defer stop2()
	other, stop3, err := c.SubscribeBlob(ctx, "other")
	require.NoError(t, err)
	defer stop3()
	// Redis subscriptions start asynchronously: publish until both hear.
	got1, got2 := false, false
	require.Eventually(t, func() bool {
		require.NoError(t, c.PublishBlob(ctx, "ch", []byte("hi")))
		for {
			select {
			case m := <-s1:
				require.Equal(t, "hi", string(m))
				got1 = true
				continue
			case m := <-s2:
				require.Equal(t, "hi", string(m))
				got2 = true
				continue
			case <-time.After(20 * time.Millisecond):
			}
			return got1 && got2
		}
	}, 5*time.Second, 10*time.Millisecond, "every subscriber hears a message")
	select {
	case m := <-other:
		t.Fatalf("another channel heard %q", m)
	default:
	}
}

func TestBlobPubSub_Memory(t *testing.T) { testBlobFanOut(t, newMemoryForLease(t)) }

func TestBlobPubSub_Redis(t *testing.T) {
	c, _ := newRedisForLease(t)
	testBlobFanOut(t, c)
}
