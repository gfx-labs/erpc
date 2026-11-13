package data

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// BlobPubSub is the optional capability of a connector that fans opaque
// messages out to every instance subscribed to a channel: best-effort, at
// most once, no history. The leader fetch publishes heads on it.
type BlobPubSub interface {
	PublishBlob(ctx context.Context, channel string, payload []byte) error
	// SubscribeBlob delivers each message published on channel after the
	// call until ctx ends or stop is called. A slow reader loses messages,
	// never blocks the publisher.
	SubscribeBlob(ctx context.Context, channel string) (msgs <-chan []byte, stop func(), err error)
}

// SupportsBlobPubSub reports whether c can publish and subscribe blobs.
func SupportsBlobPubSub(c Connector) bool {
	if f, ok := c.(*FailsafeConnector); ok {
		c = f.wrapped
	}
	_, ok := c.(BlobPubSub)
	return ok
}

const blobSubscriberBuffer = 64

// ----- memory: one process, every instance on the connector -----

type memoryBlobBus struct {
	mu   sync.Mutex
	subs map[string]map[chan []byte]struct{}
}

func (m *MemoryConnector) PublishBlob(_ context.Context, channel string, payload []byte) error {
	b := &m.blobs
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs[channel] {
		select {
		case ch <- append([]byte(nil), payload...):
		default:
		}
	}
	return nil
}

func (m *MemoryConnector) SubscribeBlob(ctx context.Context, channel string) (<-chan []byte, func(), error) {
	b := &m.blobs
	ch := make(chan []byte, blobSubscriberBuffer)
	b.mu.Lock()
	if b.subs == nil {
		b.subs = map[string]map[chan []byte]struct{}{}
	}
	if b.subs[channel] == nil {
		b.subs[channel] = map[chan []byte]struct{}{}
	}
	b.subs[channel][ch] = struct{}{}
	b.mu.Unlock()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subs[channel], ch)
			b.mu.Unlock()
		})
	}
	go func() {
		<-ctx.Done()
		stop()
	}()
	return ch, stop, nil
}

// ----- redis -----

func (r *RedisConnector) PublishBlob(ctx context.Context, channel string, payload []byte) error {
	if err := r.checkReady(); err != nil {
		return err
	}
	return r.client.Publish(ctx, "blob:"+channel, payload).Err()
}

// SubscribeBlob holds one subscription per call; go-redis resubscribes on
// reconnect, and a closed subscription (a replaced client) is opened again.
func (r *RedisConnector) SubscribeBlob(ctx context.Context, channel string) (<-chan []byte, func(), error) {
	if err := r.checkReady(); err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	out := make(chan []byte, blobSubscriberBuffer)
	go func() {
		for ctx.Err() == nil {
			var ps *redis.PubSub
			if r.checkReady() == nil {
				ps = r.client.Subscribe(ctx, "blob:"+channel)
			}
			if ps != nil {
				done := make(chan struct{})
				go func() {
					select {
					case <-ctx.Done():
						_ = ps.Close()
					case <-done:
					}
				}()
				for msg := range ps.Channel() {
					select {
					case out <- []byte(msg.Payload):
					default:
					}
				}
				close(done)
				_ = ps.Close()
			}
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}()
	return out, cancel, nil
}

// ----- failsafe -----

func (f *FailsafeConnector) PublishBlob(ctx context.Context, channel string, payload []byte) error {
	if b, ok := f.wrapped.(BlobPubSub); ok {
		return b.PublishBlob(ctx, channel, payload)
	}
	return ErrLeaseUnsupported
}

func (f *FailsafeConnector) SubscribeBlob(ctx context.Context, channel string) (<-chan []byte, func(), error) {
	if b, ok := f.wrapped.(BlobPubSub); ok {
		return b.SubscribeBlob(ctx, channel)
	}
	return nil, nil, ErrLeaseUnsupported
}
