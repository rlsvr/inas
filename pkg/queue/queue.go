// Package queue provides a bounded, generic FIFO queue intended as an
// in-process primitive: producers Push, consumers Pop, and Close stops
// further work while letting buffered items drain.
package queue

import (
	"context"
	"errors"
	"sync"
)

// ErrClosed is returned by Push and Pop when operating on a closed queue.
var ErrClosed = errors.New("queue: closed")

// Queue is a thread-safe, bounded, generic FIFO queue.
type Queue[T any] struct {
	ch        chan T
	done      chan struct{}
	closeOnce sync.Once
}

// New constructs a Queue with the given capacity. Panics if capacity <= 0.
func New[T any](capacity int) *Queue[T] {
	if capacity <= 0 {
		panic("queue: capacity must be > 0")
	}
	return &Queue[T]{
		ch:   make(chan T, capacity),
		done: make(chan struct{}),
	}
}

// Push blocks until v is enqueued, ctx is canceled, or the queue is closed.
// If Close races with Push, either delivery or ErrClosed is returned.
func (q *Queue[T]) Push(ctx context.Context, v T) error {
	select {
	case <-q.done:
		return ErrClosed
	default:
	}
	select {
	case q.ch <- v:
		return nil
	case <-q.done:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TryPush enqueues v without blocking. It returns false if the queue is
// full or closed.
func (q *Queue[T]) TryPush(v T) bool {
	select {
	case <-q.done:
		return false
	default:
	}
	select {
	case q.ch <- v:
		return true
	default:
		return false
	}
}

// Pop blocks until an item is available, ctx is canceled, or the queue is
// closed and drained. After Close, Pop continues to return buffered items
// and only returns ErrClosed once the buffer is empty.
func (q *Queue[T]) Pop(ctx context.Context) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	select {
	case v := <-q.ch:
		return v, nil
	case <-q.done:
		select {
		case v := <-q.ch:
			return v, nil
		default:
			return zero, ErrClosed
		}
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

// Recv returns the underlying receive channel for use in caller-side
// selects. The channel is never closed; pair it with Done to observe
// shutdown.
func (q *Queue[T]) Recv() <-chan T { return q.ch }

// Done returns a channel that is closed when Close has been called.
func (q *Queue[T]) Done() <-chan struct{} { return q.done }

// Close is idempotent and safe to call concurrently with Push and Pop.
func (q *Queue[T]) Close() {
	q.closeOnce.Do(func() { close(q.done) })
}

// Len returns the number of currently buffered items.
func (q *Queue[T]) Len() int { return len(q.ch) }

// Cap returns the maximum capacity of the queue.
func (q *Queue[T]) Cap() int { return cap(q.ch) }
