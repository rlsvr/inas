// Package worker provides a generic worker pool that drains a Source[T]
// by calling a dispatch func on each item until the queue is closed or
// the context is canceled.
package worker

import (
	"context"
	"reflect"
	"sync/atomic"
)

// Source represents any queue or consumer from which items can be popped.
type Source[T any] interface {
	Pop(ctx context.Context) (T, error)
}

// Pool is a running set of N workers fanning items out of a queue into
// a dispatch func. Pool stops when the queue is closed or the context
// passed to Run is canceled.
type Pool struct {
	stopped chan struct{}
}

// Run spawns n goroutines that each pop from q and call dispatch(ctx, item).
// dispatch is expected to handle its own errors; the pool itself only cares
// about lifecycle. Each worker exits when q.Pop returns an error (ctx
// canceled or queue closed).
//
// Panics if n <= 0, q == nil, or dispatch == nil.
func Run[T any](ctx context.Context, n int, q Source[T], dispatch func(context.Context, T)) *Pool {
	if n <= 0 {
		panic("worker: n must be > 0")
	}
	if q == nil || (reflect.ValueOf(q).Kind() == reflect.Pointer && reflect.ValueOf(q).IsNil()) {
		panic("worker: q must be non-nil")
	}
	if dispatch == nil {
		panic("worker: dispatch must be non-nil")
	}
	p := &Pool{stopped: make(chan struct{})}
	var remaining atomic.Int64
	remaining.Store(int64(n))
	for range n {
		go func() {
			defer func() {
				if remaining.Add(-1) == 0 {
					close(p.stopped)
				}
			}()
			for {
				item, err := q.Pop(ctx)
				if err != nil {
					return
				}
				dispatch(ctx, item)
			}
		}()
	}
	return p
}

// Stopped returns a channel that is closed once all workers have exited.
// Workers exit after returning from their current dispatch call, so this
// fires only after in-flight work has completed.
func (p *Pool) Stopped() <-chan struct{} { return p.stopped }

// Wait blocks until all workers have exited, or ctx is canceled. Returns
// nil on clean shutdown, ctx.Err() if the wait was canceled.
func (p *Pool) Wait(ctx context.Context) error {
	select {
	case <-p.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
