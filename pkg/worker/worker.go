// Package worker provides a small generic worker pool that drains a
// queue.Queue[T] by calling a dispatch func on each item until the queue
// is closed or the context is canceled.
package worker

import (
	"context"
	"sync/atomic"

	"github.com/rlsvr/inas/pkg/queue"
)

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
// n must be > 0. Panics if not.
func Run[T any](ctx context.Context, n int, q *queue.Queue[T], dispatch func(context.Context, T)) *Pool {
	if n <= 0 {
		panic("worker: n must be > 0")
	}
	p := &Pool{stopped: make(chan struct{})}
	var remaining atomic.Int32
	remaining.Store(int32(n)) //nolint:gosec // n > 0 validated above
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
