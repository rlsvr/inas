// Package inas provides generic in-process concurrency primitives for Go
// with zero external dependencies.
//
// Primitives:
//   - inas/pkg/queue: Bounded, generic FIFO queue (Queue[T])
//   - inas/pkg/worker: Generic worker pool fanning out from a Queue (Run[T], Pool)
package inas

import (
	"context"

	"github.com/rlsvr/inas/pkg/queue"
	"github.com/rlsvr/inas/pkg/worker"
)

type (
	// Queue is a bounded generic FIFO queue.
	Queue[T any] = queue.Queue[T]

	// Pool is a generic worker pool.
	Pool = worker.Pool
)

// ErrClosed is returned by queue Push/Pop operations when the queue is closed.
var ErrClosed = queue.ErrClosed

// NewQueue constructs a bounded generic FIFO queue with the given capacity.
func NewQueue[T any](capacity int) *queue.Queue[T] {
	return queue.New[T](capacity)
}

// RunWorkerPool spawns a generic worker pool of N workers consuming from a Queue.
func RunWorkerPool[T any](ctx context.Context, n int, q *queue.Queue[T], dispatch func(context.Context, T)) *worker.Pool {
	return worker.Run(ctx, n, q, dispatch)
}
