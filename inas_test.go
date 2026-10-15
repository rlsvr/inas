package inas_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/rlsvr/inas"
)

func TestFacade_QueueAndWorker(t *testing.T) {
	q := inas.NewQueue[int](16)
	defer q.Close()

	var sum atomic.Int64
	pool := inas.RunWorkerPool(context.Background(), 2, q, func(_ context.Context, v int) {
		sum.Add(int64(v))
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		_ = q.Push(ctx, i)
	}
	q.Close()

	if err := pool.Wait(ctx); err != nil {
		t.Fatalf("wait: %v", err)
	}

	if sum.Load() != 15 {
		t.Fatalf("sum = %d, want 15", sum.Load())
	}
}
