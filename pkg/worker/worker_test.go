package worker_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rlsvr/inas/pkg/queue"
	"github.com/rlsvr/inas/pkg/worker"
)

func TestRunPanicsOnZero(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for n <= 0")
		}
	}()
	q := queue.New[int](4)
	_ = worker.Run(context.Background(), 0, q, func(_ context.Context, _ int) {})
}

func TestRunPanicsOnNilQueue(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for untyped nil queue")
		}
	}()
	_ = worker.Run[int](context.Background(), 1, nil, func(_ context.Context, _ int) {})
}

func TestRunPanicsOnTypedNilQueue(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for typed nil queue pointer")
		}
	}()
	var q *queue.Queue[int]
	_ = worker.Run[int](context.Background(), 1, q, func(_ context.Context, _ int) {})
}

func TestRunPanicsOnNilDispatch(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for nil dispatch")
		}
	}()
	q := queue.New[int](4)
	_ = worker.Run(context.Background(), 1, q, nil)
}

func TestRunDispatchesAllItems(t *testing.T) {
	q := queue.New[int](8)
	var sum atomic.Int64
	p := worker.Run(context.Background(), 3, q, func(_ context.Context, v int) {
		sum.Add(int64(v))
	})

	for i := 1; i <= 10; i++ {
		if err := q.Push(context.Background(), i); err != nil {
			t.Fatalf("push: %v", err)
		}
	}
	q.Close()

	if err := p.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got := sum.Load(); got != 55 {
		t.Fatalf("sum = %d, want 55", got)
	}
}

func TestStoppedClosesAfterQueueClose(t *testing.T) {
	q := queue.New[int](4)
	p := worker.Run(context.Background(), 2, q, func(_ context.Context, _ int) {})
	q.Close()
	select {
	case <-p.Stopped():
	case <-time.After(time.Second):
		t.Fatal("Stopped did not fire after queue close")
	}
}

func TestStoppedClosesAfterCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	q := queue.New[int](4)
	p := worker.Run(ctx, 2, q, func(_ context.Context, _ int) {})
	cancel()

	select {
	case <-p.Stopped():
	case <-time.After(time.Second):
		t.Fatal("Stopped did not fire after ctx cancel")
	}
}

func TestWaitTimeout(t *testing.T) {
	q := queue.New[int](4)
	p := worker.Run(context.Background(), 1, q, func(_ context.Context, _ int) {
		time.Sleep(500 * time.Millisecond)
	})
	_ = q.Push(context.Background(), 1)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := p.Wait(ctx); err == nil {
		t.Fatal("expected timeout error from Wait")
	}
	q.Close()
}

func TestRunRecoversDispatchPanic(t *testing.T) {
	q := queue.New[int](4)
	var processed atomic.Int64
	p := worker.Run(context.Background(), 1, q, func(_ context.Context, v int) {
		if v == 1 {
			panic("simulated dispatch panic")
		}
		processed.Add(1)
	})

	_ = q.Push(context.Background(), 1)
	_ = q.Push(context.Background(), 2)
	q.Close()

	if err := p.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if processed.Load() != 1 {
		t.Fatalf("expected item 2 to be processed despite item 1 panic, got %d", processed.Load())
	}
}
