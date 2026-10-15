package queue_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rlsvr/inas/pkg/queue"
)

func TestNewZeroCapacityPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for capacity <= 0")
		}
	}()
	_ = queue.New[int](0)
}

func TestPushPopFIFO(t *testing.T) {
	q := queue.New[int](8)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := q.Push(ctx, i); err != nil {
			t.Fatalf("Push(%d) returned %v", i, err)
		}
	}
	if got := q.Len(); got != 5 {
		t.Fatalf("Len = %d, want 5", got)
	}
	if got := q.Cap(); got != 8 {
		t.Fatalf("Cap = %d, want 8", got)
	}
	for i := 0; i < 5; i++ {
		v, err := q.Pop(ctx)
		if err != nil {
			t.Fatalf("Pop returned %v", err)
		}
		if v != i {
			t.Fatalf("Pop = %d, want %d", v, i)
		}
	}
}

func TestTryPushFullReturnsFalse(t *testing.T) {
	q := queue.New[int](2)
	if !q.TryPush(1) {
		t.Fatal("first TryPush should succeed")
	}
	if !q.TryPush(2) {
		t.Fatal("second TryPush should succeed")
	}
	if q.TryPush(3) {
		t.Fatal("third TryPush should fail (queue full)")
	}
}

func TestPushBlocksUntilSpace(t *testing.T) {
	q := queue.New[int](1)
	if !q.TryPush(1) {
		t.Fatal("first TryPush should succeed")
	}

	pushed := make(chan struct{})
	go func() {
		_ = q.Push(context.Background(), 2)
		close(pushed)
	}()

	select {
	case <-pushed:
		t.Fatal("push should block while queue is full")
	case <-time.After(50 * time.Millisecond):
	}

	v, err := q.Pop(context.Background())
	if err != nil || v != 1 {
		t.Fatalf("Pop = %d, %v, want 1, nil", v, err)
	}

	select {
	case <-pushed:
	case <-time.After(time.Second):
		t.Fatal("blocked push did not complete after pop")
	}
}

func TestPushCanceledContext(t *testing.T) {
	q := queue.New[int](1)
	_ = q.TryPush(1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := q.Push(ctx, 2)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Push error = %v, want context.Canceled", err)
	}
}

func TestPopCanceledContext(t *testing.T) {
	q := queue.New[int](1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := q.Pop(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Pop error = %v, want context.Canceled", err)
	}
}

func TestCloseDrainsRemaining(t *testing.T) {
	q := queue.New[int](4)
	_ = q.TryPush(10)
	_ = q.TryPush(20)
	q.Close()

	ctx := context.Background()
	v1, err := q.Pop(ctx)
	if err != nil || v1 != 10 {
		t.Fatalf("v1 = %d, %v, want 10, nil", v1, err)
	}

	v2, err := q.Pop(ctx)
	if err != nil || v2 != 20 {
		t.Fatalf("v2 = %d, %v, want 20, nil", v2, err)
	}

	_, err = q.Pop(ctx)
	if !errors.Is(err, queue.ErrClosed) {
		t.Fatalf("Pop after drain = %v, want ErrClosed", err)
	}

	if err := q.Push(ctx, 30); !errors.Is(err, queue.ErrClosed) {
		t.Fatalf("Push after close = %v, want ErrClosed", err)
	}
}

func TestConcurrentPushPop(t *testing.T) {
	q := queue.New[int](16)
	const count = 1000
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		ctx := context.Background()
		for i := 0; i < count; i++ {
			_ = q.Push(ctx, i)
		}
		q.Close()
	}()

	var received []int
	go func() {
		defer wg.Done()
		ctx := context.Background()
		for {
			v, err := q.Pop(ctx)
			if err != nil {
				return
			}
			received = append(received, v)
		}
	}()

	wg.Wait()
	if len(received) != count {
		t.Fatalf("received %d items, want %d", len(received), count)
	}
}

func TestPopPrioritizesCanceledContext(t *testing.T) {
	q := queue.New[int](10)
	for i := 1; i <= 5; i++ {
		if !q.TryPush(i) {
			t.Fatalf("TryPush(%d) failed", i)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled before calling Pop

	val, err := q.Pop(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Pop with canceled ctx returned err=%v, val=%d; want context.Canceled", err, val)
	}
	if q.Len() != 5 {
		t.Fatalf("queue length changed: got %d, want 5", q.Len())
	}
}

func BenchmarkQueue_TryPushPop(b *testing.B) {
	q := queue.New[int](b.N + 1)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		q.TryPush(i)
	}
	for i := 0; i < b.N; i++ {
		_, _ = q.Pop(ctx)
	}
}
