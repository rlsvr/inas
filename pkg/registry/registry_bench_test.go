package registry

import (
	"context"
	"testing"
	"time"
)

func BenchmarkRegistry_SnapshotRead(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := New[string, int](ctx, Options{
		ReconfigInterval: 5 * time.Millisecond,
		ReconfigWait:     true,
	}, Hooks[string, int]{})
	defer func() { _ = reg.Close() }()

	for i := 0; i < 100; i++ {
		_ = reg.Register([]string{"feed-topic"}, i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			snap := reg.Snapshot()
			_ = snap.MemberCount("feed-topic")
			_ = snap.Members("feed-topic")
		}
	})
}

func BenchmarkRegistry_RegisterDeregister(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := New[string, int](ctx, Options{
		ReconfigInterval:  10 * time.Millisecond,
		ReconfigBatchSize: 256,
		ReconfigWait:      false,
	}, Hooks[string, int]{})
	defer func() { _ = reg.Close() }()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		unsub := reg.Register([]string{"topic-bench"}, i)
		unsub()
	}
}
