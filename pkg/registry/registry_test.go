package registry

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRegistry_BasicLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var activeCalls atomic.Int64
	var inactiveCalls atomic.Int64
	var regCalls atomic.Int64
	var deregCalls atomic.Int64

	reg := New[string, int](ctx, Options{
		ReconfigInterval:  5 * time.Millisecond,
		ReconfigBatchSize: 10,
		ReconfigWait:      true,
	}, Hooks[string, int]{
		OnActive: func(entry string) {
			activeCalls.Add(1)
		},
		OnInactive: func(entry string) {
			inactiveCalls.Add(1)
		},
		OnRegister: func(entry string, member int) {
			regCalls.Add(1)
		},
		OnDeregister: func(entry string, member int) {
			deregCalls.Add(1)
		},
	})
	defer func() { _ = reg.Close() }()

	// Register member 101 under entry-A and entry-B
	unsub1 := reg.Register([]string{"entry-A", "entry-B"}, 101)

	if activeCalls.Load() != 2 {
		t.Fatalf("expected 2 OnActive calls, got %d", activeCalls.Load())
	}
	if regCalls.Load() != 2 {
		t.Fatalf("expected 2 OnRegister calls, got %d", regCalls.Load())
	}

	snap := reg.Snapshot()
	if snap.MemberCount("entry-A") != 1 || snap.MemberCount("entry-B") != 1 {
		t.Fatalf("expected 1 member in both entries, got %d and %d", snap.MemberCount("entry-A"), snap.MemberCount("entry-B"))
	}
	if !snap.HasMember("entry-A", 101) {
		t.Fatal("expected entry-A to have member 101")
	}

	// Register member 102 under entry-A
	unsub2 := reg.Register([]string{"entry-A"}, 102)

	// activeCalls should still be 2 (entry-A was already active)
	if activeCalls.Load() != 2 {
		t.Fatalf("expected activeCalls to remain 2, got %d", activeCalls.Load())
	}
	if reg.MemberCount("entry-A") != 2 {
		t.Fatalf("expected 2 members in entry-A, got %d", reg.MemberCount("entry-A"))
	}

	// Unsubscribe 101
	unsub1()

	if deregCalls.Load() != 2 {
		t.Fatalf("expected 2 OnDeregister calls, got %d", deregCalls.Load())
	}
	// entry-B had 1 member (101), so entry-B should have transitioned to inactive
	if inactiveCalls.Load() != 1 {
		t.Fatalf("expected 1 OnInactive call (for entry-B), got %d", inactiveCalls.Load())
	}
	// entry-A still has member 102
	if reg.MemberCount("entry-A") != 1 {
		t.Fatalf("expected 1 member in entry-A, got %d", reg.MemberCount("entry-A"))
	}

	// Unsubscribe 102
	unsub2()

	if inactiveCalls.Load() != 2 {
		t.Fatalf("expected 2 OnInactive calls, got %d", inactiveCalls.Load())
	}
	if reg.MemberCount("entry-A") != 0 {
		t.Fatalf("expected 0 members in entry-A, got %d", reg.MemberCount("entry-A"))
	}
}

func TestRegistry_DebounceAndRevival(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var activeCalls atomic.Int64
	var inactiveCalls atomic.Int64

	reg := New[string, string](ctx, Options{
		ReconfigInterval:  5 * time.Millisecond,
		ReconfigBatchSize: 10,
		ReconfigWait:      true,
		Debounce:          40 * time.Millisecond, // 40ms debounce window
	}, Hooks[string, string]{
		OnActive: func(entry string) {
			activeCalls.Add(1)
		},
		OnInactive: func(entry string) {
			inactiveCalls.Add(1)
		},
	})
	defer func() { _ = reg.Close() }()

	// 1. First member joins
	unsub1 := reg.Register([]string{"room-1"}, "alice")
	if activeCalls.Load() != 1 {
		t.Fatalf("expected 1 OnActive call, got %d", activeCalls.Load())
	}

	// 2. Member leaves
	unsub1()

	// Snapshot should show 0 members immediately
	if reg.MemberCount("room-1") != 0 {
		t.Fatalf("expected 0 members in snapshot, got %d", reg.MemberCount("room-1"))
	}

	// But OnInactive should NOT have been called yet because of 40ms debounce
	if inactiveCalls.Load() != 0 {
		t.Fatalf("expected 0 OnInactive calls during debounce, got %d", inactiveCalls.Load())
	}

	// 3. New member joins after 10ms (well within 40ms debounce)
	time.Sleep(10 * time.Millisecond)
	unsub2 := reg.Register([]string{"room-1"}, "bob")

	// OnActive should NOT be called again (topic was revived from debounce!)
	if activeCalls.Load() != 1 {
		t.Fatalf("expected activeCalls to remain 1 after revival, got %d", activeCalls.Load())
	}

	// Wait past the original 40ms deadline
	time.Sleep(50 * time.Millisecond)

	// Inactive should still NOT have been called
	if inactiveCalls.Load() != 0 {
		t.Fatalf("expected 0 OnInactive calls because room was revived, got %d", inactiveCalls.Load())
	}

	// 4. Now bob leaves
	unsub2()

	// Wait for debounce to expire (40ms + margin)
	time.Sleep(60 * time.Millisecond)

	// Now OnInactive must be called!
	if inactiveCalls.Load() != 1 {
		t.Fatalf("expected 1 OnInactive call after debounce expired, got %d", inactiveCalls.Load())
	}
}

func TestRegistry_AllMembersDeduplication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := New[string, string](ctx, Options{
		ReconfigInterval: 5 * time.Millisecond,
		ReconfigWait:     true,
	}, Hooks[string, string]{})
	defer func() { _ = reg.Close() }()

	unsub1 := reg.Register([]string{"topic-1", "topic-2"}, "client-A")
	unsub2 := reg.Register([]string{"topic-2", "topic-3"}, "client-B")
	defer unsub1()
	defer unsub2()

	snap := reg.Snapshot()
	all := snap.AllMembers()
	if len(all) != 2 {
		t.Fatalf("expected 2 unique members, got %d (%v)", len(all), all)
	}
}

func TestRegistry_ConcurrentRace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := New[string, int](ctx, Options{
		ReconfigInterval:  2 * time.Millisecond,
		ReconfigBatchSize: 64,
		ReconfigWait:      false,
		Debounce:          5 * time.Millisecond,
	}, Hooks[string, int]{})
	defer func() { _ = reg.Close() }()

	var wg sync.WaitGroup
	workers := 50
	iterations := 200

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				topic := "topic-concurrent"
				unsub := reg.Register([]string{topic}, workerID)
				_ = reg.Snapshot().MemberCount(topic)
				unsub()
			}
		}(i)
	}

	wg.Wait()
}

func TestRegistry_CloseDrainsCleanly(t *testing.T) {
	ctx := context.Background()

	var inactiveCalls atomic.Int64

	reg := New[string, string](ctx, Options{
		ReconfigInterval: 5 * time.Millisecond,
		ReconfigWait:     true,
		Debounce:         10 * time.Second, // Long debounce
	}, Hooks[string, string]{
		OnInactive: func(entry string) {
			inactiveCalls.Add(1)
		},
	})

	_ = reg.Register([]string{"channel-1", "channel-2"}, "user-1")

	// Close immediately; should invoke OnInactive for both active entries
	if err := reg.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}

	if inactiveCalls.Load() != 2 {
		t.Fatalf("expected 2 OnInactive calls on Close, got %d", inactiveCalls.Load())
	}
}
