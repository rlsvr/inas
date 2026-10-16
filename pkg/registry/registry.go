// Package registry provides a generic, zero-dependency in-process registry
// for managing dynamic entries and their registered members with actor-based
// batched updates, lock-free copy-on-write snapshots, and debounced lifecycle eviction.
package registry

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Default settings for Registry options.
const (
	DefaultReconfigInterval  = 10 * time.Millisecond
	DefaultReconfigBatchSize = 128
	DefaultQueueCapacity     = 4096
)

// Options configures the behavior of a Registry.
type Options struct {
	// ReconfigInterval is the maximum time to wait before applying a batched reconfig.
	ReconfigInterval time.Duration
	// ReconfigBatchSize is the number of reconfig operations that trigger an immediate flush.
	ReconfigBatchSize int
	// ReconfigWait configures whether Register blocks until the reconfig actor has applied the change.
	ReconfigWait bool
	// Debounce is the duration an entry is kept warm after its last member leaves before calling OnInactive.
	// If 0, OnInactive is called immediately when the last member departs.
	Debounce time.Duration
	// QueueCapacity is the buffer size of the inbound request queue.
	QueueCapacity int
}

// Hooks provides optional lifecycle callbacks. All callbacks are invoked sequentially
// inside the actor goroutine, so they do not race with registry mutations.
//
// WARNING: Callbacks MUST NOT synchronously call Register (with ReconfigWait) or Close
// on the same Registry instance, as doing so will deadlock the actor goroutine.
type Hooks[K comparable, M comparable] struct {
	// OnActive is called when an entry transitions from 0 to 1 active members.
	OnActive func(entry K)
	// OnInactive is called when an entry transitions to 0 active members and its Debounce TTL expires.
	OnInactive func(entry K)
	// OnRegister is called when a member is added to an entry.
	OnRegister func(entry K, member M)
	// OnDeregister is called when a member is removed from an entry.
	OnDeregister func(entry K, member M)
}

// Snapshot is an immutable, copy-on-write view of the registry.
// All read operations are 100% lock-free.
type Snapshot[K comparable, M comparable] struct {
	members    map[K][]M
	memberSets map[K]map[M]struct{}
	allMembers []M
	entries    []K
}

func newEmptySnapshot[K comparable, M comparable]() *Snapshot[K, M] {
	return &Snapshot[K, M]{
		members:    make(map[K][]M),
		memberSets: make(map[K]map[M]struct{}),
		allMembers: nil,
		entries:    nil,
	}
}

// Members returns a pre-sliced slice of members registered under entry.
// Returns nil if the entry does not exist or has no active members.
// The returned slice is shared across readers and MUST NOT be mutated.
func (s *Snapshot[K, M]) Members(entry K) []M {
	if s == nil {
		return nil
	}
	return s.members[entry]
}

// MemberCount returns the number of active members in the specified entry.
func (s *Snapshot[K, M]) MemberCount(entry K) int {
	if s == nil {
		return 0
	}
	return len(s.members[entry])
}

// HasMember reports whether member is currently registered under entry.
func (s *Snapshot[K, M]) HasMember(entry K, member M) bool {
	if s == nil {
		return false
	}
	set, ok := s.memberSets[entry]
	if !ok {
		return false
	}
	_, found := set[member]
	return found
}

// Entries returns all active entry keys in this snapshot.
// The returned slice is shared across readers and MUST NOT be mutated.
func (s *Snapshot[K, M]) Entries() []K {
	if s == nil {
		return nil
	}
	return s.entries
}

// AllMembers returns a deduplicated slice of all active members across all entries.
// The returned slice is shared across readers and MUST NOT be mutated.
func (s *Snapshot[K, M]) AllMembers() []M {
	if s == nil {
		return nil
	}
	return s.allMembers
}

type opType uint8

const (
	opRegister opType = iota
	opDeregister
)

type request[K comparable, M comparable] struct {
	op      opType
	entries []K
	member  M
	done    chan struct{}
}

// Registry manages dynamic associations between entries (K) and members (M)
// using an actor-based batching engine, lock-free copy-on-write snapshots,
// and debounced lifecycle hooks.
type Registry[K comparable, M comparable] struct {
	opts        Options
	hooks       Hooks[K, M]
	ctx         context.Context
	cancel      context.CancelFunc
	currentSnap atomic.Pointer[Snapshot[K, M]]
	reqCh       chan request[K, M]
	actorDone   chan struct{}
	closeOnce   sync.Once
}

// New constructs and starts a new Registry.
func New[K comparable, M comparable](ctx context.Context, opts Options, hooks Hooks[K, M]) *Registry[K, M] {
	if opts.ReconfigInterval <= 0 {
		opts.ReconfigInterval = DefaultReconfigInterval
	}
	if opts.ReconfigBatchSize <= 0 {
		opts.ReconfigBatchSize = DefaultReconfigBatchSize
	}
	if opts.QueueCapacity <= 0 {
		opts.QueueCapacity = DefaultQueueCapacity
	}

	actCtx, cancel := context.WithCancel(ctx) //nolint:gosec // cancel is preserved in r.cancel and called in r.Close
	r := &Registry[K, M]{
		opts:      opts,
		hooks:     hooks,
		ctx:       actCtx,
		cancel:    cancel,
		reqCh:     make(chan request[K, M], opts.QueueCapacity),
		actorDone: make(chan struct{}),
	}
	r.currentSnap.Store(newEmptySnapshot[K, M]())

	go r.runActor()
	return r
}

// Register registers member under the given entries. It returns an idempotent deregister func.
// If ReconfigWait is true, Register blocks until the actor applies the registration.
func (r *Registry[K, M]) Register(entries []K, member M) func() {
	if len(entries) == 0 {
		return func() {}
	}

	// Defensive copy to prevent caller slice mutation from corrupting actor state or unregister closure.
	entriesCopy := slices.Clone(entries)

	var done chan struct{}
	if r.opts.ReconfigWait {
		done = make(chan struct{})
	}

	select {
	case r.reqCh <- request[K, M]{op: opRegister, entries: entriesCopy, member: member, done: done}:
		if done != nil {
			select {
			case <-done:
			case <-r.ctx.Done():
				return func() {}
			}
		}
	case <-r.ctx.Done():
		return func() {}
	}

	var deregOnce sync.Once
	return func() {
		deregOnce.Do(func() {
			var unsubDone chan struct{}
			if r.opts.ReconfigWait {
				unsubDone = make(chan struct{})
			}

			select {
			case r.reqCh <- request[K, M]{op: opDeregister, entries: entriesCopy, member: member, done: unsubDone}:
				if unsubDone != nil {
					select {
					case <-unsubDone:
					case <-r.ctx.Done():
					}
				}
			case <-r.ctx.Done():
			}
		})
	}
}

// Snapshot returns the current immutable snapshot. Lock-free (atomic load).
func (r *Registry[K, M]) Snapshot() *Snapshot[K, M] {
	return r.currentSnap.Load()
}

// MemberCount returns the number of active members in the given entry.
func (r *Registry[K, M]) MemberCount(entry K) int {
	return r.Snapshot().MemberCount(entry)
}

// ActiveEntries returns all active entries in the current snapshot.
func (r *Registry[K, M]) ActiveEntries() []K {
	return r.Snapshot().Entries()
}

func (r *Registry[K, M]) runActor() {
	defer close(r.actorDone)

	interval := r.opts.ReconfigInterval
	batchCap := r.opts.ReconfigBatchSize
	batch := make([]request[K, M], 0, batchCap)

	timer := time.NewTimer(interval)
	timerActive := true
	defer timer.Stop()

	// Actor-owned state
	liveEntries := make(map[K]map[M]struct{})
	evictAt := make(map[K]time.Time)

	for {
		select {
		case <-r.ctx.Done():
			r.drainAndClose(batch, liveEntries)
			return

		case req := <-r.reqCh:
			batch = append(batch, req)
			needImmediateFlush := req.done != nil

			// Opportunistically drain queued requests up to batchCap
		drainQueue:
			for len(batch) < batchCap {
				select {
				case nextReq := <-r.reqCh:
					batch = append(batch, nextReq)
					if nextReq.done != nil {
						needImmediateFlush = true
					}
				default:
					break drainQueue
				}
			}

			if len(batch) >= batchCap || needImmediateFlush {
				r.applyBatch(batch, liveEntries, evictAt)
				batch = batch[:0]
				if timerActive {
					stopTimer(timer)
					timerActive = false
				}
				if len(evictAt) > 0 {
					timer.Reset(interval)
					timerActive = true
				}
			} else if !timerActive {
				timer.Reset(interval)
				timerActive = true
			}

		case now := <-timer.C:
			timerActive = false
			r.checkDebounce(now, liveEntries, evictAt)
			if len(batch) > 0 {
				r.applyBatch(batch, liveEntries, evictAt)
				batch = batch[:0]
			}
			// Only keep timer ticking if there are pending items or active debounce timers
			if len(batch) > 0 || len(evictAt) > 0 {
				timer.Reset(interval)
				timerActive = true
			}
		}
	}
}

func (r *Registry[K, M]) applyBatch(batch []request[K, M], live map[K]map[M]struct{}, evictAt map[K]time.Time) {
	now := time.Now()

	for _, req := range batch {
		switch req.op {
		case opRegister:
			for _, entry := range req.entries {
				r.applyRegister(entry, req.member, live, evictAt)
			}
		case opDeregister:
			for _, entry := range req.entries {
				r.applyDeregister(entry, req.member, live, evictAt, now)
			}
		}
	}

	r.publishSnapshot(live)

	for _, req := range batch {
		if req.done != nil {
			close(req.done)
		}
	}
}

func (r *Registry[K, M]) applyRegister(entry K, member M, live map[K]map[M]struct{}, evictAt map[K]time.Time) {
	set, exists := live[entry]
	if !exists {
		set = make(map[M]struct{})
		live[entry] = set
		if r.hooks.OnActive != nil {
			r.hooks.OnActive(entry)
		}
	} else {
		delete(evictAt, entry)
	}
	if _, alreadyMember := set[member]; alreadyMember {
		return
	}
	set[member] = struct{}{}
	if r.hooks.OnRegister != nil {
		r.hooks.OnRegister(entry, member)
	}
}

func (r *Registry[K, M]) applyDeregister(entry K, member M, live map[K]map[M]struct{}, evictAt map[K]time.Time, now time.Time) {
	set, exists := live[entry]
	if !exists {
		return
	}
	if _, isMember := set[member]; !isMember {
		return
	}
	delete(set, member)
	if r.hooks.OnDeregister != nil {
		r.hooks.OnDeregister(entry, member)
	}
	if len(set) > 0 {
		return
	}
	if r.opts.Debounce > 0 {
		evictAt[entry] = now.Add(r.opts.Debounce)
		return
	}
	delete(live, entry)
	if r.hooks.OnInactive != nil {
		r.hooks.OnInactive(entry)
	}
}

func (r *Registry[K, M]) checkDebounce(now time.Time, live map[K]map[M]struct{}, evictAt map[K]time.Time) {
	changed := false
	for entry, deadline := range evictAt {
		if now.After(deadline) || now.Equal(deadline) {
			set := live[entry]
			if len(set) == 0 {
				delete(live, entry)
				if r.hooks.OnInactive != nil {
					r.hooks.OnInactive(entry)
				}
				changed = true
			}
			delete(evictAt, entry)
		}
	}
	if changed {
		r.publishSnapshot(live)
	}
}

func (r *Registry[K, M]) publishSnapshot(live map[K]map[M]struct{}) {
	snapMembers := make(map[K][]M, len(live))
	snapMemberSets := make(map[K]map[M]struct{}, len(live))
	entries := make([]K, 0, len(live))
	allSeen := make(map[M]struct{}, len(live)*2)
	allMembers := make([]M, 0, len(live)*2)

	for entry, set := range live {
		if len(set) == 0 {
			continue // pending eviction with 0 members, omit from active snapshot
		}
		entries = append(entries, entry)
		setCopy := make(map[M]struct{}, len(set))
		sliceCopy := make([]M, 0, len(set))

		for m := range set {
			setCopy[m] = struct{}{}
			sliceCopy = append(sliceCopy, m)
			if _, seen := allSeen[m]; !seen {
				allSeen[m] = struct{}{}
				allMembers = append(allMembers, m)
			}
		}
		snapMembers[entry] = sliceCopy
		snapMemberSets[entry] = setCopy
	}

	r.currentSnap.Store(&Snapshot[K, M]{
		members:    snapMembers,
		memberSets: snapMemberSets,
		allMembers: allMembers,
		entries:    entries,
	})
}

func (r *Registry[K, M]) drainAndClose(batch []request[K, M], live map[K]map[M]struct{}) {
	// Drain any in-flight batch
	for _, req := range batch {
		if req.done != nil {
			close(req.done)
		}
	}
	// Drain channel
drainLoop:
	for {
		select {
		case req := <-r.reqCh:
			if req.done != nil {
				close(req.done)
			}
		default:
			break drainLoop
		}
	}

	// Invoke OnDeregister for all remaining members and OnInactive for all remaining entries
	for entry, set := range live {
		if r.hooks.OnDeregister != nil {
			for member := range set {
				r.hooks.OnDeregister(entry, member)
			}
		}
		if r.hooks.OnInactive != nil {
			r.hooks.OnInactive(entry)
		}
	}

	r.currentSnap.Store(newEmptySnapshot[K, M]())
}

// Close gracefully stops the registry actor and flushes pending evictions.
func (r *Registry[K, M]) Close() error {
	r.closeOnce.Do(func() {
		r.cancel()
		<-r.actorDone
	})
	return nil
}

func stopTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}
