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
	opDeregisterMember
)

type request[K comparable, M comparable] struct {
	op        opType
	member    M
	singleKey K
	multiKeys []K
	hasSingle bool
	done      chan struct{}
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

// Register registers member under a single entry without allocating a slice.
// It returns an idempotent deregister func targeting this specific entry.
// If ReconfigWait is true, Register blocks until the actor applies the registration.
func (r *Registry[K, M]) Register(entry K, member M) func() {
	var done chan struct{}
	if r.opts.ReconfigWait {
		done = make(chan struct{})
	}

	req := request[K, M]{
		op:        opRegister,
		member:    member,
		singleKey: entry,
		hasSingle: true,
		done:      done,
	}

	select {
	case r.reqCh <- req:
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
			r.Deregister(entry, member)
		})
	}
}

// RegisterMulti registers member under multiple entries.
// It returns an idempotent deregister func targeting these specific entries.
func (r *Registry[K, M]) RegisterMulti(entries []K, member M) func() {
	if len(entries) == 0 {
		return func() {}
	}
	if len(entries) == 1 {
		return r.Register(entries[0], member)
	}

	// Defensive copy to prevent caller slice mutation from corrupting actor state or unregister closure.
	entriesCopy := slices.Clone(entries)

	var done chan struct{}
	if r.opts.ReconfigWait {
		done = make(chan struct{})
	}

	req := request[K, M]{
		op:        opRegister,
		member:    member,
		multiKeys: entriesCopy,
		hasSingle: false,
		done:      done,
	}

	select {
	case r.reqCh <- req:
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
			for _, e := range entriesCopy {
				r.Deregister(e, member)
			}
		})
	}
}

// Deregister removes member from the specified entry.
func (r *Registry[K, M]) Deregister(entry K, member M) {
	var done chan struct{}
	if r.opts.ReconfigWait {
		done = make(chan struct{})
	}

	req := request[K, M]{
		op:        opDeregister,
		member:    member,
		singleKey: entry,
		hasSingle: true,
		done:      done,
	}

	select {
	case r.reqCh <- req:
		if done != nil {
			select {
			case <-done:
			case <-r.ctx.Done():
			}
		}
	case <-r.ctx.Done():
	}
}

// DeregisterMember removes member from all registered entries.
func (r *Registry[K, M]) DeregisterMember(member M) {
	var done chan struct{}
	if r.opts.ReconfigWait {
		done = make(chan struct{})
	}

	req := request[K, M]{
		op:     opDeregisterMember,
		member: member,
		done:   done,
	}

	select {
	case r.reqCh <- req:
		if done != nil {
			select {
			case <-done:
			case <-r.ctx.Done():
			}
		}
	case <-r.ctx.Done():
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
	var batch []request[K, M]
	defer func() {
		_ = recover()
		// In case of actor panic or termination, unblock all waiting callers so they don't deadlock
		for _, req := range batch {
			safeCloseDone(req.done)
		}
		for {
			select {
			case req := <-r.reqCh:
				safeCloseDone(req.done)
			default:
				close(r.actorDone)
				return
			}
		}
	}()

	interval := r.opts.ReconfigInterval
	batchCap := r.opts.ReconfigBatchSize
	batch = make([]request[K, M], 0, batchCap)

	timer := time.NewTimer(interval)
	timerActive := true
	defer timer.Stop()

	// Actor-owned state: bidirectional index
	liveEntries := make(map[K]map[M]struct{})
	liveMembers := make(map[M]map[K]struct{})
	evictAt := make(map[K]time.Time)

	for {
		select {
		case <-r.ctx.Done():
			r.drainAndClose(batch, liveEntries, liveMembers)
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
				now := time.Now()
				r.applyBatch(now, batch, liveEntries, liveMembers, evictAt)
				r.checkDebounce(now, liveEntries, evictAt)
				clear(batch)
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
			if len(batch) > 0 {
				r.applyBatch(now, batch, liveEntries, liveMembers, evictAt)
				clear(batch)
				batch = batch[:0]
			}
			r.checkDebounce(now, liveEntries, evictAt)
			// Only keep timer ticking if there are pending items or active debounce timers
			if len(batch) > 0 || len(evictAt) > 0 {
				timer.Reset(interval)
				timerActive = true
			}
		}
	}
}

func (r *Registry[K, M]) applyBatch(now time.Time, batch []request[K, M], liveEntries map[K]map[M]struct{}, liveMembers map[M]map[K]struct{}, evictAt map[K]time.Time) {
	dirty := make(map[K]struct{})

	for _, req := range batch {
		switch req.op {
		case opRegister:
			if req.hasSingle {
				if r.applyRegister(req.singleKey, req.member, liveEntries, liveMembers, evictAt) {
					dirty[req.singleKey] = struct{}{}
				}
			} else {
				for _, entry := range req.multiKeys {
					if r.applyRegister(entry, req.member, liveEntries, liveMembers, evictAt) {
						dirty[entry] = struct{}{}
					}
				}
			}
		case opDeregister:
			if r.applyDeregister(req.singleKey, req.member, liveEntries, liveMembers, evictAt, now) {
				dirty[req.singleKey] = struct{}{}
			}
		case opDeregisterMember:
			if memberEntries, exists := liveMembers[req.member]; exists {
				entriesToRemove := make([]K, 0, len(memberEntries))
				for entry := range memberEntries {
					entriesToRemove = append(entriesToRemove, entry)
				}
				for _, entry := range entriesToRemove {
					if r.applyDeregister(entry, req.member, liveEntries, liveMembers, evictAt, now) {
						dirty[entry] = struct{}{}
					}
				}
			}
		}
	}

	if len(dirty) > 0 {
		r.publishSnapshot(liveEntries, dirty)
	}

	for _, req := range batch {
		safeCloseDone(req.done)
	}
}

func (r *Registry[K, M]) applyRegister(entry K, member M, liveEntries map[K]map[M]struct{}, liveMembers map[M]map[K]struct{}, evictAt map[K]time.Time) bool {
	set, exists := liveEntries[entry]
	if !exists {
		set = make(map[M]struct{})
		liveEntries[entry] = set
		safeCall1(r.hooks.OnActive, entry)
	} else {
		delete(evictAt, entry)
	}
	if _, alreadyMember := set[member]; alreadyMember {
		return false
	}
	set[member] = struct{}{}

	memberEntries, memExists := liveMembers[member]
	if !memExists {
		memberEntries = make(map[K]struct{})
		liveMembers[member] = memberEntries
	}
	memberEntries[entry] = struct{}{}

	safeCall2(r.hooks.OnRegister, entry, member)
	return true
}

func (r *Registry[K, M]) applyDeregister(entry K, member M, liveEntries map[K]map[M]struct{}, liveMembers map[M]map[K]struct{}, evictAt map[K]time.Time, now time.Time) bool {
	set, exists := liveEntries[entry]
	if !exists {
		return false
	}
	if _, isMember := set[member]; !isMember {
		return false
	}
	delete(set, member)

	if memberEntries, memExists := liveMembers[member]; memExists {
		delete(memberEntries, entry)
		if len(memberEntries) == 0 {
			delete(liveMembers, member)
		}
	}

	safeCall2(r.hooks.OnDeregister, entry, member)
	if len(set) > 0 {
		return true
	}
	if r.opts.Debounce > 0 {
		evictAt[entry] = now.Add(r.opts.Debounce)
		return true
	}
	delete(liveEntries, entry)
	safeCall1(r.hooks.OnInactive, entry)
	return true
}

func (r *Registry[K, M]) checkDebounce(now time.Time, liveEntries map[K]map[M]struct{}, evictAt map[K]time.Time) {
	for entry, deadline := range evictAt {
		if now.After(deadline) || now.Equal(deadline) {
			set := liveEntries[entry]
			if len(set) == 0 {
				delete(liveEntries, entry)
				safeCall1(r.hooks.OnInactive, entry)
			}
			delete(evictAt, entry)
		}
	}
}

func (r *Registry[K, M]) publishSnapshot(liveEntries map[K]map[M]struct{}, dirty map[K]struct{}) {
	prev := r.currentSnap.Load()
	snapMembers := make(map[K][]M, len(liveEntries))
	snapMemberSets := make(map[K]map[M]struct{}, len(liveEntries))
	entries := make([]K, 0, len(liveEntries))
	allSeen := make(map[M]struct{}, len(liveEntries)*2)
	allMembers := make([]M, 0, len(liveEntries)*2)

	for entry, set := range liveEntries {
		if len(set) == 0 {
			continue // pending eviction with 0 members, omit from active snapshot
		}
		entries = append(entries, entry)

		_, isDirty := dirty[entry]
		if !isDirty && prev != nil {
			if prevSlice, ok := prev.members[entry]; ok {
				snapMembers[entry] = prevSlice
				snapMemberSets[entry] = prev.memberSets[entry]
				for _, m := range prevSlice {
					if _, seen := allSeen[m]; !seen {
						allSeen[m] = struct{}{}
						allMembers = append(allMembers, m)
					}
				}
				continue
			}
		}

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
		snapMembers[entry] = slices.Clip(sliceCopy)
		snapMemberSets[entry] = setCopy
	}

	r.currentSnap.Store(&Snapshot[K, M]{
		members:    snapMembers,
		memberSets: snapMemberSets,
		allMembers: slices.Clip(allMembers),
		entries:    slices.Clip(entries),
	})
}

func (r *Registry[K, M]) drainAndClose(batch []request[K, M], liveEntries map[K]map[M]struct{}, liveMembers map[M]map[K]struct{}) {
	// Drain any remaining requests from reqCh into batch
drainLoop:
	for {
		select {
		case req := <-r.reqCh:
			batch = append(batch, req)
		default:
			break drainLoop
		}
	}

	// Apply pending requests so any in-flight registrations or deregistrations are processed consistently
	if len(batch) > 0 {
		now := time.Now()
		evictAt := make(map[K]time.Time)
		r.applyBatch(now, batch, liveEntries, liveMembers, evictAt)
		clear(batch)
	}

	// Clean up all remaining live entries: deregister remaining members and deactivate entries
	for entry, set := range liveEntries {
		if len(set) > 0 {
			for member := range set {
				safeCall2(r.hooks.OnDeregister, entry, member)
			}
		}
		safeCall1(r.hooks.OnInactive, entry)
	}

	clear(liveMembers)
	r.currentSnap.Store(newEmptySnapshot[K, M]())
}

func safeCloseDone(ch chan struct{}) {
	if ch == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	close(ch)
}

func safeCall1[A any](fn func(A), a A) {
	if fn == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	fn(a)
}

func safeCall2[A, B any](fn func(A, B), a A, b B) {
	if fn == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	fn(a, b)
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
