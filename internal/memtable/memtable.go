package memtable

import (
	"log/slog"
	"sync"
	"sync/atomic"
)

type MemTable struct {
	sl *SkipList
}

func New() *MemTable {
	return &MemTable{sl: NewSkipList()}
}

func (mem *MemTable) Put(key, value []byte) {
	mem.sl.Put(key, value)
}

func (mem *MemTable) Delete(key []byte) {
	mem.sl.PutTombstone(key)
}

// Get returns (value, tombstone, found).
func (mem *MemTable) Get(key []byte) ([]byte, bool, bool) {
	return mem.sl.Get(key)
}

func (mem *MemTable) NewIterator() *Iterator {
	return mem.sl.NewIterator()
}

// Manager owns the active and immutable memtables.
type Manager struct {
	active     atomic.Pointer[MemTable]
	activeSize atomic.Int64

	// mutex for the cond vars and immutables update
	mu         sync.Mutex
	putStall   sync.Cond // Puts wait here when stalled
	rotWait    sync.Cond // rotateLoop waits here for a free immutable slot
	immutables []*MemTable

	stallSet     atomic.Bool
	maxImmutable int
	memTableSize int64

	rotateCh chan struct{} // signals rotateLoop that active is full
	flushCh  chan struct{} // signals flushLoop that an immutable is ready
}

func NewManager(memTableSize int64, maxImmutable int) *Manager {
	mgr := &Manager{
		immutables:   make([]*MemTable, 0, maxImmutable),
		maxImmutable: maxImmutable,
		memTableSize: memTableSize,
		rotateCh:     make(chan struct{}, 1),
		flushCh:      make(chan struct{}, 1),
	}
	mgr.putStall = *sync.NewCond(&mgr.mu)
	mgr.rotWait = *sync.NewCond(&mgr.mu)
	mgr.active.Store(New())
	return mgr
}

func (mgr *Manager) ActiveSize() int64 {
	return mgr.activeSize.Load()
}

func (mgr *Manager) Put(key, value []byte) {
retry:
	// fast path — no stall
	if !mgr.stallSet.Load() {
		mgr.active.Load().Put(key, value)
		size := mgr.activeSize.Add(int64(len(key)) + int64(len(value)))
		if size >= mgr.memTableSize {
			// signal rotate goroutine and return immediately
			select {
			case mgr.rotateCh <- struct{}{}:
			default:
			}
		}
		return
	}

	// slow path — wait until stall clears
	slog.Warn("Put stall is set, waiting")
	mgr.mu.Lock()
	for mgr.stallSet.Load() {
		mgr.putStall.Wait()
	}
	mgr.mu.Unlock()
	slog.Info("Put stall cleared, retrying...")
	goto retry
}

func (mgr *Manager) Delete(key []byte) {
	mgr.active.Load().Delete(key)
}

// Get checks active first, then immutables newest-to-oldest.
func (mgr *Manager) Get(key []byte) ([]byte, bool, bool) {
	if value, tomb, found := mgr.active.Load().Get(key); found {
		return value, tomb, found
	}

	mgr.mu.Lock()
	imms := mgr.immutables
	mgr.mu.Unlock()

	for i := len(imms) - 1; i >= 0; i-- {
		if value, tomb, found := imms[i].Get(key); found {
			return value, tomb, found
		}
	}
	return nil, false, false
}

// Rotate swaps the active memtable into immutables and installs a fresh one.
// Waits if all immutable slots are taken. Signals the flush goroutine.
// Called from rotateLoop in db — never from the write path.
func (mgr *Manager) Rotate() {
	mgr.mu.Lock()

	// wait for a free slot
	for len(mgr.immutables) >= mgr.maxImmutable {
		slog.Warn("Rotate waiting for free slot, setting stall")
		mgr.stallSet.Store(true)
		mgr.rotWait.Wait()
	}
	fresh := New()
	old := mgr.active.Swap(fresh)
	mgr.activeSize.Store(0)
	mgr.immutables = append(mgr.immutables, old)
	mgr.mu.Unlock()
	slog.Info("Rotated memtable")
	// Lift stall once we can offload memtable and get a new one
	mgr.stallSet.Store(false)
	mgr.putStall.Broadcast()

	select {
	case mgr.flushCh <- struct{}{}:
	default:
	}

}

// FinishFlush removes the oldest immutable after a successful flush.
func (mgr *Manager) FinishFlush() {
	mgr.mu.Lock()
	mgr.immutables = mgr.immutables[1:]
	mgr.mu.Unlock()

	mgr.rotWait.Signal()
}

// RotateCh returns the channel the rotate goroutine should listen on.
func (mgr *Manager) RotateCh() <-chan struct{} {
	return mgr.rotateCh
}

// FlushCh returns the channel the flush goroutine should listen on.
func (mgr *Manager) FlushCh() <-chan struct{} {
	return mgr.flushCh
}

// ImmutableFront returns the oldest immutable memtable to flush, or nil.
func (mgr *Manager) ImmutableFront() *MemTable {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if len(mgr.immutables) == 0 {
		return nil
	}
	return mgr.immutables[0]
}

func (mgr *Manager) NumImmutables() int {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return len(mgr.immutables)
}

func (mgr *Manager) Close() {
	close(mgr.rotateCh)
	close(mgr.flushCh)
}
