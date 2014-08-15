package spinlock

import (
	"sync/atomic"
)

type Spinlock struct {
	state int32
}

const (
	mutexLocked = 1 << iota // mutex is locked
)

// Lock locks s.
// If the lock is already in use, the calling goroutine
// spins until the mutex is available.
func (s *Spinlock) Lock() {
	done := false
	for !done {
		done = atomic.CompareAndSwapInt32(&s.state, 0, mutexLocked)
	}
}

// Unlock unlocks s.
//
// A locked Spinlock is not associated with a particular goroutine.
// It is allowed for one goroutine to lock a Spinlock and then
// arrange for another goroutine to unlock it.
func (s *Spinlock) Unlock() {
	new := atomic.AddInt32(&s.state, -mutexLocked)
	if (new+mutexLocked)&mutexLocked == 0 {
		panic("sync: unlock of unlocked mutex")
	}
}

type RWSpinlock struct {
	w           Spinlock
	readerCount int32
}

const spinlockMaxReaders = 1 << 30

func (l *RWSpinlock) RLock() {
	if atomic.AddInt32(&l.readerCount, 1) < 0 {
		for atomic.LoadInt32(&l.readerCount) < 0 {
			// spin
		}
	}
}

func (l *RWSpinlock) RUnlock() {
	atomic.AddInt32(&l.readerCount, -1)
}

func (l *RWSpinlock) Lock() {
	l.w.Lock()
	r := atomic.AddInt32(&l.readerCount, -spinlockMaxReaders) + spinlockMaxReaders
	for r != 0 {
		r = atomic.LoadInt32(&l.readerCount)
	}
}

func (l *RWSpinlock) Unlock() {
	atomic.AddInt32(&l.readerCount, spinlockMaxReaders)
	l.w.Unlock()
}
