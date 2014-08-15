package spinlock

import (
	"runtime"
	"sync/atomic"
)

const (
	PREEMPT = 5
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
	i := PREEMPT
	for !done {
		if i == 0 {
			runtime.Gosched()
			i = PREEMPT
		}
		done = atomic.CompareAndSwapInt32(&s.state, 0, mutexLocked)
		i--
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
		i := PREEMPT
		for atomic.LoadInt32(&l.readerCount) < 0 {
			if i == 0 {
				runtime.Gosched()
				i = PREEMPT
			}
			i--
		}
	}
}

func (l *RWSpinlock) RUnlock() {
	atomic.AddInt32(&l.readerCount, -1)
}

func (l *RWSpinlock) Lock() {
	l.w.Lock()
	r := atomic.AddInt32(&l.readerCount, -spinlockMaxReaders) + spinlockMaxReaders
	i := PREEMPT
	for r != 0 {
		if i == 0 {
			runtime.Gosched()
			i = PREEMPT
		}
		r = atomic.LoadInt32(&l.readerCount)
		i--
	}
}

func (l *RWSpinlock) Unlock() {
	atomic.AddInt32(&l.readerCount, spinlockMaxReaders)
	l.w.Unlock()
}
