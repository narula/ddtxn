package wfmutex

import (
	"log"
	"runtime/debug"
	"sync/atomic"
)

const (
	LOCKED uint64 = 1 << 63
)

type WFMutex struct {
	w uint64
}

// Lock locks rw for writing.  If the record is already locked it
// returns false, if I successfully obtained the lock it returns true.
// Lock never blocks.
func (rw *WFMutex) Lock() bool {
	// First, check if it's locked
	locked_q := atomic.LoadUint64(&rw.w)
	high_bit := locked_q >> 63
	if high_bit&1 != 0 {
		return false
	}
	// Not locked, try to compare and swap to get it.  We
	// compare-and-swap in the last value to preserve the version in
	// case a transaction writes something it previously read.  Other
	// transactions should still fail because it is locked, and when
	// this transaction unlocks it will atomically write a new
	// version.
	var locked_t uint64 = LOCKED | locked_q
	done := atomic.CompareAndSwapUint64(&rw.w, locked_q, locked_t)
	if !done {
		return false
	}
	return true
}

func (rw *WFMutex) Read() uint64 {
	return atomic.LoadUint64(&rw.w)
}

// Unlock unlocks rw for writing.  It is a run-time error if rw is
// not locked for writing on entry to Unlock.
func (rw *WFMutex) Unlock(t uint64) {
	locked_q := atomic.LoadUint64(&rw.w)
	x := locked_q & LOCKED
	if x == 0 {
		debug.PrintStack()
		log.Fatalf("Trying to unlock an unlocked lock\n")
	}
	if t&LOCKED != 0 {
		log.Fatalf("Bad TID %v\n", t)
	}
	done := atomic.CompareAndSwapUint64(&rw.w, locked_q, t)
	if !done {
		log.Fatalf("Compare and swap failed but should have succeeded\n")
	}
}
