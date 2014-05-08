package wfmutex

import (
	"testing"
	"time"
)

func TestWFMutex(t *testing.T) {
	rw := new(WFMutex)
	var tid uint64 = 42
	if !rw.Lock() {
		t.Errorf("Could not lock\n")
	}
	if rw.Read()&LOCKED == 0 {
		t.Errorf("Should be locked\n")
	}
	rw.Unlock(tid)
	if rw.Read()&LOCKED != 0 {
		t.Errorf("Should be unlocked\n")
	}

	ngo := 10
	locked := make([]int, ngo)
	unlocked := make([]int, ngo)

	for i := 0; i < ngo; i++ {
		done := time.NewTimer(time.Duration(100 * time.Millisecond)).C
		go func(id int, done <-chan time.Time) {
			tid := uint64(id)
			for {
				select {
				case <-done:
					return
				default:
					if rw.Lock() {
						locked[id]++
						x := rw.Read()
						if x != LOCKED {
							t.Errorf("I locked it! %x %x %x\n", id, x, rw.w)
						}
						rw.Unlock(tid)
						unlocked[id]++
					}
				}
			}
		}(i, done)
	}
	total_locked := 0
	total_unlocked := 0
	for i := 0; i < ngo; i++ {
		total_locked += locked[i]
		total_unlocked += unlocked[i]
	}
	if total_locked != total_unlocked {
		t.Errorf("Mismatched %v %v %x\n", total_locked, total_unlocked, rw.w)
	}
}
