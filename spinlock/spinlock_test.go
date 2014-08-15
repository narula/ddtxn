package spinlock

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSpinlock(t *testing.T) {
	s := new(Spinlock)
	s.Lock()
	s.Unlock()

	y := 0
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			s.Lock()
			time.Sleep(1 * time.Millisecond)
			y = y + 1
			s.Unlock()
			wg.Done()
		}()
		wg.Add(1)
		go func() {
			s.Lock()
			time.Sleep(1 * time.Millisecond)
			y = y - 1
			s.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	if y != 0 {
		t.Fatalf("Bad lock\n")
	}
	fmt.Printf("Passed TestSpinlock\n")
}

func TestRWSpinlock(t *testing.T) {
	s := new(RWSpinlock)
	s.Lock()
	s.Unlock()

	s.RLock()
	s.RUnlock()

	y := 0
	x := 0
	n := 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			s.Lock()
			y = y + 1
			x = x - 1
			s.Unlock()
			wg.Done()
		}()
		wg.Add(1)
		go func() {
			s.RLock()
			if x+y != 0 {
				t.Fatalf("Bad read %v %v\n", x, y)
			}
			s.RUnlock()
			wg.Done()
		}()
	}
	wg.Wait()
	if y != n || x != -1*n {
		t.Fatalf("Bad lock\n")
	}
	fmt.Printf("Passed TestRWSpinlock\n")
}
