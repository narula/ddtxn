package spinlock

import (
	"testing"
)

func TestSpinlock(t *testing.T) {
	s := new(RWSpinlock)
	s.Lock()
	s.Unlock()
}
