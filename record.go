package ddtxn

import (
	"ddtxn/wfmutex"
	"flag"
	"log"
	"sync"
	"sync/atomic"
)

var Conflicts = flag.Bool("conflicts", false, "Measure conflicts\n")

type KeyType int

const (
	SUM = iota
	MAX
	WRITE
	LIST
	ORDERED_PAIR
)

type BRecord struct {
	key       Key
	key_type  KeyType
	dd        bool
	mu        sync.RWMutex
	value     Value
	int_value int32
	entries   []Entry
	last      wfmutex.WFMutex
	conflict  int32 // how many times was the lock already held when someone wanted it
}

func MakeBR(k Key, val Value, kt KeyType) *BRecord {
	//dlog.Printf("Making %v %v %v\n", k, val, kt)
	b := &BRecord{
		key:      k,
		last:     wfmutex.WFMutex{},
		key_type: kt,
	}
	switch kt {
	case SUM:
		b.int_value = val.(int32)
	case MAX:
		b.int_value = val.(int32)
	case WRITE:
		b.value = val
	case LIST:
		if val == nil {
			b.entries = make([]Entry, 0)
		} else {
			b.entries = make([]Entry, 1)
			b.entries[0] = val.(Entry)
		}
	case ORDERED_PAIR:
		return nil
	}
	return b
}

func (br *BRecord) Value() Value {
	switch br.key_type {
	case SUM:
		return br.int_value
	case MAX:
		return br.int_value
	case WRITE:
		return br.value
	case LIST:
		return br.entries
	}
	return nil
}

// Used during "normal" phase
func (br *BRecord) Lock() bool {
	x := br.last.Lock()
	if *Conflicts {
		if !x {
			atomic.AddInt32(&br.conflict, 1)
		}
	}
	return x
}

func (br *BRecord) Unlock(tid TID) {
	br.last.Unlock(uint64(tid))
}

func (br *BRecord) IsUnlocked() (bool, uint64) {
	x := br.last.Read()
	if x&wfmutex.LOCKED != 0 {
		if *Conflicts {
			// warning!  turning a read-only thing into a read/write!
			atomic.AddInt32(&br.conflict, 1)
		}
		return false, x
	}
	return true, x
}

func (br *BRecord) Verify(last uint64) bool {
	ok, new_last := br.IsUnlocked()
	if !ok {
		return false
	}
	if uint64(new_last) != last {
		if *Conflicts {
			atomic.AddInt32(&br.conflict, 1)
		}
		return false
	}
	return true
}

// Used during "merge" phase, along with br.mu
func (br *BRecord) Apply(val Value) {
	switch br.key_type {
	case SUM:
		delta := val.(int32)
		atomic.AddInt32(&br.int_value, delta)
	case MAX:
		delta := val.(int32)
		br.mu.Lock()
		defer br.mu.Unlock()
		if br.int_value < delta {
			br.int_value = delta
		}
	case WRITE:
		br.mu.Lock()
		defer br.mu.Unlock()
		br.value = val
	case LIST:
		br.mu.Lock()
		defer br.mu.Unlock()
		entries := val.([]Entry)
		br.listApply(entries)
	}
	if *Conflicts {
		br.conflict = 0
	}
}

type Entry struct {
	order int
	key   Key
	top   int
}

const (
	DEFAULT_LIST_SIZE = 10
)

func (br *BRecord) AddOneToRecord(e Entry) {
	br.entries = AddOneToList(br.entries, e)
}

func AddOneToList(lst []Entry, e Entry) []Entry {
	added := false
	for i := 0; i < len(lst); i++ {
		if lst[i].order < e.order {
			lst := append(lst, Entry{})
			copy(lst[i+1:], lst[i:])
			lst[i] = e
			added = true
			break
		}
	}
	if added {
		if len(lst) <= DEFAULT_LIST_SIZE {
			//
		} else {
			lst = lst[:DEFAULT_LIST_SIZE]
		}
	} else if len(lst) < DEFAULT_LIST_SIZE {
		// This goes at the end
		lst = append(lst, e)
	} else {
		lst = lst[:DEFAULT_LIST_SIZE]
	}
	if len(lst) > DEFAULT_LIST_SIZE {
		log.Fatalf("How did this happen?  %v %v %v\n", e, lst, lst[:DEFAULT_LIST_SIZE])
	}
	return lst
}

// default desc
func (br *BRecord) listApply(entries []Entry) {
	lidx := 0
	widx := 0
	i := 0
	new_entries := make([]Entry, DEFAULT_LIST_SIZE)
	for i < DEFAULT_LIST_SIZE {
		if lidx < len(br.entries) && widx < len(entries) && br.entries[lidx].order > entries[widx].order {
			new_entries[i] = br.entries[lidx]
			lidx++
			i++
		} else if widx < len(entries) {
			new_entries[i] = entries[widx]
			widx++
			i++
		} else if lidx < len(br.entries) {
			new_entries[i] = br.entries[lidx]
			widx++
			i++
		} else {
			break
		}
	}
	br.entries = new_entries[:i]
}
