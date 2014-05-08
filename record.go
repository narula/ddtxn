package ddtxn

import (
	"ddtxn/wfmutex"
	"log"
	"sync"
	"sync/atomic"
)

type KeyType int

const (
	SUM = iota
	MAX
	WRITE
	LIST
)

type BRecord struct {
	mu        sync.RWMutex
	key       Key
	value     Value
	int_value int32
	entries   []Entry
	last      wfmutex.WFMutex
	key_type  KeyType
	locked    int // how many times was the lock already held when someone wanted it
	dd        bool
	lastEpoch uint64
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
	if *Dynamic && *SysType == DOPPEL {
		if !x {
			br.locked++
		}
	}
	return x
}

func (br *BRecord) Unlock(tid TID) {
	if *Dynamic && *SysType == DOPPEL {
		x := CLEAR_TID & uint64(tid)
		if x > br.lastEpoch {
			br.lastEpoch = x
			br.locked = 0
		}
	}
	br.last.Unlock(uint64(tid))
}

func (br *BRecord) IsUnlocked() (bool, uint64) {
	x := br.last.Read()
	if x&wfmutex.LOCKED != 0 {
		if *Dynamic && *SysType == DOPPEL {
			// warning!  turning a read-only thing into a read/write!
			br.locked++
		}
		return false, x
	}
	return true, x
}

func (br *BRecord) Verify(last uint64) bool {
	ok, new_last := br.IsUnlocked()
	if *Dynamic && *SysType == DOPPEL {
		x := CLEAR_TID & last
		if x > br.lastEpoch {
			// warning!  turning a read-only thing into a read/write!
			br.lastEpoch = x
			br.locked = 0
		}
	}
	if !ok || uint64(new_last) != last {
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
	br.locked = 0
}

type Entry struct {
	order int
	key   Key
	top   int
}

const (
	DEFAULT_LIST_SIZE = 10
)

func AddToList(k Key, w *Worker, e Entry) {
	lst := w.derived_list[k]
	added := false
	for i := 0; i < len(lst); i++ {
		if lst[i].order < e.order {
			lst = append(lst, Entry{})
			copy(lst[i+1:], lst[i:])
			lst[i] = e
			w.derived_list[k] = lst
			added = true
			break
		}
	}

	if added {
		if len(w.derived_list[k]) <= DEFAULT_LIST_SIZE {
			return
		} else {
			w.derived_list[k] = w.derived_list[k][:DEFAULT_LIST_SIZE]
		}
	} else if len(w.derived_list[k]) < DEFAULT_LIST_SIZE {
		w.derived_list[k] = append(w.derived_list[k], e)
	} else if len(w.derived_list[k]) > DEFAULT_LIST_SIZE {
		w.derived_list[k] = w.derived_list[k][:DEFAULT_LIST_SIZE]
	}
	if len(w.derived_list[k]) > DEFAULT_LIST_SIZE {
		log.Fatalf("How did this happen AddToList?  %v %v %v\n", e, w.derived_list[k], w.derived_list[k][:DEFAULT_LIST_SIZE])
	}
}

func (br *BRecord) AddOneToList(e Entry) {
	lst := br.entries
	added := false
	for i := 0; i < len(lst); i++ {
		if lst[i].order < e.order {
			lst := append(lst, Entry{})
			copy(lst[i+1:], lst[i:])
			lst[i] = e
			br.entries = lst
			added = true
			break
		}
	}
	if added {
		if len(br.entries) <= DEFAULT_LIST_SIZE {
			//
		} else {
			br.entries = br.entries[:DEFAULT_LIST_SIZE]
		}
	} else if len(br.entries) < DEFAULT_LIST_SIZE {
		br.entries = append(br.entries, e)
	} else {
		br.entries = br.entries[:DEFAULT_LIST_SIZE]
	}
	if len(br.entries) > DEFAULT_LIST_SIZE {
		log.Fatalf("How did this happen?  %v %v %v\n", e, br.entries, br.entries[:DEFAULT_LIST_SIZE])
	}
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
