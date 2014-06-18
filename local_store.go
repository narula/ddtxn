package ddtxn

import (
	"ddtxn/dlog"
	"log"
	"runtime/debug"
)

// Local per-worker store. Specific types to more quickly apply local
// changes

// Phases
const (
	SPLIT = iota
	MERGE
	JOIN
)

type LocalStore struct {
	sums       map[Key]int32
	max        map[Key]int32
	bw         map[Key]Value
	lists      map[Key][]Entry
	s          *Store
	phase      uint32
	Ncopy      int64
	candidates *Candidates
	count      bool
}

func NewLocalStore(s *Store) *LocalStore {
	x := make([]*OneStat, 0)
	sh := StatsHeap(x)
	ls := &LocalStore{
		sums:       make(map[Key]int32),
		max:        make(map[Key]int32),
		bw:         make(map[Key]Value),
		lists:      make(map[Key][]Entry),
		s:          s,
		candidates: &Candidates{make(map[Key]*OneStat), &sh},
	}
	return ls
}

func (ls *LocalStore) Apply(key Key, key_type KeyType, v Value, op KeyType) {
	if op != key_type {
		// Perhaps do something.  When is this set?
		dlog.Printf("Different op types %v %v\n", key_type, op)
	}
	switch op {
	case SUM:
		ls.sums[key] += v.(int32)
	case MAX:
		delta := v.(int32)
		if ls.max[key] < delta {
			ls.max[key] = delta
		}
	case WRITE:
		ls.bw[key] = v
	case LIST:
		entry := v.(Entry)
		l, ok := ls.lists[key]
		if !ok {
			l = make([]Entry, 0)
			ls.lists[key] = l
		}
		// TODO: Use listApply or add one to list to keep them sorted
		ls.lists[key] = append(l, entry)
	}
}

func (ls *LocalStore) Merge() {
	for k, v := range ls.sums {
		if *SysType == OCC {
			debug.PrintStack()
			log.Fatalf("Why is there derived data %v %v\n", k, v)
		}
		if v == 0 {
			continue
		}
		if v == 0 {
			continue
		}
		d := ls.s.getOrCreateTypedKey(k, int32(0), SUM)
		d.Apply(v)
		ls.sums[k] = 0
		ls.Ncopy++
	}

	for k, v := range ls.max {
		if *SysType == OCC {
			debug.PrintStack()
			log.Fatalf("Why is there derived data %v %v\n", k, v)
		}

		if v == 0 {
			continue
		}
		d := ls.s.getOrCreateTypedKey(k, int32(0), MAX)
		d.Apply(v)
		ls.Ncopy++
	}

	for k, v := range ls.bw {
		if *SysType == OCC {
			debug.PrintStack()
			log.Fatalf("Why is there derived data %v %v\n", k, v)
		}

		d := ls.s.getOrCreateTypedKey(k, "", WRITE)
		d.Apply(v)
		ls.Ncopy++
	}

	for k, v := range ls.lists {
		if *SysType == OCC {
			debug.PrintStack()
			log.Fatalf("Why is there derived data %v %v\n", k, v)
		}
		if len(v) == 0 {
			continue
		}

		d := ls.s.getOrCreateTypedKey(k, nil, LIST)
		d.Apply(v)
		delete(ls.lists, k)
		ls.Ncopy++
	}
}
