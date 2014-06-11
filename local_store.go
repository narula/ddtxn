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

// idea of *currently running transaction* to save allocations?
// re-use ETransaction
// 2PL?
// Phase?
// allocate results

// potential outcomes:
//   - success no results
//   - success results
//   - abort
//   - stash

type Write struct {
	key    Key
	br     *BRecord
	v      Value
	op     KeyType
	create bool
	locked bool
	vint32 int32
	dd     bool
}

type ETransaction struct {
	read   []*BRecord
	lasts  []uint64
	w      *Worker
	s      *Store
	ls     *LocalStore
	writes []Write
}

// Re-use this?
func StartTransaction(w *Worker) *ETransaction {
	tx := &ETransaction{
		read:   make([]*BRecord, 0, 30),
		lasts:  make([]uint64, 0, 30),
		writes: make([]Write, 0, 60),
		w:      w,
		s:      w.store,
		ls:     w.local_store,
	}
	return tx
}

func (tx *ETransaction) Reset() {
	tx.lasts = tx.lasts[:0]
	tx.read = tx.read[:0]
	tx.writes = tx.writes[:0]
}

func (tx *ETransaction) Read(k Key) (*BRecord, error) {
	if *SysType == DOPPEL {
		if tx.ls.phase == SPLIT {
			// When there is a small amount of dd, ranging over the
			// map is faster than using hash_lookup!
			for i := 0; i < len(tx.s.dd); i++ {
				if k == tx.s.dd[i] {
					return nil, ESTASH
				}
			}
		}
	}
	// TODO: If I wrote the key, return that value instead
	br, err := tx.s.getKey(k)
	if err != nil {
		return nil, err
	}
	ok, last := br.IsUnlocked()
	// if locked and not by me, abort
	// else note the last timestamp, save it, return value
	if !ok {
		if *SysType == DOPPEL {
			tx.ls.candidates.Conflict(k)
		}
		tx.Abort()
		return nil, EABORT
	}
	n := len(tx.read)
	tx.read = tx.read[0 : n+1]
	tx.read[n] = br
	tx.lasts = tx.lasts[0 : n+1]
	tx.lasts[n] = last
	return br, nil
}

func (tx *ETransaction) add(k Key, v Value, op KeyType, create bool) {
	if len(tx.writes) == cap(tx.writes) {
		// TODO: extend
		log.Fatalf("Ran out of room\n")
	}
	n := len(tx.writes)
	tx.writes = tx.writes[0 : n+1]
	tx.writes[n].key = k
	tx.writes[n].br = nil
	tx.writes[n].v = v
	tx.writes[n].op = op
	tx.writes[n].create = create
	tx.writes[n].locked = false
}

func (tx *ETransaction) addInt32(k Key, v int32, op KeyType, create bool) {
	if len(tx.writes) == cap(tx.writes) {
		// TODO: extend
		log.Fatalf("Ran out of room\n")
	}
	n := len(tx.writes)
	tx.writes = tx.writes[0 : n+1]
	tx.writes[n].key = k
	tx.writes[n].br = nil
	tx.writes[n].vint32 = v
	tx.writes[n].op = op
	tx.writes[n].create = create
	tx.writes[n].locked = false
}

func (tx *ETransaction) WriteInt32(k Key, a int32, op KeyType) {
	tx.addInt32(k, a, op, false)
}

func (tx *ETransaction) Write(k Key, v Value, kt KeyType) {
	if kt == SUM || kt == MAX {
		tx.addInt32(k, v.(int32), kt, true)
		return
	}
	tx.add(k, v, kt, true)
}

func (tx *ETransaction) Abort() TID {
	for i, _ := range tx.writes {
		if tx.writes[i].locked {
			tx.writes[i].br.Unlock(0)
		}
	}
	return 0
}

func (tx *ETransaction) Commit() TID {
	// for each write key
	//  if global get from global store and lock
	for i, _ := range tx.writes {
		w := &tx.writes[i]
		if *SysType == DOPPEL && tx.ls.phase == SPLIT && tx.s.IsDD(w.key) {
			w.dd = true
			continue
		}
		if w.br == nil {
			br, err := tx.s.getKey(w.key)
			if br == nil || err != nil {
				switch w.op {
				case SUM:
					br = tx.s.CreateInt32Key(w.key, w.vint32, w.op)
				case MAX:
					br = tx.s.CreateInt32Key(w.key, w.vint32, w.op)
				default:
					br = tx.s.CreateKey(w.key, "", WRITE)
				}
			}
			w.br = br
		}
		if !w.br.Lock() {
			if *SysType == DOPPEL {
				tx.ls.candidates.Conflict(w.key)
			}
			return tx.Abort()
		}
		w.locked = true
	}
	// TODO: acquire timestamp higher than anything i've read or am
	// writing
	tid := tx.w.commitTID()

	// for each read key
	//  verify
	if len(tx.read) != len(tx.lasts) {
		debug.PrintStack()
		log.Fatalf("Mismatch in lengths reads: %v, lasts: %v\n", tx.read, tx.lasts)
	}
	count := false
	if *SysType == DOPPEL && tid%1000 == 0 {
		count = true
	}
	for i, _ := range tx.read {
		if count {
			tx.ls.candidates.Read(tx.read[i].key)
		}
		rd := false
		if !tx.read[i].Verify(tx.lasts[i]) {
			for j, _ := range tx.writes {
				if tx.writes[j].key == tx.read[i].key {
					// We would have aborted if we did not successfully
					// lock this earlier
					rd = true
					break
				}
			}
			if rd {
				continue
			}
			if *SysType == DOPPEL {
				tx.ls.candidates.Conflict(tx.read[i].key)
			}
			return tx.Abort()
		}
	}
	// for each write key
	//  if dd and split phase, apply locally
	//  else apply globally and unlock
	for i, _ := range tx.writes {
		w := &tx.writes[i]
		if tx.ls.phase == SPLIT && w.dd {
			if count {
				tx.ls.candidates.Write(w.key)
			}
			switch w.op {
			case SUM:
				tx.ls.Apply(w.key, w.op, w.vint32, w.op)
			case MAX:
				tx.ls.Apply(w.key, w.op, w.vint32, w.op)
			default:
				tx.ls.Apply(w.key, w.op, w.v, w.op)
			}
		} else {
			switch w.op {
			case SUM:
				tx.s.SetInt32(w.br, w.vint32, w.op)
			case MAX:
				tx.s.SetInt32(w.br, w.vint32, w.op)
			default:
				tx.s.Set(w.br, w.v, w.op)
			}
			w.br.Unlock(tid)
		}
	}
	return tid
}
