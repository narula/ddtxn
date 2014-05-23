package ddtxn

import (
	"ddtxn/dlog"
	"log"
	"runtime/debug"
)

// Local per-worker store. Specific types to more quickly apply local
// changes

type LocalStore struct {
	sums  map[Key]int32
	max   map[Key]int32
	bw    map[Key]Value
	lists map[Key][]Entry
	s     *Store
	stash bool
	Ncopy int64
}

func NewLocalStore(s *Store) *LocalStore {
	ls := &LocalStore{
		sums:  make(map[Key]int32),
		max:   make(map[Key]int32),
		bw:    make(map[Key]Value),
		lists: make(map[Key][]Entry),
		s:     s,
	}
	return ls
}

func (ls *LocalStore) Apply(br *BRecord, v Value, op KeyType) {
	if op != br.key_type {
		// Perhaps do something.  When is this set?
		dlog.Printf("Different op types %v %v %v\n", br.key_type, op, br)
	}
	switch op {
	case SUM:
		ls.sums[br.key] += v.(int32)
	case MAX:
		delta := v.(int32)
		if ls.max[br.key] < delta {
			ls.max[br.key] = delta
		}
	case WRITE:
		ls.bw[br.key] = v
	case LIST:
		entry := v.(Entry)
		l, ok := ls.lists[br.key]
		if !ok {
			l = make([]Entry, 0)
			ls.lists[br.key] = l
		}
		ls.lists[br.key] = append(l, entry)
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
	br     *BRecord
	v      Value
	op     KeyType
	create bool
	locked bool
	vint32 int32
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
		writes: make([]Write, 0, 30),
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
	br, err := tx.s.getKey(k)
	if err != nil {
		return nil, err
	}
	if *SysType == DOPPEL && tx.ls.stash && br.dd {
		return nil, ESTASH
	}
	if err != nil {
		return nil, err
	}
	ok, last := br.IsUnlockedNoCount()
	// if locked, abort
	// else note the last timestamp, save it, return value
	if !ok {
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

func (tx *ETransaction) add(k Key, br *BRecord, v Value, op KeyType, create bool) {
	if len(tx.writes) == cap(tx.writes) {
		// TODO: extend
		log.Fatalf("Ran out of room\n")
	}
	n := len(tx.writes)
	tx.writes = tx.writes[0 : n+1]
	tx.writes[n].br = br
	tx.writes[n].v = v
	tx.writes[n].op = op
	tx.writes[n].create = create
	tx.writes[n].locked = false
}

func (tx *ETransaction) addInt32(k Key, br *BRecord, v int32, op KeyType, create bool) {
	if len(tx.writes) == cap(tx.writes) {
		// TODO: extend
		log.Fatalf("Ran out of room\n")
	}
	n := len(tx.writes)
	tx.writes = tx.writes[0 : n+1]
	tx.writes[n].br = br
	tx.writes[n].vint32 = v
	tx.writes[n].op = op
	tx.writes[n].create = create
	tx.writes[n].locked = false
}

func (tx *ETransaction) WriteInt32(k Key, a int32, op KeyType) {
	br, err := tx.s.getKey(k)
	if err != nil {
		br = tx.s.CreateInt32Key(k, a, op)
	}
	tx.addInt32(k, br, a, op, false)
}

func (tx *ETransaction) Write(k Key, v Value, kt KeyType) {
	br := tx.s.getOrCreateTypedKey(k, v, kt)
	if kt == SUM || kt == MAX {
		tx.addInt32(k, br, v.(int32), kt, true)
		return
	}
	tx.add(k, br, v, kt, true)
}

func (tx *ETransaction) Abort() TID {
	for _, w := range tx.writes {
		if w.locked {
			w.br.Unlock(0)
		}
	}
	return 0
}

func (tx *ETransaction) Commit() TID {
	// for each write key
	//  if global get from global store and lock
	// TODO: if br is nil, create the key
	for i, _ := range tx.writes {
		br := tx.writes[i].br
		if !br.dd {
			if !br.Lock() {
				return tx.Abort()
			}
			tx.writes[i].locked = true
		}
	}
	// TODO: acquire timestamp higher than anything i've read or am
	// writing
	tid := tx.w.commitTID()

	// for each read key
	//  verify
	for i, br := range tx.read {
		if !br.Verify(tx.lasts[i]) {
			// Check to make sure we aren't writing the key as well
			return tx.Abort()
		}
	}
	// for each write key
	//  if dd, apply locally
	//  else apply globally and unlock
	for i, _ := range tx.writes {
        w := &tx.writes[i]
		if tx.w.ID == 0 && *SysType == DOPPEL {
			tx.s.checkLock(w.br)
		}
		if w.br.dd {
			switch w.op {
			case SUM:
				tx.ls.Apply(w.br, w.vint32, w.op)
			case MAX:
				tx.ls.Apply(w.br, w.vint32, w.op)
			default:
				tx.ls.Apply(w.br, w.v, w.op)
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
