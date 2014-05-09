package ddtxn

import (
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

func (ls *LocalStore) Apply(br *BRecord, v Value) {
	switch br.key_type {
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
	}

	for k, v := range ls.bw {
		if *SysType == OCC {
			debug.PrintStack()
			log.Fatalf("Why is there derived data %v %v\n", k, v)
		}

		d := ls.s.getOrCreateTypedKey(k, "", WRITE)
		d.Apply(v)
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

type ETransaction struct {
	t      *Transaction
	lasts  map[*BRecord]uint64
	writes map[Key]*BRecord
	values map[Key]Value
	locked map[*BRecord]bool
	w      *Worker
	s      *Store
	ls     *LocalStore
}

// Re-use this?
func StartTransaction(t *Transaction, w *Worker) *ETransaction {
	tx := &ETransaction{
		t:      t,
		lasts:  make(map[*BRecord]uint64),
		writes: make(map[Key]*BRecord),
		values: make(map[Key]Value),
		locked: make(map[*BRecord]bool),
		w:      w,
		s:      w.store,
		ls:     w.local_store,
	}
	return tx
}

func (tx *ETransaction) Read(k Key) (*BRecord, error) {
	br, err := tx.s.getKey(k)
	if br.dd {
		//
		tx.w.stashTxn(*tx.t)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	ok, last := br.IsUnlocked()
	// if locked, abort
	// else note the last timestamp, save it, return value
	if !ok {
		tx.Abort()
		return nil, EABORT
	}
	tx.lasts[br] = last
	return br, nil
}

func (tx *ETransaction) Write(k Key, v Value) {
	// Optimization: check to see if tx already tried to read or write
	// it and it's in tx.lasts or tx.writes map.
	br, err := tx.s.getKey(k)
	if err != nil {
		debug.PrintStack()
		log.Fatalf("no key %v %v\n", k, v)
	}
	tx.values[k] = v
	tx.writes[k] = br
}

func (tx *ETransaction) Abort() TID {
	for br, _ := range tx.locked {
		br.Unlock(0)
	}
	return 0
}

func (tx *ETransaction) Commit() TID {
	// for each write key
	//  if global get from global store and lock
	// TODO: if br is nil, create the key
	for _, br := range tx.writes {
		if !br.dd {
			if !br.Lock() {
				return tx.Abort()
			}
			tx.locked[br] = true
		}
	}
	// TODO: acquire timestamp higher than anything i've read or am
	// writing
	tid := tx.w.commitTID()

	// for each read key
	//  verify
	for br, last := range tx.lasts {
		if ok, _ := tx.locked[br]; !ok && !br.Verify(last) {
			return tx.Abort()
		}
	}

	// for each write key
	//  if dd, apply locally
	//  else apply globally and unlock
	for k, br := range tx.writes {
		if br.dd {
			tx.ls.Apply(br, tx.values[k])
		} else {
			tx.s.Set(br, tx.values[k])
			br.Unlock(tid)
		}
	}
	return tid
}
