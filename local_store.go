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

type Write struct {
	br     *BRecord
	v      Value
	op     KeyType
	create bool
}

type ETransaction struct {
	q      *Query
	lasts  map[*BRecord]uint64
	writes map[Key]Write
	locked map[*BRecord]bool
	w      *Worker
	s      *Store
	ls     *LocalStore
}

// Re-use this?
func StartTransaction(q *Query, w *Worker) *ETransaction {
	tx := &ETransaction{
		q:      q,
		lasts:  make(map[*BRecord]uint64),
		writes: make(map[Key]Write),
		locked: make(map[*BRecord]bool),
		w:      w,
		s:      w.store,
		ls:     w.local_store,
	}
	return tx
}

func (tx *ETransaction) Read(k Key) (*BRecord, error) {
	br, err := tx.s.readKey(k)
	if err != nil {
		return nil, err
	}
	if br.dd {
		// Increment something
		tx.w.stashTxn(*tx.q)
		return nil, ESTASH
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

func (tx *ETransaction) Write(k Key, v Value, op KeyType) {
	// Optimization: check to see if tx already tried to read or write
	// it and it's in tx.lasts or tx.writes map.
	br, err := tx.s.writeKey(k)
	if err != nil {
		debug.PrintStack()
		log.Fatalf("no key %v %v\n", k, v)
	}
	tx.writes[k] = Write{br, v, op, false}
}

func (tx *ETransaction) WriteOrCreate(k Key, v Value, kt KeyType) {
	br := tx.s.getOrCreateTypedKey(k, v, kt)
	tx.writes[k] = Write{br, v, kt, true}
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
	for _, w := range tx.writes {
		br := w.br
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
	for _, w := range tx.writes {
		if w.br.dd {
			tx.ls.Apply(w.br, w.v, w.op)
		} else {
			tx.s.Set(w.br, w.v, w.op)
			w.br.Unlock(tid)
		}
	}
	return tid
}
