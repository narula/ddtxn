package ddtxn

import (
	"flag"
	"log"
	"runtime/debug"
)

var SampleRate = flag.Int64("sr", 1000, "Sample every sr nanoseconds\n")

// TODO: 2PL

type Write struct {
	key    Key
	br     *BRecord
	v      Value
	op     KeyType
	create bool
	locked bool
	vint32 int32

	// TODO: Handle writing more than once to a list per txn
	ve Entry
	dd bool
}

// Not threadsafe.  Tracks execution of transaction.
type ETransaction struct {
	read    []*BRecord
	lasts   []uint64
	w       *Worker
	s       *Store
	ls      *LocalStore
	writes  []Write
	t       int64 // Used just as a rough count
	padding [128]byte
}

func StartTransaction(w *Worker) *ETransaction {
	tx := &ETransaction{
		read:   make([]*BRecord, 0, 100),
		lasts:  make([]uint64, 0, 100),
		writes: make([]Write, 0, 100),
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
	tx.ls.count = (*SysType == DOPPEL && tx.t%*SampleRate == 0)
	if tx.ls.count {
		tx.w.Nstats[NSAMPLES]++
	}
	tx.t++
}

func (tx *ETransaction) Read(k Key) (*BRecord, error) {
	if *SysType == DOPPEL {
		if tx.ls.phase == SPLIT {
			if tx.s.IsDD(k) {
				if tx.ls.count {
					tx.ls.candidates.Stash(k)
				}
				return nil, ESTASH
			}
		}
	}
	// TODO: If I wrote the key, return that value instead
	br, err := tx.s.getKey(k)
	tx.w.Nstats[NGETKEYCALLS]++
	if *CountKeys {
		p, r := UndoCKey(k)
		if r == 117 {
			tx.w.NKeyAccesses[p]++
		}
	}
	if err != nil {
		return nil, err
	}
	ok, last := br.IsUnlocked()
	// if locked and not by me, abort
	// else note the last timestamp, save it, return value
	if !ok {
		if tx.ls.count {
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

func (tx *ETransaction) addList(k Key, v Entry, op KeyType, create bool) {
	if len(tx.writes) == cap(tx.writes) {
		// TODO: extend
		log.Fatalf("Ran out of room\n")
	}
	n := len(tx.writes)
	tx.writes = tx.writes[0 : n+1]
	tx.writes[n].key = k
	tx.writes[n].br = nil
	tx.writes[n].ve = v
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

func (tx *ETransaction) WriteList(k Key, l Entry, kt KeyType) {
	if kt != LIST {
		log.Fatalf("Not a list\n")
	}
	tx.addList(k, l, kt, true)
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
			//tx.w.Nstats[NGETKEYCALLS]++
			if *CountKeys {
				p, r := UndoCKey(w.key)
				if r == 117 {
					tx.w.NKeyAccesses[p]++
				}
			}
			if br == nil || err != nil {
				switch w.op {
				case SUM:
					br = tx.s.CreateInt32Key(w.key, 0, w.op)
				case MAX:
					br = tx.s.CreateInt32Key(w.key, 0, w.op)
				case LIST:
					br = tx.s.CreateKey(w.key, nil, LIST)
				default:
					if w.v == nil {
						br = tx.s.CreateKey(w.key, "", WRITE)
					} else {
						br = tx.s.CreateKey(w.key, w.v, WRITE)
					}
				}
			}
			w.br = br
		}
		if !w.br.Lock() {
			if tx.ls.count {
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
	for i, _ := range tx.read {
		// Would have checked for dd earlier
		if tx.ls.count {
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
			if tx.ls.count {
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
			if tx.ls.count {
				tx.ls.candidates.Write(w.key)
			}
			//tx.w.Nstats[NDDWRITES]++
			switch w.op {
			case SUM:
				tx.ls.Apply(w.key, w.op, w.vint32, w.op)
			case MAX:
				tx.ls.Apply(w.key, w.op, w.vint32, w.op)
			case LIST:
				tx.ls.ApplyList(w.key, w.ve)
			default:
				tx.ls.Apply(w.key, w.op, w.v, w.op)
			}
		} else {
			switch w.op {
			case SUM:
				tx.s.SetInt32(w.br, w.vint32, w.op)
			case MAX:
				tx.s.SetInt32(w.br, w.vint32, w.op)
			case LIST:
				tx.s.Set(w.br, w.ve, w.op)
			default:
				tx.s.Set(w.br, w.v, w.op)
			}
			w.br.Unlock(tid)
		}
	}
	return tid
}
