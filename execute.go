package ddtxn

import (
	"ddtxn/dlog"
	"flag"
	"log"
	"math/rand"
)

var SampleRate = flag.Int64("sr", 1000, "Sample every sr transactions\n")

// Phases
const (
	SPLIT = iota
	MERGE
	JOIN
)

// TODO: Handle writing more than once to a key in one transaction
type Write struct {
	key    Key
	br     *BRecord
	v      Value
	op     KeyType
	locked bool
	vint32 int32
	ve     Entry
}

type ReadKey struct {
	key  Key
	br   *BRecord
	last uint64
}

type ETransaction interface {
	Reset()
	Read(k Key) (*BRecord, error)
	WriteInt32(k Key, a int32, op KeyType) error
	WriteList(k Key, l Entry, kt KeyType)
	Write(k Key, v Value, kt KeyType)
	Abort() TID
	Commit() TID
	SetPhase(int)
	GetPhase() int
	Store() *Store

	// Tell 2PL I am going to read and potentially write this key.
	// This is because I don't know how to upgrade locks.
	MaybeWrite(k Key)

	// Tell Doppel not to count this transaction's reads and writes.
	NoCount()

	// Get a unique key
	UID(rune) uint64
}

// Not threadsafe.  Tracks execution of transaction.
type OTransaction struct {
	padding0 [128]byte
	read     []ReadKey
	w        *Worker
	s        *Store
	ls       *LocalStore
	phase    int
	writes   []Write
	t        int64 // Used just as a rough count
	count    bool
	sr_rate  int64
	padding  [128]byte
}

func (tx *OTransaction) UID(f rune) uint64 {
	return tx.w.NextKey(f)
}

func (tx *OTransaction) NoCount() {
	tx.count = false
}

func StartOTransaction(w *Worker) *OTransaction {
	tx := &OTransaction{
		read:    make([]ReadKey, 0, 100),
		writes:  make([]Write, 0, 100),
		w:       w,
		s:       w.store,
		ls:      w.local_store,
		sr_rate: int64(w.ID),
	}
	return tx
}

func (tx *OTransaction) Reset() {
	tx.read = tx.read[:0]
	tx.writes = tx.writes[:0]
	tx.count = (*SysType == DOPPEL && tx.sr_rate == 0)
	if tx.count {
		tx.w.Nstats[NSAMPLES]++
		tx.sr_rate = *SampleRate + int64(rand.Intn(100)) - int64(tx.w.ID)
	} else {
		tx.sr_rate--
	}
	tx.t++
}

func (tx *OTransaction) isSplit(br *BRecord) bool {
	return *SysType == DOPPEL && tx.phase == SPLIT && tx.s.any_dd && br != nil && br.dd
}

func (tx *OTransaction) Read(k Key) (*BRecord, error) {
	if len(tx.read) > 0 {
		for i := 0; i < len(tx.writes); i++ {
			r := &tx.writes[i]
			if r.key == k {
				// I wrote and read the same piece of data in one
				// transaction.  This is a strong signal this
				// shouldn't be dd.
				if tx.count {
					tx.ls.candidates.ReadWrite(k, r.br)
				}
				if tx.isSplit(r.br) {
					return nil, ESTASH
				}
				// TODO: Return the value I wrote instead!
				return r.br, nil
			}
		}
	}
	br, err := tx.s.getKey(k)
	if *CountKeys {
		p, r := UndoCKey(k)
		if r == 'p' {
			tx.w.NKeyAccesses[p]++
		}
	}
	if tx.isSplit(br) {
		if tx.count {
			tx.ls.candidates.Stash(k)
		}
		return nil, ESTASH
	}
	if tx.count {
		tx.ls.candidates.Read(k, br)
	}
	if err == ENOKEY {
		n := len(tx.read)
		tx.read = tx.read[0 : n+1]
		tx.read[n].key = k
		tx.read[n].br = nil
		tx.read[n].last = 0
		return nil, err
	}
	ok, last := br.IsUnlocked()
	// if locked abort
	// else note the last timestamp, save it, return value
	if !ok {
		tx.w.Nstats[NLOCKED]++
		return nil, EABORT
	}
	n := len(tx.read)
	tx.read = tx.read[0 : n+1]
	tx.read[n].key = k
	tx.read[n].br = br
	tx.read[n].last = last
	return br, nil
}

func (tx *OTransaction) WriteInt32(k Key, a int32, op KeyType) error {
	// During the normal phase, Doppel operates just like OCC, for
	// ease of exposition.  That means it would have to put the key
	// into the read set and potentially abort accordingly.  Doing so
	// here, but not using the value until commit time.
	br, err := tx.s.getKey(k)
	if *CountKeys {
		p, r := UndoCKey(k)
		if r == 'p' {
			tx.w.NKeyAccesses[p]++
		}
	}
	if tx.isSplit(br) {
		if tx.count {
			tx.ls.candidates.Write(k, br)
		}
		// Do not need to read-validate
	} else {
		var last uint64
		if br == nil || err == ENOKEY {
			last = 0
		} else {
			var ok bool
			ok, last = br.IsUnlocked()
			if !ok {
				tx.w.Nstats[NLOCKED]++
				if tx.count {
					tx.ls.candidates.Conflict(k, br)
				}
				return EABORT
			}
		}
		// Note the last timestamp and save it
		n := len(tx.read)
		tx.read = tx.read[0 : n+1]
		tx.read[n].key = k
		tx.read[n].br = br
		tx.read[n].last = last
	}
	n := len(tx.writes)
	tx.writes = tx.writes[0 : n+1]
	tx.writes[n].key = k
	tx.writes[n].br = br
	tx.writes[n].vint32 = a
	tx.writes[n].op = op
	tx.writes[n].locked = false
	return nil
}

func (tx *OTransaction) Write(k Key, v Value, kt KeyType) {
	if len(tx.writes) == cap(tx.writes) {
		log.Fatalf("Ran out of room\n")
	}
	n := len(tx.writes)
	tx.writes = tx.writes[0 : n+1]
	tx.writes[n].key = k
	tx.writes[n].br = nil
	tx.writes[n].v = v
	tx.writes[n].op = kt
	tx.writes[n].locked = false
}

// TODO: Read-then-write like WriteInt32
func (tx *OTransaction) WriteList(k Key, l Entry, kt KeyType) {
	if kt != LIST {
		log.Fatalf("Not a list\n")
	}
	if len(tx.writes) == cap(tx.writes) {
		log.Fatalf("Ran out of room\n")
	}
	n := len(tx.writes)
	tx.writes = tx.writes[0 : n+1]
	tx.writes[n].key = k
	tx.writes[n].br = nil
	tx.writes[n].ve = l
	tx.writes[n].op = kt
	tx.writes[n].locked = false
}

func (tx *OTransaction) SetPhase(p int) {
	tx.phase = p
}

func (tx *OTransaction) GetPhase() int {
	return tx.phase
}

func (tx *OTransaction) Store() *Store {
	return tx.s
}

func (tx *OTransaction) Abort() TID {
	for i, _ := range tx.writes {
		if tx.writes[i].locked {
			tx.writes[i].br.Unlock(0)
		}
	}
	return 0
}

func (tx *OTransaction) checkOwnership(br *BRecord, last uint64) bool {
	for j, _ := range tx.writes {
		if tx.writes[j].key == br.key && tx.writes[j].locked {
			if br.Own(last) {
				return true
			}
		}
	}
	return false
}

func (tx *OTransaction) Commit() TID {
	// for each write key
	//  if global get from global store and lock
	for i, _ := range tx.writes {
		w := &tx.writes[i]
		if w.br == nil {
			var err error
			w.br, err = tx.s.getKey(w.key)
			if *CountKeys {
				p, r := UndoCKey(w.key)
				if r == 'p' {
					tx.w.NKeyAccesses[p]++
				}
			}
			// Data doesn't exist, create it
			if w.br == nil || err == ENOKEY {
				var err2 error
				w.br, err2 = tx.s.CreateLockedKey(w.key, w.op)
				if err2 != nil {
					// Someone snuck in and created the key
					if tx.count {
						tx.ls.candidates.Conflict(w.key, w.br)
					}
					tx.w.Nstats[NFAIL_VERIFY]++
					dlog.Printf("Fail verify key %v\n", w.key)
					return tx.Abort()
				}
				w.locked = true
				continue
			}
		}
		if tx.isSplit(w.br) {
			continue
		}
		if !w.br.Lock() {
			tx.w.Nstats[NO_LOCK]++
			if tx.count {
				tx.ls.candidates.Conflict(w.key, w.br)
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
	for i, _ := range tx.read {
		rk := &tx.read[i]
		var err error
		if rk.br == nil {
			rk.br, err = tx.s.getKey(rk.key)
			if *CountKeys {
				p, r := UndoCKey(rk.key)
				if r == 'p' {
					tx.w.NKeyAccesses[p]++
				}
			}
			// Verify it still doesn't exist or I'm the one who
			// created and locked it to write
			if err == ENOKEY || tx.checkOwnership(rk.br, rk.last) {
				continue
			}
			tx.w.Nstats[NFAIL_VERIFY]++
			dlog.Printf("Fail verify2 key %v\n", rk.br.key)
			return tx.Abort()
		}
		if rk.br.Verify(rk.last) {
			continue
		}
		// It didn't verify, but I might own it because I wrote it
		if tx.checkOwnership(rk.br, rk.last) {
			continue
		}
		tx.w.Nstats[NFAIL_VERIFY]++
		return tx.Abort()
	}
	// for each write key
	//  if dd and split phase, apply locally
	//  else apply globally and unlock
	for i, _ := range tx.writes {
		w := &tx.writes[i]
		if tx.isSplit(w.br) {
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
				if w.br == nil {
					log.Fatalf("How is this nil?\n")
				}
				tx.s.Set(w.br, w.v, w.op)
			}
			w.br.Unlock(tid)
		}
	}
	return tid
}

func (tx *OTransaction) MaybeWrite(k Key) {
	// no op
}

type Rec struct {
	br     *BRecord
	read   bool
	v      interface{}
	vint32 int32
	ve     Entry
	kt     KeyType
	noset  bool
}

// Not threadsafe.  Tracks execution of transaction.
type LTransaction struct {
	padding0 [128]byte
	keys     []Rec
	w        *Worker
	s        *Store
	t        int64 // Used just as a rough count
	ls       *LocalStore
	phase    int
	padding  [128]byte
}

func StartLTransaction(w *Worker) *LTransaction {
	tx := &LTransaction{
		keys: make([]Rec, 0, 100),
		w:    w,
		s:    w.store,
		ls:   w.local_store,
	}
	return tx
}

func (tx *LTransaction) Reset() {
	tx.keys = tx.keys[:0]
	tx.t++
}

func (tx *LTransaction) UID(f rune) uint64 {
	return tx.w.NextKey(f)
}

func (tx *LTransaction) Read(k Key) (*BRecord, error) {
	// TODO: If I wrote the key, return that value instead
	if exists, n := tx.already_exists(k); exists {
		return tx.keys[n].br, nil
	}
	br, err := tx.s.getKey(k)
	if *CountKeys {
		p, r := UndoCKey(k)
		if r == 'p' {
			tx.w.NKeyAccesses[p]++
		}
	}
	if err != nil {
		// TODO: Create and read lock for an empty key?
		return nil, err
	}
	dlog.Printf("%v Read() Rlocking %v\n", tx.w.ID, k)
	br.SRLock()
	n := len(tx.keys)
	tx.keys = tx.keys[0 : n+1]
	tx.keys[n] = Rec{br: br, read: true, noset: false}
	return br, nil
}

// This is when I am reading a key and I might write it later; acquire
// the write lock *before* the read.
func (tx *LTransaction) MaybeWrite(k Key) {
	if exists, _ := tx.already_exists(k); exists {
		log.Fatalf("Shouldn't already have a lock on this\n")
	}
	br, err := tx.s.getKey(k)
	if *CountKeys {
		p, r := UndoCKey(k)
		if r == 'p' {
			tx.w.NKeyAccesses[p]++
		}
	}
	if br == nil || err != nil {
		// Will acquire lock when I do the write and create the key.
		//dlog.Printf("Key doesn't exist? %v %v\n", k, err)
		return
	}
	dlog.Printf("%v Locking %v in MaybeWrite\n", tx.w.ID, br.key)
	br.SLock()
	n := len(tx.keys)
	tx.keys = tx.keys[0 : n+1]
	tx.keys[n] = Rec{br: br, read: false, noset: true}
}

func (tx *LTransaction) already_exists(k Key) (bool, int) {
	n := len(tx.keys)
	for i := 0; i < len(tx.keys); i++ {
		if tx.keys[i].br.key == k {
			return true, i
		}
	}
	if len(tx.keys) == cap(tx.keys) {
		// TODO: extend
		log.Fatalf("Ran out of room\n")
	}
	return false, n
}

func (tx *LTransaction) make_or_get_key(k Key, op KeyType) *BRecord {
	br, err := tx.s.getKey(k)
	if br != nil && err == nil {
		dlog.Printf("%v Locking %v in make_or_get_key\n", tx.w.ID, k)
		br.SLock()
		return br
	}
	//dlog.Printf("Error %v %v %v\n", k, err)
	var err2 error
	dlog.Printf("%v Perhaps locking %v in make_or_get_key, Create\n", tx.w.ID, k)
	br, err2 = tx.s.CreateMuLockedKey(k, op)
	if br == nil || err2 != nil {
		dlog.Printf("%v Error creating key: %v %v\n", tx.w.ID, k, err2)
		br, err = tx.s.getKey(k)
		if err != nil {
			log.Fatalf("Should exist\n")
		}
		br.SLock()
	}
	return br
}

func (tx *LTransaction) WriteInt32(k Key, a int32, op KeyType) error {
	exists, n := tx.already_exists(k)
	if exists {
		if tx.keys[n].read == true {
			log.Fatalf("Already have read lock on this key; cannot upgrade %v\n", k)
		}
		// Already locked.  TODO: aggregate
		//dlog.Printf("%v Double write?\n", tx.w.ID, tx.keys[n].vint32)
		tx.keys[n].vint32 = a
		tx.keys[n].kt = op
		tx.keys[n].noset = false
		return nil
	}
	br := tx.make_or_get_key(k, op)
	tx.keys = tx.keys[0 : n+1]
	tx.keys[n] = Rec{br: br, read: false, vint32: a, kt: op, noset: false}
	return nil
}

func (tx *LTransaction) Write(k Key, v Value, op KeyType) {
	if op == SUM || op == MAX {
		tx.WriteInt32(k, v.(int32), op)
		return
	}
	exists, n := tx.already_exists(k)
	if exists {
		if tx.keys[n].read == true {
			log.Fatalf("Already have read lock on this key; cannot upgrade %v\n", k)
		}
		// Already locked.
		tx.keys[n].v = v
		tx.keys[n].kt = op
		return
	}
	br := tx.make_or_get_key(k, op)
	tx.keys = tx.keys[0 : n+1]
	tx.keys[n] = Rec{br: br, read: false, v: v, kt: op, noset: false}
}

func (tx *LTransaction) WriteList(k Key, l Entry, op KeyType) {
	if op != LIST {
		log.Fatalf("Not a list\n")
	}
	exists, n := tx.already_exists(k)
	if exists {
		if tx.keys[n].read == true {
			log.Fatalf("Already have read lock on this key; cannot upgrade %v\n", k)
		}
		// Already locked.  TODO: append
		tx.keys[n].ve = l
		tx.keys[n].kt = op
		return
	}
	br := tx.make_or_get_key(k, op)
	tx.keys = tx.keys[0 : n+1]
	tx.keys[n] = Rec{br: br, read: false, ve: l, kt: op, noset: false}
}

func (tx *LTransaction) SetPhase(p int) {
	tx.phase = p
}

func (tx *LTransaction) GetPhase() int {
	return tx.phase
}

func (tx *LTransaction) Store() *Store {
	return tx.s
}

func (tx *LTransaction) Abort() TID {
	for i := len(tx.keys) - 1; i >= 0; i-- {
		if tx.keys[i].read {
			dlog.Printf("%v Abort() RUnlocking %v\n", tx.w.ID, tx.keys[i].br.key)
			tx.keys[i].br.SRUnlock()
		} else {
			dlog.Printf("%v Abort() Unlocking %v\n", tx.w.ID, tx.keys[i].br.key)
			tx.keys[i].br.SUnlock()
		}
	}
	return 0
}

func (tx *LTransaction) Commit() TID {
	tid := tx.w.commitTID()
	for i := len(tx.keys) - 1; i >= 0; i-- {
		// Apply and unlock
		//
		// TODO: Phantom reads!!  Right now if I read ENOKEY, it is
		// not read locked.
		if tx.keys[i].read == false {
			if tx.keys[i].noset {
				// No changes, we write-locked it because we thought
				// we *might* write
				dlog.Printf("%v Commit() Unlocking %v (noset)\n", tx.w.ID, tx.keys[i].br.key)
				tx.keys[i].br.SUnlock()
				continue
			}
			switch tx.keys[i].kt {
			case SUM:
				tx.s.SetInt32(tx.keys[i].br, tx.keys[i].vint32, tx.keys[i].kt)
			case MAX:
				tx.s.SetInt32(tx.keys[i].br, tx.keys[i].vint32, tx.keys[i].kt)
			case LIST:
				tx.s.Set(tx.keys[i].br, tx.keys[i].ve, tx.keys[i].kt)
			default:
				tx.s.Set(tx.keys[i].br, tx.keys[i].v, tx.keys[i].kt)
			}
			dlog.Printf("%v Commit() Unlocking %v\n", tx.w.ID, tx.keys[i].br.key)
			tx.keys[i].br.SUnlock()
		} else {
			dlog.Printf("%v Commit() RUnlocking %v\n", tx.w.ID, tx.keys[i].br.key)
			tx.keys[i].br.SRUnlock()
		}
	}
	return tid
}

func (tx *LTransaction) NoCount() {
	// noop
}
