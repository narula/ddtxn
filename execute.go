package ddtxn

import (
	"ddtxn/dlog"
	"flag"
	"log"
	"math/rand"
)

var SampleRate = flag.Int64("sr", 500, "Sample every sr transactions\n")
var AlwaysSplit = flag.Bool("split", false, "Split every piece of data\n")
var NoConflictType = flag.Int("noconflict", -1, "Type of operation NOT to record conflicts on")

// Phases
const (
	SPLIT = iota
	MERGE
	JOIN
)

// TODO: Handle writing more than once to a key in one transaction
type WriteKey struct {
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
	WriteList(k Key, l Entry, op KeyType) error
	WriteOO(k Key, a int32, v Value, op KeyType) error
	Write(k Key, v Value, op KeyType)
	Abort() TID
	Commit() TID
	SetPhase(int)
	GetPhase() int
	Store() *Store
	Worker() *Worker

	// Tell 2PL I am going to read and potentially write this key.
	// This is because I don't know how to upgrade locks.
	MaybeWrite(k Key)

	// Tell Doppel not to count this transaction's reads and writes
	// when deciding if records should be split.
	NoCount()

	// Get a unique key; give it up
	UID(rune) uint64
	RelinquishKey(uint64, rune)
}

// Tracks execution of transaction.
type OTransaction struct {
	padding0    [128]byte
	read        []ReadKey
	w           *Worker
	s           *Store
	ls          *LocalStore
	phase       int
	writes      []WriteKey
	maxSeen     uint64
	t           int64 // Used just as a rough count
	count       bool
	sr_rate     int64
	dummyRecord *BRecord
	padding     [128]byte
}

func (tx *OTransaction) UID(f rune) uint64 {
	return tx.w.NextKey(f)
}

func (tx *OTransaction) RelinquishKey(n uint64, r rune) {
	tx.w.GiveBack(n, r)
}

func (tx *OTransaction) NoCount() {
	tx.count = false
}

func StartOTransaction(w *Worker) *OTransaction {
	tx := &OTransaction{
		read:        make([]ReadKey, 0, 100),
		writes:      make([]WriteKey, 0, 100),
		w:           w,
		s:           w.store,
		ls:          w.local_store,
		sr_rate:     int64(w.ID),
		dummyRecord: &BRecord{},
	}
	return tx
}

func (tx *OTransaction) Reset() {
	tx.read = tx.read[:0]
	tx.writes = tx.writes[:0]
	tx.t++
	tx.count = (*SysType == DOPPEL && tx.sr_rate == 0)
	if tx.count {
		tx.w.Nstats[NSAMPLES]++
		tx.sr_rate = *SampleRate + int64(rand.Intn(100)) - int64(tx.w.ID)
	} else {
		tx.sr_rate--
	}
}

func (tx *OTransaction) isSplit(br *BRecord) bool {
	if *SysType == DOPPEL {
		if tx.phase == SPLIT {
			if *AlwaysSplit {
				return true
			}
			if tx.s.any_dd {
				if br != nil {
					if br.dd {
						return true
					}
				}
			}
		}
	}
	return false
}

func (tx *OTransaction) Read(k Key) (*BRecord, error) {
	if len(tx.writes) > 0 {
		for i := 0; i < len(tx.writes); i++ {
			w := &tx.writes[i]
			if w.key == k {
				// I wrote and read the same piece of data in one
				// transaction.  This is a strong signal this
				// shouldn't be dd.  Also I should return this value.
				// But I return a pointer to a record (sigh) so use a
				// dummy record.
				if tx.count {
					tx.ls.candidates.ReadWrite(k, w.br)
				}
				if tx.isSplit(w.br) {
					return nil, ESTASH
				}
				tx.dummyRecord.key_type = w.op
				tx.dummyRecord.int_value = w.vint32
				tx.dummyRecord.value = w.v
				if w.op == LIST {
					tx.dummyRecord.entries = tx.dummyRecord.entries[0 : len(w.br.entries)+1]
					copy(tx.dummyRecord.entries, w.br.entries)
					tx.dummyRecord.entries = append(tx.dummyRecord.entries, w.ve)
				}
				return tx.dummyRecord, nil
			}
		}
	}
	br, err := tx.s.getKey(k, tx.w.ld)
	if *CountKeys {
		p, r := UndoCKey(k)
		if r == 'm' {
			tx.w.NKeyAccesses[p]++
		}
	}

	if err == ENOKEY {
		// Can't be stashed, right?
		n := len(tx.read)
		tx.read = tx.read[0 : n+1]
		tx.read[n].key = k
		tx.read[n].br = nil
		tx.read[n].last = 0
		return nil, err
	} else {
		if tx.isSplit(br) {
			if tx.count {
				tx.ls.candidates.Stash(k)
			}
			return nil, ESTASH
		}
		if tx.count {
			tx.ls.candidates.Read(k, br)
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
		if last > tx.maxSeen {
			tx.maxSeen = last
		}
		return br, nil
	}
	log.Fatalf("What")
	return nil, nil
}

func (tx *OTransaction) WriteInt32(k Key, a int32, op KeyType) error {
	// During the normal phase, Doppel operates just like OCC, for
	// ease of exposition.  That means it would have to put the key
	// into the read set and potentially abort accordingly.  Doing so
	// here, but not using the value until commit time.
	br, err := tx.s.getKey(k, tx.w.ld)
	if *CountKeys {
		p, r := UndoCKey(k)
		if r == 'm' {
			tx.w.NKeyAccesses[p]++
		}
	}
	if tx.isSplit(br) {
		if tx.count {
			tx.ls.candidates.Write(k, br, op)
		}
		if br.key_type != op {
			log.Fatalf("%v Doing a write to a non-%v type: %v", k, op, br.key_type)
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
				if tx.count && KeyType(*NoConflictType) != op {
					tx.ls.candidates.Conflict(k, br, op)
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
		if last > tx.maxSeen {
			tx.maxSeen = last
		}
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

func (tx *OTransaction) Write(k Key, v Value, op KeyType) {
	if len(tx.writes) == cap(tx.writes) {
		log.Fatalf("Ran out of room\n")
	}
	n := len(tx.writes)
	tx.writes = tx.writes[0 : n+1]
	tx.writes[n].key = k
	tx.writes[n].br = nil
	tx.writes[n].v = v
	tx.writes[n].op = op
	tx.writes[n].locked = false
}

func (tx *OTransaction) WriteList(k Key, l Entry, op KeyType) error {
	if op != LIST {
		log.Fatalf("Not a list\n")
	}
	if len(tx.writes) == cap(tx.writes) {
		log.Fatalf("Ran out of room\n")
	}

	// During the normal phase, Doppel operates just like OCC, for
	// ease of exposition.  That means it would have to put the key
	// into the read set and potentially abort accordingly.  Doing so
	// here, but not using the value until commit time.
	br, err := tx.s.getKey(k, tx.w.ld)
	if *CountKeys {
		p, r := UndoCKey(k)
		if r == 'm' {
			tx.w.NKeyAccesses[p]++
		}
	}
	if tx.isSplit(br) {
		if tx.count {
			tx.ls.candidates.Write(k, br, op)
		}
		if br.key_type != LIST {
			log.Fatalf("%v Doing a write to a non-LIST type: %v", k, br.key_type)
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
				if tx.count && KeyType(*NoConflictType) != LIST {
					tx.ls.candidates.Conflict(k, br, LIST)
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
		if last > tx.maxSeen {
			tx.maxSeen = last
		}
	}

	n := len(tx.writes)
	tx.writes = tx.writes[0 : n+1]
	tx.writes[n].key = k
	tx.writes[n].br = br
	tx.writes[n].ve = l
	tx.writes[n].op = op
	tx.writes[n].locked = false
	return nil
}

func (tx *OTransaction) WriteOO(k Key, a int32, v Value, op KeyType) error {
	if op != OOWRITE {
		log.Fatalf("Not an OOWRITE\n")
	}
	if len(tx.writes) == cap(tx.writes) {
		log.Fatalf("Ran out of room\n")
	}

	// During the normal phase, Doppel operates just like OCC, for
	// ease of exposition.  That means it would have to put the key
	// into the read set and potentially abort accordingly.  Doing so
	// here, but not using the value until commit time.
	br, err := tx.s.getKey(k, tx.w.ld)
	if *CountKeys {
		p, r := UndoCKey(k)
		if r == 'm' {
			tx.w.NKeyAccesses[p]++
		}
	}
	if tx.isSplit(br) {
		if tx.count {
			tx.ls.candidates.Write(k, br, op)
		}
		if br.key_type != OOWRITE {
			log.Fatalf("%v Doing a write to a non-OOWRITE type: %v", k, br.key_type)
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
				if tx.count && KeyType(*NoConflictType) != OOWRITE {
					tx.ls.candidates.Conflict(k, br, OOWRITE)
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
		if last > tx.maxSeen {
			tx.maxSeen = last
		}
	}

	n := len(tx.writes)
	tx.writes = tx.writes[0 : n+1]
	tx.writes[n].key = k
	tx.writes[n].br = nil
	tx.writes[n].v = v
	tx.writes[n].vint32 = a
	tx.writes[n].op = op
	tx.writes[n].locked = false
	return nil
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

func (tx *OTransaction) Worker() *Worker {
	return tx.w
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
			w.br, err = tx.s.getKey(w.key, tx.w.ld)
			if *CountKeys {
				p, r := UndoCKey(w.key)
				if r == 'm' {
					tx.w.NKeyAccesses[p]++
				}
			}
			// Data doesn't exist, create it
			if w.br == nil || err == ENOKEY {
				if w.br == nil {
					dlog.Printf("w.br was nil %v\n", w.key)
				} else if err == ENOKEY {
					dlog.Printf("err==ENOKEY %v\n", w.key)
				}
				var err2 error
				w.br, err2 = tx.s.CreateLockedKey(w.key, w.op)
				if err2 != nil {
					// Someone snuck in and created the key
					if tx.count && w.op != KeyType(*NoConflictType) {
						tx.ls.candidates.Conflict(w.key, w.br, w.op)
					}
					tx.w.Nstats[NFAIL_VERIFY]++
					//dlog.Printf("Fail verify key %v\n", w.key)
					return tx.Abort()
				}
				w.locked = true
				continue
			}
		}
		if tx.isSplit(w.br) {
			continue
		}
		// Check last TID
		var former uint64
		var ok bool
		if ok, former = w.br.Lock(); !ok {
			tx.w.Nstats[NO_LOCK]++
			if tx.count && w.op != KeyType(*NoConflictType) {
				tx.ls.candidates.Conflict(w.key, w.br, w.op)
			}
			return tx.Abort()
		}
		w.locked = true
		if former > tx.maxSeen {
			tx.maxSeen = former
		}
	}

	// Get TID higher than anything I've seen
	tid := tx.w.commitTID()
	if uint64(tid) < tx.maxSeen {
		tx.w.resetTID(tx.maxSeen)
		tid = tx.w.commitTID()
		if uint64(tid) < tx.maxSeen {
			log.Fatalf("%v MaxSeen %v, reset TID but %v<%v", tx.w.ID, tx.maxSeen, tid, tx.maxSeen)
		}
	}

	// for each read key
	//  verify
	for i, _ := range tx.read {
		rk := &tx.read[i]
		var err error
		if rk.br == nil {
			rk.br, err = tx.s.getKey(rk.key, tx.w.ld)
			if *CountKeys {
				p, r := UndoCKey(rk.key)
				if r == 'm' {
					tx.w.NKeyAccesses[p]++
				}
			}
			// Verify it still doesn't exist or I'm the one who
			// created and locked it to write
			if err == ENOKEY || tx.checkOwnership(rk.br, rk.last) {
				continue
			}
			tx.w.Nstats[NFAIL_VERIFY]++
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
				tx.ls.ApplyInt32(w.key, w.op, w.vint32, w.op)
			case MAX:
				tx.ls.ApplyInt32(w.key, w.op, w.vint32, w.op)
			case LIST:
				tx.ls.ApplyList(w.key, w.ve)
			case OOWRITE:
				tx.ls.ApplyOO(w.key, w.vint32, w.v)
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
				tx.s.SetList(w.br, w.ve, w.op)
			case OOWRITE:
				tx.s.SetOO(w.br, w.vint32, w.v, w.op)
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
	op     KeyType
	noset  bool
	key    Key
}

// Not threadsafe.  Tracks execution of transaction.
type LTransaction struct {
	padding0    [128]byte
	keys        []Rec
	w           *Worker
	s           *Store
	t           int64 // Used just as a rough count
	ls          *LocalStore
	phase       int
	dummyRecord *BRecord
	padding     [128]byte
}

func StartLTransaction(w *Worker) *LTransaction {
	tx := &LTransaction{
		keys:        make([]Rec, 0, 100),
		w:           w,
		s:           w.store,
		ls:          w.local_store,
		dummyRecord: &BRecord{},
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

func (tx *LTransaction) RelinquishKey(n uint64, r rune) {
	tx.w.GiveBack(n, r)
}

func (tx *LTransaction) Read(k Key) (*BRecord, error) {
	if exists, n := tx.already_exists(k); exists {
		if tx.keys[n].noset == true && tx.keys[n].br.exists {
			// MaybeWrite(); if the key exists.
			return tx.keys[n].br, nil
		}
		if tx.keys[n].noset == true && tx.keys[n].br.exists == false {
			// Doesn't really exist yet; created to lock for read or MaybeWrite()
			return nil, ENOKEY
		}
		if tx.keys[n].br.exists && tx.keys[n].noset == false && tx.keys[n].read == false {
			tx.dummyRecord.key_type = tx.keys[n].op
			tx.dummyRecord.int_value = tx.keys[n].vint32
			tx.dummyRecord.value = tx.keys[n].v
			dlog.Printf("Creating dummy record for key %v %v %v %v\n", k, tx.dummyRecord.key_type, tx.dummyRecord.int_value, tx.dummyRecord.value)
			if tx.keys[n].op == LIST {
				tx.dummyRecord.entries = tx.dummyRecord.entries[0 : len(tx.keys[n].br.entries)+1]
				copy(tx.dummyRecord.entries, tx.keys[n].br.entries)
				tx.dummyRecord.entries = append(tx.dummyRecord.entries, tx.keys[n].ve)
			}
			return tx.dummyRecord, nil
		}
		if tx.keys[n].br.exists && tx.keys[n].noset == false && tx.keys[n].read == true {
			return tx.keys[n].br, nil
		}
		dlog.Printf("Returning ENOKEY for key %v supposedly at slot %v. %v\n", k, n, tx.keys[n])
		return nil, ENOKEY
	}
	br, err := tx.s.getKey(k, tx.w.ld)
	if *CountKeys {
		p, r := UndoCKey(k)
		if r == 'm' {
			tx.w.NKeyAccesses[p]++
		}
	}
	n := len(tx.keys)
	tx.keys = tx.keys[0 : n+1]
	tx.keys[n].read = true
	tx.keys[n].noset = false
	tx.keys[n].key = k
	tx.keys[n].br = br
	if err == nil {
		br.SRLock()
		return br, nil
	}
	if br, err = tx.s.CreateMuLockedKey(k, WRITE); err == nil {
		tx.keys[n].noset = true
		tx.keys[n].read = false
		tx.keys[n].br = br
		br.exists = false
		return nil, ENOKEY
	}
	// Perhaps someone snuck in and created this key already.
	if br, err = tx.s.getKey(k, tx.w.ld); err == nil {
		br.SRLock()
		tx.keys[n].br = br
		return br, nil
	}
	log.Fatalf("Can't create key %v and it's not there now\n", k)
	return br, nil
}

// This is when I am reading a key and I might write it later; acquire
// the write lock *before* the read.
func (tx *LTransaction) MaybeWrite(k Key) {
	if exists, _ := tx.already_exists(k); exists {
		log.Fatalf("Shouldn't already have a lock on this\n")
	}
	br, err := tx.s.getKey(k, tx.w.ld)
	if *CountKeys {
		p, r := UndoCKey(k)
		if r == 'm' {
			tx.w.NKeyAccesses[p]++
		}
	}
	if br == nil || err != nil {
		if br, err = tx.s.CreateMuLockedKey(k, WRITE); err != nil {
			// Perhaps someone snuck in and created this key already.
			if br, err = tx.s.getKey(k, tx.w.ld); err != nil {
				log.Fatalf("Can't create key %v and it's not there now\n", k)
			}
			br.SLock()
		} // Created and Locked
		br.exists = false
	} else {
		br.SLock()
	}
	n := len(tx.keys)
	tx.keys = tx.keys[0 : n+1]
	tx.keys[n].br = br
	tx.keys[n].read = false
	tx.keys[n].noset = true
	tx.keys[n].key = k
}

func (tx *LTransaction) already_exists(k Key) (bool, int) {
	n := len(tx.keys)
	for i := 0; i < len(tx.keys); i++ {
		if tx.keys[i].key == k {
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
	br, err := tx.s.getKey(k, tx.w.ld)
	if *CountKeys {
		p, r := UndoCKey(k)
		if r == 'm' {
			tx.w.NKeyAccesses[p]++
		}
	}
	if br != nil && err == nil {
		br.SLock()
		return br
	}
	var err2 error
	br, err2 = tx.s.CreateMuLockedKey(k, op)
	if *CountKeys {
		p, r := UndoCKey(k)
		if r == 'm' {
			tx.w.NKeyAccesses[p]++
		}
	}
	if br == nil || err2 != nil {
		br, err = tx.s.getKey(k, tx.w.ld)
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
		tx.keys[n].vint32 = a
		tx.keys[n].op = op
		tx.keys[n].noset = false
		tx.keys[n].key = k
		return nil
	}
	br := tx.make_or_get_key(k, op)
	tx.keys = tx.keys[0 : n+1]
	tx.keys[n].br = br
	tx.keys[n].read = false
	tx.keys[n].vint32 = a
	tx.keys[n].op = op
	tx.keys[n].noset = false
	tx.keys[n].key = k
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
		tx.keys[n].op = op
		tx.keys[n].key = k
		return
	}
	br := tx.make_or_get_key(k, op)
	tx.keys = tx.keys[0 : n+1]
	tx.keys[n].br = br
	tx.keys[n].read = false
	tx.keys[n].v = v
	tx.keys[n].op = op
	tx.keys[n].noset = false
	tx.keys[n].key = k
}

func (tx *LTransaction) WriteList(k Key, l Entry, op KeyType) error {
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
		tx.keys[n].op = op
		tx.keys[n].key = k
		return nil
	}
	br := tx.make_or_get_key(k, op)
	tx.keys = tx.keys[0 : n+1]
	tx.keys[n].br = br
	tx.keys[n].read = false
	tx.keys[n].ve = l
	tx.keys[n].op = op
	tx.keys[n].noset = false
	tx.keys[n].key = k
	return nil
}

func (tx *LTransaction) WriteOO(k Key, a int32, v Value, op KeyType) error {
	if op != OOWRITE {
		log.Fatalf("Not overwrite \n")
	}
	exists, n := tx.already_exists(k)
	if exists {
		if tx.keys[n].read == true {
			log.Fatalf("Already have read lock on this key; cannot upgrade %v\n", k)
		}
		// Already locked.
		if a > tx.keys[n].vint32 {
			tx.keys[n].v = v
			tx.keys[n].vint32 = a
			tx.keys[n].op = op
			tx.keys[n].key = k
		}
		return nil
	}
	br := tx.make_or_get_key(k, op)
	tx.keys = tx.keys[0 : n+1]
	tx.keys[n].br = br
	tx.keys[n].read = false
	tx.keys[n].vint32 = a
	tx.keys[n].v = v
	tx.keys[n].op = op
	tx.keys[n].noset = false
	tx.keys[n].key = k
	return nil
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

func (tx *LTransaction) Worker() *Worker {
	return tx.w
}

func (tx *LTransaction) Abort() TID {
	for i := len(tx.keys) - 1; i >= 0; i-- {
		if tx.keys[i].read {
			tx.keys[i].br.SRUnlock()
		} else {
			tx.keys[i].br.SUnlock()
		}
	}
	return 0
}

func (tx *LTransaction) Commit() TID {
	tid := tx.w.commitTID()
	for i := len(tx.keys) - 1; i >= 0; i-- {
		// Apply and unlock
		if tx.keys[i].read == false {
			if tx.keys[i].noset {
				// No changes, we write-locked it because we thought
				// we *might* write
				tx.keys[i].br.SUnlock()
				continue
			}
			switch tx.keys[i].op {
			case SUM:
				tx.s.SetInt32(tx.keys[i].br, tx.keys[i].vint32, tx.keys[i].op)
			case MAX:
				tx.s.SetInt32(tx.keys[i].br, tx.keys[i].vint32, tx.keys[i].op)
			case LIST:
				tx.s.SetList(tx.keys[i].br, tx.keys[i].ve, tx.keys[i].op)
			case OOWRITE:
				tx.s.SetOO(tx.keys[i].br, tx.keys[i].vint32, tx.keys[i].v, tx.keys[i].op)
			default:
				tx.s.Set(tx.keys[i].br, tx.keys[i].v, tx.keys[i].op)
			}
			tx.keys[i].br.SUnlock()
		} else {
			//fmt.Printf("k: %v\n", tx.keys[i].br.key)
			tx.keys[i].br.SRUnlock()
		}
	}
	return tid
}

func (tx *LTransaction) NoCount() {
	// noop
}
