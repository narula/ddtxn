package ddtxn

import (
	"errors"
	"flag"
	"log"
	"runtime/debug"
	"sync"
)

type TID uint64
type Key [16]byte
type Value interface{}

type Chunk struct {
	padding1 [128]byte
	sync.RWMutex
	rows     map[Key]*BRecord
	padding2 [128]byte
}

var UseRLocks = flag.Bool("rlock", true, "Use Rlocks\n")

var (
	ENOKEY  = errors.New("doppel: no key")
	EABORT  = errors.New("doppel: abort")
	ESTASH  = errors.New("doppel: stash")
	EEXISTS = errors.New("doppel: trying to create key which already exists")
)

const (
	CHUNKS = 256
)

// Global data
type Store struct {
	padding1        [128]byte
	store           []*Chunk
	NChunksAccessed []int64
	dd              map[Key]bool
	any_dd          bool
	cand            *Candidates
	padding2        [128]byte
}

func NewStore() *Store {
	x := make([]*OneStat, 0)
	sh := StatsHeap(x)
	s := &Store{
		store:           make([]*Chunk, CHUNKS),
		NChunksAccessed: make([]int64, CHUNKS),
		dd:              make(map[Key]bool),
		cand:            &Candidates{make(map[Key]*OneStat), &sh},
	}
	var bb byte

	for i := 0; i < CHUNKS; i++ {
		chunk := &Chunk{
			rows: make(map[Key]*BRecord),
		}
		bb = byte(i)
		s.store[bb] = chunk
	}
	return s
}

func (s *Store) getOrCreateTypedKey(k Key, v Value, kt KeyType) *BRecord {
	br, err := s.getKey(k)
	if err == ENOKEY {
		if !*UseRLocks {
			log.Fatalf("Should have preallocated keys if not locking chunks\n")
		}
		// Create key
		chunk := s.store[k[0]]
		var ok bool
		chunk.Lock()
		br, ok = chunk.rows[k]
		if !ok {
			br = MakeBR(k, v, kt)
			chunk.rows[k] = br
		}
		chunk.Unlock()
	}
	return br
}

func (s *Store) CreateKey(k Key, v Value, kt KeyType) *BRecord {
	chunk := s.store[k[0]]
	br := MakeBR(k, v, kt)
	chunk.Lock()
	chunk.rows[k] = br
	chunk.Unlock()
	return br
}

// The next two functions exist because we have to make sure the
// record is locked and inserted while holding the lock on the chunk.

func (s *Store) CreateLockedKey(k Key, kt KeyType) (*BRecord, error) {
	chunk := s.store[k[0]]
	br := MakeBR(k, nil, kt)
	br.Lock()
	chunk.Lock()
	_, ok := chunk.rows[k]
	if ok {
		return nil, EEXISTS
	}
	chunk.rows[k] = br
	chunk.Unlock()
	return br, nil
}

func (s *Store) CreateMuLockedKey(k Key, kt KeyType) (*BRecord, error) {
	chunk := s.store[k[0]]
	br := MakeBR(k, nil, kt)
	br.mu.Lock()
	chunk.Lock()
	_, ok := chunk.rows[k]
	if ok {
		return nil, EEXISTS
	}
	chunk.rows[k] = br
	chunk.Unlock()
	return br, nil
}

func (s *Store) SetInt32(br *BRecord, v int32, op KeyType) {
	switch op {
	case SUM:
		br.int_value += v
	case MAX:
		if v > br.int_value {
			br.int_value = v
		}
	}
}

func (s *Store) Set(br *BRecord, v Value, op KeyType) {
	switch op {
	case SUM:
		br.int_value += v.(int32)
	case MAX:
		x := v.(int32)
		if x > br.int_value {
			br.int_value = v.(int32)
		}
	case WRITE:
		br.value = v
	case LIST:
		if v != nil {
			br.AddOneToRecord(v.(Entry))
		}
	}
}

func (s *Store) Get(k Key) (*BRecord, error) {
	return s.getKey(k)
}

func (s *Store) getKey(k Key) (*BRecord, error) {
	if len(k) == 0 {
		debug.PrintStack()
		log.Fatalf("[store] getKey(): Empty key\n")
	}
	//s.NChunksAccessed[k[0]]++
	if !*UseRLocks {
		x, err := s.getKeyStatic(k)
		return x, err
	}
	chunk := s.store[k[0]]
	if chunk == nil {
		log.Fatalf("[store] Didn't initialize chunk for key %v byte %v\n", k, k[0])
	}
	chunk.RLock()
	vr, ok := chunk.rows[k]
	if !ok || vr == nil {
		chunk.RUnlock()
		return vr, ENOKEY
	}
	chunk.RUnlock()
	return vr, nil
}

func (s *Store) getKeyStatic(k Key) (*BRecord, error) {
	if len(k) == 0 {
		log.Fatalf("[store] getKey(): Empty key\n")
	}
	chunk := s.store[k[0]]
	if chunk == nil {
		log.Fatalf("[store] Didn't initialize chunk for key %v byte %v\n", k, k[0])
	}
	vr, ok := chunk.rows[k]
	if !ok || vr == nil {
		return vr, ENOKEY
	}
	return vr, nil
}

func (s *Store) IsDD(k Key) bool {
	if !s.any_dd {
		return false
	}
	x, ok := s.dd[k]
	return x && ok
}
