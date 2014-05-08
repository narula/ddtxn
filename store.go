package ddtxn

import (
	"ddtxn/dlog"
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
	sync.RWMutex
	rows map[Key]*BRecord
}

var (
	ENOKEY = errors.New("doppel: no key")
	EABORT = errors.New("doppel: abort")
)

const (
	CHUNKS = 256
)

// Global data
type Store struct {
	store           []*Chunk
	candidates      map[Key]bool
	lock_candidates sync.Mutex
	NChunksAccessed []int64
}

func NewStore() *Store {
	s := &Store{
		store:           make([]*Chunk, CHUNKS),
		candidates:      make(map[Key]bool),
		NChunksAccessed: make([]int64, CHUNKS),
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

func (s *Store) getOrCreateKey(k Key) *BRecord {
	br, err := s.getKey(k)
	if err == ENOKEY {
		// Create key
		chunk := s.store[k[0]]
		var ok bool
		chunk.Lock()
		br, ok = chunk.rows[k]
		if !ok {
			br = MakeBR(k, nil, WRITE)
			chunk.rows[k] = br
		}
		chunk.Unlock()
	}
	return br
}

func (s *Store) getOrCreateTypedKey(k Key, v Value, kt KeyType) *BRecord {
	br, err := s.getKey(k)
	if err == ENOKEY {
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

var UseRLocks = flag.Bool("rlock", true, "Use Rlocks\n")

func (s *Store) getKey(k Key) (*BRecord, error) {
	if len(k) == 0 {
		debug.PrintStack()
		log.Fatalf("[store] getKey(): Empty key\n")
	}
	if !*UseRLocks {
		x, err := s.getKeyStatic(k)
		return x, err
	}
	dlog.Printf("key %v\n", k)
	chunk := s.store[k[0]]
	if chunk == nil {
		log.Fatalf("[store] Didn't initialize chunk for key %v byte %v\n", k, k[0])
	}
	chunk.RLock()
	vr, ok := chunk.rows[k]
	if !ok {
		chunk.RUnlock()
		return vr, ENOKEY
	}
	chunk.RUnlock()
	if *Dynamic && *SysType == DOPPEL {
		if vr.locked > THRESHOLD {
			s.lock_candidates.Lock()
			s.candidates[k] = true
			vr.locked = 0
			s.lock_candidates.Unlock()
		}
	}
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
	if !ok {
		return vr, ENOKEY
	}
	if *Dynamic && *SysType == DOPPEL {
		if vr.locked > THRESHOLD {
			s.lock_candidates.Lock()
			s.candidates[k] = true
			vr.locked = 0
			s.lock_candidates.Unlock()
		}
	}
	return vr, nil
}
