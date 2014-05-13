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
	sync.RWMutex
	rows map[Key]*BRecord
}

var (
	ENOKEY = errors.New("doppel: no key")
	EABORT = errors.New("doppel: abort")
	ESTASH = errors.New("doppel: stash")
)

const (
	CHUNKS = 256
)

// Global data
type Store struct {
	store           []*Chunk
	candidates      map[Key]*BRecord
	rcandidates     map[Key]*BRecord
	lock_candidates sync.Mutex
	NChunksAccessed []int64
}

func NewStore() *Store {
	s := &Store{
		store:           make([]*Chunk, CHUNKS),
		candidates:      make(map[Key]*BRecord),
		rcandidates:     make(map[Key]*BRecord),
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
	br, err := s.writeKey(k)
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
	br, err := s.writeKey(k)
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
		br.AddOneToList(v.(Entry))
	}
}

var UseRLocks = flag.Bool("rlock", true, "Use Rlocks\n")

func (s *Store) readKey(k Key) (*BRecord, error) {
	vr, err := s.getKey(k)
	if *SysType == DOPPEL && err == nil {
		if vr.dd && vr.stashed > RTHRESHOLD {
			s.lock_candidates.Lock()
			s.rcandidates[k] = vr
			vr.stashed = 0
			s.lock_candidates.Unlock()
		}
	}
	return vr, err
}

func (s *Store) writeKey(k Key) (*BRecord, error) {
	vr, err := s.getKey(k)
	if *SysType == DOPPEL && err == nil {
		if !vr.dd && vr.locked > THRESHOLD {
			s.lock_candidates.Lock()
			s.candidates[k] = vr
			vr.locked = 0
			s.lock_candidates.Unlock()
		}
	}
	return vr, err
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
	if !ok {
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
	if !ok {
		return vr, ENOKEY
	}
	// TODO: mark candidates
	return vr, nil
}
