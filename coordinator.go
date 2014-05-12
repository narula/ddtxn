package ddtxn

import (
	"ddtxn/dlog"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	BUMP_EPOCH_MS = 20
	EPOCH_INCR    = 1 << 32
	TXID_MASK     = 0x00000000ffffffff
	CLEAR_TID     = 0xffffffff00000000
)

type Coordinator struct {
	n        int
	Workers  []*Worker
	epochTID TID // Global TID, atomically incremented and read
	safeTID  TID // Guaranteed no more writes at or before this point

	// Notify workers
	wsafe  []chan Msg
	wepoch []chan Msg
	wgo    []chan bool

	Done       chan chan bool
	Accelerate chan bool
}

type Msg struct {
	T TID
	C chan bool
}

func NewCoordinator(n int, s *Store) *Coordinator {
	c := &Coordinator{
		n:          n,
		Workers:    make([]*Worker, n),
		epochTID:   EPOCH_INCR,
		wepoch:     make([]chan Msg, n),
		wsafe:      make([]chan Msg, n),
		wgo:        make([]chan bool, n),
		Done:       make(chan chan bool),
		Accelerate: make(chan bool),
	}
	for i := 0; i < n; i++ {
		c.wepoch[i] = make(chan Msg)
		c.wsafe[i] = make(chan Msg)
		c.wgo[i] = make(chan bool)
		c.Workers[i] = NewWorker(i, s, c)
	}
	dlog.Printf("[coordinator] %v workers\n", n)
	go c.Process()
	return c
}

var NextEpoch int64

func (c *Coordinator) NextGlobalTID() TID {
	NextEpoch++
	x := atomic.AddUint64((*uint64)(unsafe.Pointer(&c.epochTID)), EPOCH_INCR)
	return TID(x)
}

func (c *Coordinator) GetEpoch() TID {
	x := atomic.LoadUint64((*uint64)(unsafe.Pointer(&c.epochTID)))
	return TID(x)
}

var Moved int64

func (c *Coordinator) IncrementEpoch() {
	e := c.NextGlobalTID()

	// Tell everyone about the new epoch, wait for them to
	// merge the previous epoch
	m := make(map[int]Msg)
	for i := 0; i < c.n; i++ {
		m[i] = Msg{e, make(chan bool)}
		c.wepoch[i] <- m[i]
	}
	for i := 0; i < c.n; i++ {
		<-m[i].C
	}
	if *SysType == DOPPEL {
		// Find out if anything should be added to DD
		keys := make(map[Key]bool)
		s := c.Workers[0].store
		s.lock_candidates.Lock()
		for k, _ := range s.candidates {
			keys[k] = true
		}
		s.candidates = make(map[Key]bool)
		s.lock_candidates.Unlock()
		Moved += int64(len(keys))
		for _, w := range c.Workers {
			w.AddDD(keys)
		}
	}

	// All merged.  The previous epoch is now safe; tell everyone to
	// do their reads.
	c.safeTID = e - EPOCH_INCR
	m = make(map[int]Msg)
	for i := 0; i < c.n; i++ {
		m[i] = Msg{c.safeTID, make(chan bool)}
		c.wsafe[i] <- m[i]
	}
	for i := 0; i < c.n; i++ {
		<-m[i].C
	}
	// Reads done!  Now we can do the next
	for i := 0; i < c.n; i++ {
		c.wgo[i] <- true
	}
}

func (c *Coordinator) Finish() {
	x := make(chan bool)
	c.Done <- x
	<-x
}

func (c *Coordinator) Process() {
	tm := time.NewTicker(time.Duration(BUMP_EPOCH_MS) * time.Millisecond).C
	for {
		select {
		case d := <-c.Done:
			for i := 0; i < c.n; i++ {
				txn := Query{TXN: LAST_TXN, W: make(chan *Result)}
				dlog.Printf("Finishing, waiting on %d\n", i)
				c.Workers[i].Incoming <- txn
				<-txn.W
				dlog.Printf("Worker %d finished.\n", i)
			}
			d <- true
			return
		case <-tm:
			if *SysType == DOPPEL {
				c.IncrementEpoch()
			}
		case <-c.Accelerate:
			if *SysType == DOPPEL {
				dlog.Printf("Accelerating\n")
				c.IncrementEpoch()
			}
		}
	}
}
