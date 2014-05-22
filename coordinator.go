package ddtxn

import (
	"ddtxn/dlog"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	BUMP_EPOCH_MS = 40
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
	wsafe  []chan bool
	wepoch []chan bool
	wgo    []chan bool
	wdone  []chan bool

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
		wepoch:     make([]chan bool, n),
		wsafe:      make([]chan bool, n),
		wgo:        make([]chan bool, n),
		wdone:      make([]chan bool, n),
		Done:       make(chan chan bool),
		Accelerate: make(chan bool),
	}
	for i := 0; i < n; i++ {
		c.wepoch[i] = make(chan bool)
		c.wsafe[i] = make(chan bool)
		c.wgo[i] = make(chan bool)
		c.wdone[i] = make(chan bool)
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

var RMoved int64
var WMoved int64
var Time_in_IE time.Duration

func (c *Coordinator) IncrementEpoch() {
	start := time.Now()
	c.NextGlobalTID()

	// Wait for everyone to merge the previous epoch
	for i := 0; i < c.n; i++ {
		<-c.wepoch[i]
	}

	if c.epochTID%(10*EPOCH_INCR) == 0 {
		s := c.Workers[0].store
		s.lock_candidates.Lock()
		for _, br := range s.candidates {
			br.dd = true
			WMoved += 1
			dlog.Printf("Moved %v to split\n", br.key)
		}
		for _, br := range s.rcandidates {
			br.dd = false
			RMoved += 1
			dlog.Printf("Moved %v to not split\n", br.key)
		}
		s.candidates = make(map[Key]*BRecord)
		s.rcandidates = make(map[Key]*BRecord)
		s.lock_candidates.Unlock()
	}

	// All merged.  The previous epoch is now safe; tell everyone to
	// do their reads.
	for i := 0; i < c.n; i++ {
		c.wsafe[i] <- true
	}
	for i := 0; i < c.n; i++ {
		<-c.wdone[i]
	}
	// Reads done!
	for i := 0; i < c.n; i++ {
		c.wgo[i] <- true
	}
	end := time.Since(start)
	Time_in_IE += end
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
