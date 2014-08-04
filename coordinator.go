package ddtxn

import (
	"container/heap"
	"ddtxn/dlog"
	"flag"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	BUMP_EPOCH_MS = 80
	EPOCH_INCR    = 1 << 32
	TXID_MASK     = 0x00000000ffffffff
	CLEAR_TID     = 0xffffffff00000000
)

var PhaseLength = flag.Int("phase", 80, "Phase length in milliseconds, default 80")

type Coordinator struct {
	n        int
	Workers  []*Worker
	epochTID TID // Global TID, atomically incremented and read

	// Notify workers
	wsafe  []chan bool
	wepoch []chan bool
	wgo    []chan bool
	wdone  []chan bool

	Done       chan chan bool
	Accelerate chan bool
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
var Time_in_IE1 time.Duration

func (c *Coordinator) IncrementEpoch() {
	dlog.Printf("Incrementing epoch %v\n", c.epochTID)
	start := time.Now()
	c.NextGlobalTID()

	// Wait for everyone to merge the previous epoch
	for i := 0; i < c.n; i++ {
		<-c.wepoch[i]
	}

	// All merged.  The previous epoch is now safe; tell everyone to
	// do their reads.
	for i := 0; i < c.n; i++ {
		c.wsafe[i] <- true
	}
	for i := 0; i < c.n; i++ {
		<-c.wdone[i]
	}

	// Reads done!  Check stats
	s := c.Workers[0].store
	if c.epochTID%(10*EPOCH_INCR) == 0 {
		start2 := time.Now()
		for i := 0; i < c.n; i++ {
			w := c.Workers[i]
			s.cand.Merge(w.local_store.candidates)
		}
		xx := len(*s.cand.h)
		for i := 0; i < xx; i++ {
			o := heap.Pop(s.cand.h).(*OneStat)
			x, y := UndoCKey(o.k)
			dlog.Printf("%v Considering key %v %v; ratio %v\n", i, x, y, o.ratio())
			br, _ := s.getKey(o.k)
			if !br.dd {
				br.dd = true
				WMoved += 1
				dlog.Printf("Moved %v %v to split %v\n", x, y, o.ratio())
				s.dd[o.k] = true
				s.any_dd = true
			} else {
				dlog.Printf("No need to Move %v %v to split; already dd\n", x, y)
			}
		}
		// Check to see if we need to remove anything from dd
		for k, v := range s.dd {
			if !v {
				continue
			}
			o, ok := s.cand.m[k]
			if !ok {
				br, _ := s.getKey(k)
				x, y := UndoCKey(br.key)
				dlog.Printf("Key %v %v was split but now is not in store candidates\n", x, y)
				continue
			}
			if o.ratio() < (*WRRatio)/2 {
				br, _ := s.getKey(k)
				br.dd = false
				RMoved += 1
				x, y := UndoCKey(o.k)
				dlog.Printf("Moved %v %v from split ratio %v\n", x, y, o.ratio())
				s.dd[k] = false
				//s.dd[i], s.dd = s.dd[len(s.dd)-1], s.dd[:len(s.dd)-1]
			}
		}
		if len(s.dd) == 0 {
			s.any_dd = false
		}
		for i := 0; i < c.n; i++ {
			// Reset local stores
			w := c.Workers[i]
			x := make([]*OneStat, 0)
			sh := StatsHeap(x)
			w.local_store.candidates = &Candidates{make(map[Key]*OneStat), &sh}
		}
		// Reset global store
		x := make([]*OneStat, 0)
		sh := StatsHeap(x)
		s.cand = &Candidates{make(map[Key]*OneStat), &sh}
		end := time.Since(start2)
		Time_in_IE1 += end
	}
	for i := 0; i < c.n; i++ {
		c.wgo[i] <- true
		//dlog.Printf("Sent go to %v for %v\n", i, c.epochTID)
	}
	end := time.Since(start)
	Time_in_IE += end
}

func (c *Coordinator) Finish() {
	dlog.Printf("Coordinator finishing\n")
	if *SysType == DOPPEL {
		c.IncrementEpoch()
	}
}

func (c *Coordinator) Process() {
	tm := time.NewTicker(time.Duration(*PhaseLength) * time.Millisecond).C
	for {
		select {
		case x := <-c.Done:
			for i := 0; i < c.n; i++ {
				txn := Query{W: make(chan struct {
					R *Result
					E error
				})}
				c.Workers[i].done <- txn
				<-txn.W
				dlog.Printf("Worker %v finished\n", i)
			}
			x <- true
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
