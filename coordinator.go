package ddtxn

import (
	"container/heap"
	"ddtxn/dlog"
	"fmt"
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
	start := time.Now()
	c.NextGlobalTID()

	// Wait for everyone to merge the previous epoch
	for i := 0; i < c.n; i++ {
		<-c.wepoch[i]
		dlog.Printf("%v merged for %v\n", i, c.epochTID)
	}

	// All merged.  The previous epoch is now safe; tell everyone to
	// do their reads.
	for i := 0; i < c.n; i++ {
		c.wsafe[i] <- true
	}
	for i := 0; i < c.n; i++ {
		<-c.wdone[i]
		dlog.Printf("Got done from %v for %v\n", i, c.epochTID)
	}

	start2 := time.Now()
	// Reads done!  Check stats
	s := c.Workers[0].store
	if c.epochTID%(10*EPOCH_INCR) == 0 {
		for i := 0; i < c.n; i++ {
			w := c.Workers[i]
			s.cand.Merge(w.local_store.candidates)
		}
		for i := 0; i < len(*s.cand.h); i++ {
			o := heap.Pop(s.cand.h).(*OneStat)
			br, _ := s.getKey(o.k)
			if !br.dd {
				br.dd = true
				WMoved += 1
				x, y := UndoCKey(o.k)
				fmt.Printf("Moved %v %v to split\n", x, y)
				s.dd = append(s.dd, o.k)
			}
		}
		for i := 0; i < len(s.dd); i++ {
			if s.cand.m[s.dd[i]].ratio() < *WRRatio {
				br, _ := s.getKey(s.dd[i])
				br.dd = false
				RMoved += 1
				x, y := UndoCKey(br.key)
				fmt.Printf("Moved %v %v from split\n", x, y)
				s.dd[i], s.dd = s.dd[len(s.dd)-1], s.dd[:len(s.dd)-1]
			}
		}
	}
	end := time.Since(start2)
	Time_in_IE1 += end

	for i := 0; i < c.n; i++ {
		c.wgo[i] <- true
		dlog.Printf("Sent go to %v for %v\n", i, c.epochTID)
	}
	end = time.Since(start)
	Time_in_IE += end
}

func (c *Coordinator) Finish() {
	dlog.Printf("Coordinator finishing\n")
	x := make(chan bool)
	c.Done <- x
	<-x
}

func (c *Coordinator) Process() {
	tm := time.NewTicker(time.Duration(BUMP_EPOCH_MS) * time.Millisecond).C
	for {
		select {
		case x := <-c.Done:
			for i := 0; i < c.n; i++ {
				txn := Query{W: make(chan *Result)}
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
