package ddtxn

import (
	"container/heap"
	"ddtxn/dlog"
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"
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
	epochTID uint64 // Global TID, atomically incremented and read

	// Notify workers
	wepoch []chan TID
	wsafe  []chan TID
	wgo    []chan TID
	wdone  []chan TID

	Coordinate            bool
	PotentialPhaseChanges int64
	Done                  chan chan bool
	Accelerate            chan bool
	trigger               int32
	to_remove             map[Key]bool
}

func NewCoordinator(n int, s *Store) *Coordinator {
	c := &Coordinator{
		n:                     n,
		Workers:               make([]*Worker, n),
		epochTID:              EPOCH_INCR,
		wepoch:                make([]chan TID, n),
		wsafe:                 make([]chan TID, n),
		wgo:                   make([]chan TID, n),
		wdone:                 make([]chan TID, n),
		Done:                  make(chan chan bool),
		Accelerate:            make(chan bool),
		Coordinate:            false,
		PotentialPhaseChanges: 0,
		to_remove:             make(map[Key]bool),
	}
	for i := 0; i < n; i++ {
		c.wepoch[i] = make(chan TID)
		c.wsafe[i] = make(chan TID)
		c.wgo[i] = make(chan TID)
		c.wdone[i] = make(chan TID)
		c.Workers[i] = NewWorker(i, s, c)
	}
	dlog.Printf("[coordinator] %v workers\n", n)
	go c.Process()
	return c
}

var NextEpoch int64

func (c *Coordinator) NextGlobalTID() TID {
	atomic.AddInt64(&NextEpoch, 1)
	x := atomic.AddUint64(&c.epochTID, EPOCH_INCR)
	return TID(x)
}

func (c *Coordinator) GetEpoch() TID {
	x := atomic.LoadUint64(&c.epochTID)
	return TID(x)
}

var RMoved int64
var WMoved int64
var Time_in_IE time.Duration
var Time_in_IE1 time.Duration

func (c *Coordinator) Stats() (map[Key]bool, map[Key]bool) {
	if c.PotentialPhaseChanges%(10) != 0 {
		return nil, nil
	}
	start2 := time.Now()
	s := c.Workers[0].store
	for i := 0; i < c.n; i++ {
		w := c.Workers[i]
		c.Workers[i].Lock()
		s.cand.Merge(w.local_store.candidates)
	}
	potential_dd_keys := make(map[Key]bool)
	to_remove := make(map[Key]bool)
	xx := len(*s.cand.h)
	for i := 0; i < xx; i++ {
		o := heap.Pop(s.cand.h).(*OneStat)
		br, _ := s.getKey(o.k)
		if !br.dd {
			if o.ratio() > *WRRatio && (o.writes > 1 || o.conflicts > 1) {
				if len(s.dd) == 0 { // Higher threshold for the first one, since it kicks off phases
					if o.ratio() > 1.33*(*WRRatio) && (o.writes > 1 || o.conflicts > 2) {
						potential_dd_keys[o.k] = true
						dlog.Printf("Moving %v to split r:%v w:%v c:%v s:%v ratio:%v\n", o.k, o.reads, o.writes, o.conflicts, o.stash, o.ratio())
					} else {
						dlog.Printf("Key %v didn't pass higher ratio threshold; len(s.dd)==0  r:%v w:%v c:%v s:%v ratio:%v\n", o.k, o.reads, o.writes, o.conflicts, o.stash, o.ratio())
					}
				} else {
					potential_dd_keys[o.k] = true
					dlog.Printf("Moving %v to split r:%v w:%v c:%v s:%v ratio:%v\n", o.k, o.reads, o.writes, o.conflicts, o.stash, o.ratio())
				}
			} else {
				dlog.Printf("Not enough writes or conflicts or high enough ratio yet for key : %v; r:%v w:%v c:%v s:%v ratio:%v; wr: %v\n", o.k, o.reads, o.writes, o.conflicts, o.stash, o.ratio(), *WRRatio)
			}
		} else if br.dd {
			// Key is split; might potentially move back
		}
	}
	// Check to see if we need to remove anything from dd
	for k, v := range s.dd {
		if !v {
			continue
		}
		o, ok := s.cand.m[k]
		if !ok {
			dlog.Printf("Key %v was split but now is not in store candidates\n", k)
			continue
		}
		if o.ratio() < (*WRRatio)/2 {
			if x, ok := c.to_remove[k]; x && ok {
				c.to_remove[k] = false
				to_remove[k] = true
			} else {
				c.to_remove[k] = true
			}
			dlog.Printf("Moved %v from split r:%v w:%v c:%v s:%v ratio:%v\n", k, o.reads, o.writes, o.conflicts, o.stash, o.ratio())
		}
	}
	if len(s.dd) == 0 && len(potential_dd_keys) == 0 {
		s.any_dd = false
		c.Coordinate = false
	} else {
		c.Coordinate = true
		s.any_dd = true
	}
	// Reset global store
	x := make([]*OneStat, 0)
	sh := StatsHeap(x)
	s.cand = &Candidates{make(map[Key]*OneStat), &sh}

	for i := 0; i < c.n; i++ {
		// Reset local stores and unlock
		w := c.Workers[i]
		x := make([]*OneStat, 0)
		sh := StatsHeap(x)
		w.local_store.candidates = &Candidates{make(map[Key]*OneStat), &sh}
		w.Unlock()
	}
	end := time.Since(start2)
	Time_in_IE1 += end
	return potential_dd_keys, to_remove
}

func (c *Coordinator) IncrementEpoch(force bool) {
	c.PotentialPhaseChanges++
	move_dd, remove_dd := c.Stats()
	if !c.Coordinate && !force {
		return
	}
	start := time.Now()
	next_epoch := c.NextGlobalTID()

	// Wait for everyone to merge the previous epoch
	for i := 0; i < c.n; i++ {
		e := <-c.wepoch[i]
		if e != next_epoch {
			log.Fatalf("Out of alignment in epoch ack; I expected %v, got %v\n", next_epoch, e)
		}
	}

	// All merged.  The previous epoch is now safe; tell everyone to
	// do their reads.
	atomic.StoreInt32(&c.trigger, 0)
	for i := 0; i < c.n; i++ {
		c.wsafe[i] <- next_epoch
	}
	for i := 0; i < c.n; i++ {
		e := <-c.wdone[i]
		if e != next_epoch {
			log.Fatalf("Out of alignment in done; I expected %v, got %v\n", next_epoch, e)
		}

	}

	s := c.Workers[0].store
	// Merge dd
	if move_dd != nil {
		for k, _ := range move_dd {
			br, _ := s.getKey(k)
			br.dd = true
			s.dd[k] = true
			WMoved += 1
		}
	}
	if remove_dd != nil {
		for k, _ := range remove_dd {
			br, _ := s.getKey(k)
			br.dd = false
			s.dd[k] = false
			RMoved += 1
		}
	}

	for i := 0; i < c.n; i++ {
		c.wgo[i] <- next_epoch
	}
	end := time.Since(start)
	Time_in_IE += end
}

func (c *Coordinator) Finish() {
	dlog.Printf("Coordinator finishing\n")
	x := make(chan bool)
	c.Done <- x
	<-x
}

var Nfast int64

func (c *Coordinator) Process() {
	tm := time.NewTicker(time.Duration(*PhaseLength) * time.Millisecond).C

	// More frequently, check if the workers are demanding a phase
	// change due to long stashed queue lengths.
	check_trigger := time.NewTicker(time.Duration(*PhaseLength) * time.Microsecond * 10).C

	for {
		select {
		case x := <-c.Done:
			if *SysType == DOPPEL && c.n > 1 {
				c.IncrementEpoch(true)
			}
			for i := 0; i < c.n; i++ {
				c.Workers[i].done <- true
			}
			x <- true
			return
		case <-tm:
			if *SysType == DOPPEL && c.n > 1 {
				c.IncrementEpoch(false)
			}
		case <-check_trigger:
			if *SysType == DOPPEL && c.n > 1 {
				x := atomic.LoadInt32(&c.trigger)
				if x == int32(c.n) {
					Nfast++
					atomic.StoreInt32(&c.trigger, 0)
					c.IncrementEpoch(true)
				}
			}
		case <-c.Accelerate:
			if *SysType == DOPPEL && c.n > 1 {
				dlog.Printf("Accelerating\n")
				c.IncrementEpoch(true)
			}
		}
	}
}

func compute(w *Worker, txn int) (int64, int64) {
	var total int64
	var sum int64
	var i int64
	for i = 0; i < TIMES; i++ {
		total = total + w.times[txn][i]
		sum = sum + (w.times[txn][i] * i)
	}
	var x99 int64 = int64(float64(total) * .99)
	var y99 int64
	var v99 int64
	for i = 0; i < TIMES; i++ {
		y99 = y99 + w.times[txn][i]
		if y99 >= x99 {
			v99 = i
			break
		}
	}
	fmt.Printf("%v avg: %v us; 99: %v us, x99: %v, sum: %v, total: %v \n", txn, sum/total, v99, x99, sum, total)
	return sum / total, v99
}

func (c *Coordinator) Latency() (string, string) {
	if !*Latency {
		return "", ""
	}
	for i := 1; i < c.n; i++ {
		for j := 0; j < 4; j++ {
			for k := 0; k < TIMES; k++ {
				c.Workers[0].times[j][k] = c.Workers[0].times[j][k] + c.Workers[i].times[j][k]
			}
		}
	}
	x, y := compute(c.Workers[0], D_BUY)
	x2, y2 := compute(c.Workers[0], D_READ_TWO)
	return fmt.Sprintf("Read 99: %v\nRead Avg: %v\n", y2, x2), fmt.Sprintf("Write 99: %v\n Write Avg: %v\n", y, x)

}
