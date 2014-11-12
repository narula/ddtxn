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

var PhaseLength = flag.Int("phase", 20, "Phase length in milliseconds, default 20")
var SpinPhase = flag.Bool("spin", false, "How to do phase transitions; spin or channels")

type Coordinator struct {
	n        int
	Workers  []*Worker
	epochTID uint64 // Global TID, atomically incremented and read

	padding [128]byte
	// Notify workers
	wepoch []chan TID
	wsafe  []chan TID
	wgo    []chan TID
	wdone  []chan TID

	// Used in spin-based phase transitions
	wcepoch  uint64 // Count of workers who have seen epoch change AND merged
	gojoin   uint64 // 0 or 1; Tells workers safe to progress to JOIN phase
	wcdone   uint64 // Count of workers who have finished JOIN phase
	gosplit  uint64 // 0 or 1; Tells workers safe to progress to SPLIT phase
	padding1 [128]byte

	Coordinate            bool
	PotentialPhaseChanges int64
	Done                  chan chan bool
	Accelerate            chan bool
	trigger               int32
	to_remove             map[Key]bool

	StartTime      time.Time
	Finished       []bool
	TotalCoordTime time.Duration
	GoTime         time.Duration
	ReadTime       time.Duration
	MergeTime      time.Duration
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
		Finished:              make([]bool, n),
	}
	for i := 0; i < n; i++ {
		c.wepoch[i] = make(chan TID)
		c.wsafe[i] = make(chan TID)
		c.wgo[i] = make(chan TID)
		c.wdone[i] = make(chan TID)
		c.Finished[i] = false
		c.Workers[i] = NewWorker(i, s, c)
	}
	c.Finished = make([]bool, n)
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
	for i := 0; i < len(c.Workers); i++ {
		if c.Finished[i] {
			dlog.Printf("COORD not computing stats, worker %v finished\n", i)
			return nil, nil
		}
	}
	if c.PotentialPhaseChanges%(10) != 0 {
		return nil, nil
	}
	start2 := time.Now()
	s := c.Workers[0].store
	for i := 0; i < len(c.Workers); i++ {
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
			if !s.any_dd {
				// Higher threshold for the first one, since it kicks off phases
				if o.ratio() > 1.33*(*WRRatio) && (o.writes > 1 || o.conflicts > 5) {
					potential_dd_keys[o.k] = true
					//dlog.Printf("move %v to split1 r:%v w:%v c:%v s:%v ra:%v after: %v\n", o.k, o.reads, o.writes, o.conflicts, o.stash, o.ratio(), c.PotentialPhaseChanges)
					s.any_dd = true
				} else {
					//dlog.Printf("%v no move inertia r:%v w:%v c:%v s:%v ra:%v after: %v\n", o.k, o.reads, o.writes, o.conflicts, o.stash, o.ratio(), c.PotentialPhaseChanges)
				}
				continue
			}
			if o.ratio() > *WRRatio && (o.writes > 1 || o.conflicts > 1) {
				potential_dd_keys[o.k] = true
				//dlog.Printf("move %v to split2 r:%v w:%v c:%v s:%v ra:%v after: %v\n", o.k, o.reads, o.writes, o.conflicts, o.stash, o.ratio(), c.PotentialPhaseChanges)
				s.any_dd = true
			} else {
				//dlog.Printf("too low; no move :%v; r:%v w:%v c:%v s:%v ra:%v; wr: %v\n", o.k, o.reads, o.writes, o.conflicts, o.stash, o.ratio(), *WRRatio)
			}
		}
	}
	// Check to see if we need to remove anything from dd
	for k, v := range s.dd {
		if !v {
			continue
		}
		o, ok := s.cand.m[k]
		if !ok {
			//dlog.Printf("Key %v was split but now is not in store candidates\n", k)
			if x, ok := c.to_remove[k]; x && ok {
				c.to_remove[k] = false
				to_remove[k] = true
			} else {
				c.to_remove[k] = true
			}
			//dlog.Printf("move %v from split2 \n", k)
			continue
		}
		if o.ratio() < (*WRRatio)/2 {
			if x, ok := c.to_remove[k]; x && ok {
				c.to_remove[k] = false
				to_remove[k] = true
			} else {
				c.to_remove[k] = true
			}
			//dlog.Printf("move %v from split r:%v w:%v c:%v s:%v ratio:%v\n", k, o.reads, o.writes, o.conflicts, o.stash, o.ratio())
		}
	}
	if len(s.dd) == 0 && len(potential_dd_keys) == 0 {
		if c.Coordinate {
			fmt.Printf("Do not have to coordinate! after %v phases\n", c.PotentialPhaseChanges)
		}
		s.any_dd = false
		c.Coordinate = false
	} else {
		if !c.Coordinate {
			fmt.Printf("Have to coordinate after %v phases\n", c.PotentialPhaseChanges)
		}
		c.Coordinate = true
		s.any_dd = true
	}
	// Reset global store
	x := make([]*OneStat, 0)
	sh := StatsHeap(x)
	s.cand = &Candidates{make(map[Key]*OneStat), &sh}

	for i := 0; i < len(c.Workers); i++ {
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

func (c *Coordinator) SpinPhaseTransition(force bool) {
	start1 := time.Now()
	c.PotentialPhaseChanges++
	s := c.Workers[0].store
	var move_dd, remove_dd map[Key]bool
	if *AlwaysSplit {
		c.Coordinate = true
		s.any_dd = true
	} else {
		move_dd, remove_dd = c.Stats()
	}
	if !c.Coordinate && !force {
		c.TotalCoordTime += time.Since(start1)
		return
	}
	xx := time.Since(c.StartTime)
	_ = xx

	c.StartTime = time.Now()
	next_epoch := c.NextGlobalTID()
	_ = next_epoch
	//dlog.Printf("%v COORD changed to epoch %v; waiting for MERGE. c.n=%v; time since last start: %v\n", time.Now().UnixNano(), next_epoch, c.n, xx)

	x := atomic.LoadUint64(&c.wcepoch)
	for x < uint64(c.n) {
		g := 0
		for i := 0; i < len(c.Workers); i++ {
			if !c.Finished[i] {
				g++
			}
		}
		if g != c.n {
			c.n = g
		}
		x = atomic.LoadUint64(&c.wcepoch)
	}
	// All workers have merged, meaning all workers passed the last epoch and saw gosplit = 1
	atomic.StoreUint64(&c.gosplit, 0)
	atomic.StoreUint64(&c.wcdone, 0) // Workers won't start reading/writing this till c.gojoin is 1
	c.MergeTime += time.Since(c.StartTime)

	// All merged.  The previous epoch is now safe; tell everyone to
	// do their reads.
	//dlog.Printf("%v COORD all workers MERGED epoch %v; took %v, saying go for JOIN\n", time.Now().UnixNano(), next_epoch, time.Since(c.StartTime))
	sx := time.Now()
	atomic.StoreInt32(&c.trigger, 0)
	// Go for JOIN phase
	atomic.StoreUint64(&c.gojoin, 1) // After this workers will read/write c.wcdone

	//dlog.Printf("%v %v COORD STARTING TO WAIT for wdone\n", time.Now().UnixNano(), next_epoch)
	x = atomic.LoadUint64(&c.wcdone)
	for x != uint64(c.n) {
		g := 0
		for i := 0; i < len(c.Workers); i++ {
			if !c.Finished[i] {
				g++
			}
		}
		if g != c.n {
			c.n = g
		}
		x = atomic.LoadUint64(&c.wcdone)
	}
	atomic.StoreUint64(&c.gojoin, 0) // All workers saw gojoin and did wcdone
	//dlog.Printf("COORD all workers did JOIN for epoch %v; took %v, changing DD\n", next_epoch, time.Now())
	c.ReadTime += time.Since(sx)
	sx = time.Now()
	// Merge dd
	if !*AlwaysSplit {
		if move_dd != nil {
			for k, _ := range move_dd {
				br, _ := s.getKey(k)
				//dlog.Printf("COORD setting %v to dd for epoch %v\n", br.key, next_epoch)
				br.dd = true
				s.dd[k] = true
				WMoved += 1
			}
		}

		if remove_dd != nil {
			for k, _ := range remove_dd {
				br, _ := s.getKey(k)
				//dlog.Printf("COORD removing %v from dd for epoch %v\n", br.key, next_epoch)
				br.dd = false
				s.dd[k] = false
				RMoved += 1
			}
		}
	}
	atomic.StoreUint64(&c.gosplit, 1)
	c.GoTime += time.Since(sx)
	//dlog.Printf("%v COORD done with %v; took, Saying go for SPLIT\n", time.Now().UnixNano(), next_epoch)
	atomic.StoreUint64(&c.wcepoch, 0) // Safe?
	c.TotalCoordTime += time.Since(start1)
}

func (c *Coordinator) IncrementEpoch(force bool) {
	if *SpinPhase {
		c.SpinPhaseTransition(force)
		return
	}
	start1 := time.Now()
	c.PotentialPhaseChanges++
	s := c.Workers[0].store
	var move_dd, remove_dd map[Key]bool
	if *AlwaysSplit {
		c.Coordinate = true
		s.any_dd = true
	} else {
		move_dd, remove_dd = c.Stats()
	}
	if !c.Coordinate && !force {
		c.TotalCoordTime += time.Since(start1)
		return
	}
	c.StartTime = time.Now()
	next_epoch := c.NextGlobalTID()

	// Wait for everyone to merge the previous epoch
	for i := 0; i < c.n; i++ {
		e := <-c.wepoch[i]
		if e != next_epoch {
			log.Fatalf("Out of alignment in epoch ack; I expected %v, got %v\n", next_epoch, e)
		}
	}
	c.MergeTime += time.Since(c.StartTime)

	// All merged.  The previous epoch is now safe; tell everyone to
	// do their reads.
	sx := time.Now()
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
	c.ReadTime += time.Since(sx)
	// Merge dd
	if !*AlwaysSplit {
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
	}

	sx = time.Now()
	for i := 0; i < c.n; i++ {
		c.wgo[i] <- next_epoch
	}
	c.GoTime += time.Since(sx)
	c.TotalCoordTime += time.Since(start1)
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
			if *SysType == DOPPEL && c.n > 1 && c.Workers[0].store.any_dd {
				c.IncrementEpoch(true)
			}
			if !*SpinPhase {
				for i := 0; i < c.n; i++ {
					c.Workers[i].done <- true
				}
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
	total = total + w.tooLong[txn]
	sum = sum + w.tooLong[txn]*10000000
	var x99 int64 = int64(float64(total) * .99)
	var y99 int64
	var v99 int64
	var buckets [TIMES / 1000]int64
	for i = 0; i < TIMES; i++ {
		buckets[i/1000] += w.times[txn][i]
		y99 = y99 + w.times[txn][i]
		if y99 >= x99 {
			v99 = i
			break
		}
	}
	if total == 0 {
		log.Fatalf("No latency recorded\n")
	}
	dlog.Printf("%v avg: %v us; 99: %v us, x99: %v, sum: %v, total: %v \n", txn, sum/total, v99, x99, sum, total)

	var one int64
	var ten int64
	var hundred int64
	var more int64
	for i = 0; i < TIMES/1000; i++ {
		if i == 0 {
			one += buckets[i]
		} else if i < 10 {
			ten += buckets[i]
		} else if i < 100 {
			hundred += buckets[i]
		} else {
			more += buckets[i]
		}
	}

	fmt.Printf("Txn %v\n Less than 1ms: %v\n 1-10ms: %v\n 10-100ms: %v\n 100ms-10s: %v\n Greater than 10s: %v\n", txn, one, ten, hundred, more, w.tooLong[txn])
	total_time_in_ms := (one/2 + 5*ten + 55*100 + 15000*more)
	fmt.Printf("Rough total time in ms: %v\n", total_time_in_ms)
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
			c.Workers[0].tooLong[j] = c.Workers[0].tooLong[j] + c.Workers[i].tooLong[j]
		}
	}
	x, y := compute(c.Workers[0], D_BUY)
	x2, y2 := compute(c.Workers[0], D_READ_TWO)
	return fmt.Sprintf("Read Avg: %v\nWrite Avg: %v\n", x2, x), fmt.Sprintf("Read 99: %v\nWrite 99: %v\n", y2, y)
}
