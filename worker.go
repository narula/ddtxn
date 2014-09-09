package ddtxn

import (
	"flag"
	"fmt"
	"log"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DOPPEL = iota
	OCC
	LOCKING
)

var SysType = flag.Int("sys", DOPPEL, "Type of system to run\n")
var CountKeys = flag.Bool("ck", false, "Count keys accessed")
var Latency = flag.Bool("latency", false, "Measure latency")

type TransactionFunc func(Query, ETransaction) (*Result, error)

const (
	BUFFER     = 100000
	START_SIZE = 1000000
	TIMES      = 10
)

const (
	D_BUY = iota
	D_BUY_AND_READ
	D_READ_ONE
	D_READ_TWO
	D_INCR_ONE
	D_ATOMIC_INCR_ONE

	RUBIS_BID         // 6   7%
	RUBIS_VIEWBIDHIST // 7   3%
	RUBIS_BUYNOW      // 8   3%
	RUBIS_COMMENT     // 9   1%
	RUBIS_NEWITEM     // 10  4%
	RUBIS_PUTBID      // 11  10%
	RUBIS_PUTCOMMENT  // 12  1%
	RUBIS_REGISTER    // 13  4%
	RUBIS_SEARCHCAT   // 14  27%
	RUBIS_SEARCHREG   // 15  12%
	RUBIS_VIEW        // 16  23%
	RUBIS_VIEWUSER    // 17  4%

	BIG_INCR
	BIG_RW
	LAST_TXN

	NABORTS
	NENOKEY
	NSTASHED
	NENORETRY
	NSAMPLES
	NGETKEYCALLS
	NDDWRITES
	NO_LOCK
	NFAIL_VERIFY
	NLOCKED
	NDIDSTASHED
	LAST_STAT
)

type Worker struct {
	sync.RWMutex
	padding     [128]byte
	ID          int
	store       *Store
	coordinator *Coordinator
	local_store *LocalStore
	next        TID
	epoch       TID
	done        chan bool
	waiters     *TStore
	E           ETransaction
	txns        []TransactionFunc

	// Stats
	Nstats       []int64
	Nwait        time.Duration
	Nwait2       time.Duration
	NKeyAccesses []int64
	tickle       chan TID

	// Rubis junk
	LastKey      []int
	CurrKey      []int
	PreAllocated bool
	start        int

	times [4][TIMES]int64
}

func (w *Worker) Register(fn int, transaction TransactionFunc) {
	w.txns[fn] = transaction
}

func NewWorker(id int, s *Store, c *Coordinator) *Worker {
	w := &Worker{
		ID:           id,
		store:        s,
		local_store:  NewLocalStore(s),
		coordinator:  c,
		Nstats:       make([]int64, LAST_STAT),
		epoch:        TID(c.epochTID),
		done:         make(chan bool),
		txns:         make([]TransactionFunc, LAST_TXN),
		tickle:       make(chan TID),
		PreAllocated: false,
	}
	if *SysType == DOPPEL {
		w.waiters = TSInit(START_SIZE)
	} else {
		w.waiters = TSInit(1)
	}
	if *SysType == LOCKING {
		w.E = StartLTransaction(w)
	} else {
		w.E = StartOTransaction(w)
	}
	w.E.SetPhase(SPLIT)
	w.Register(D_BUY, BuyTxn)
	w.Register(D_BUY_AND_READ, BuyAndReadTxn)
	w.Register(D_READ_ONE, ReadOneTxn)
	w.Register(D_READ_TWO, ReadTxn)
	w.Register(D_INCR_ONE, IncrTxn)
	w.Register(D_ATOMIC_INCR_ONE, AtomicIncr)

	w.Register(RUBIS_BID, StoreBidTxn)
	w.Register(RUBIS_VIEWBIDHIST, ViewBidHistoryTxn)
	w.Register(RUBIS_BUYNOW, StoreBuyNowTxn)
	w.Register(RUBIS_COMMENT, StoreCommentTxn)
	w.Register(RUBIS_NEWITEM, NewItemTxn)
	w.Register(RUBIS_PUTBID, PutBidTxn)
	w.Register(RUBIS_PUTCOMMENT, PutCommentTxn)
	w.Register(RUBIS_REGISTER, RegisterUserTxn)
	w.Register(RUBIS_SEARCHCAT, SearchItemsCategTxn)
	w.Register(RUBIS_SEARCHREG, SearchItemsRegionTxn)
	w.Register(RUBIS_VIEW, ViewItemTxn)
	w.Register(RUBIS_VIEWUSER, ViewUserInfoTxn)

	w.Register(BIG_INCR, BigIncrTxn)
	w.Register(BIG_RW, BigRWTxn)
	go w.run()
	return w
}

func (w *Worker) stashTxn(t Query) {
	if w.waiters.Add(t) {
		atomic.AddInt32(&w.coordinator.trigger, 1)
	}
}

func (w *Worker) doTxn(t Query) (*Result, error) {
	if t.TXN >= LAST_TXN {
		debug.PrintStack()
		log.Fatalf("Unknown transaction number %v\n", t.TXN)
	}
	w.E.Reset()
	x, err := w.txns[t.TXN](t, w.E)
	if err == ESTASH {
		w.Nstats[NSTASHED]++
		w.stashTxn(t)
		return nil, err
	} else if err == nil {
		w.Nstats[t.TXN]++
		if *Latency {
			x := time.Since(t.S)
			if t.TXN < 4 {
				y := x.Nanoseconds() / 1000
				if y >= TIMES {
					fmt.Printf("(doTxn): Too big: %v\n", x)
				} else {
					w.times[t.TXN][y]++
				}
			}
		}
	} else if err == EABORT {
		w.Nstats[NABORTS]++
	} else if err == ENOKEY {
		w.Nstats[NENOKEY]++
	} else if err == ENORETRY {
		w.Nstats[NENORETRY]++
	}
	return x, err
}

func (w *Worker) doTxn2(t Query) (*Result, error) {
	if t.TXN >= LAST_TXN {
		debug.PrintStack()
		log.Fatalf("Unknown transaction number %v\n", t.TXN)
	}
	w.E.Reset()
	x, err := w.txns[t.TXN](t, w.E)
	if err == ESTASH {
		log.Fatalf("Should not be in stashing stage right now\n")
	} else if err == nil {
		w.Nstats[t.TXN]++
		if *Latency {
			x := time.Since(t.S)
			if t.TXN < 4 {
				y := x.Nanoseconds() / 1000
				if y >= TIMES {
					fmt.Printf("(doTxn2) Too big: %v\n", x)
				} else {
					w.times[t.TXN][y]++
				}
			}
		}
	} else if err == EABORT {
		w.Nstats[NABORTS]++
	} else if err == ENOKEY {
		w.Nstats[NENOKEY]++
	} else if err == ENORETRY {
		w.Nstats[NENORETRY]++
	}
	return x, err
}

func (w *Worker) transition() {
	if *SysType == DOPPEL {
		w.Lock()
		defer w.Unlock()
		e := w.coordinator.GetEpoch()
		if e <= w.epoch {
			return
		}
		start := time.Now()
		w.E.SetPhase(MERGE)
		w.local_store.Merge()
		//dlog.Printf("[%v] Sending ack for epoch change %v\n", w.ID, e)
		w.coordinator.wepoch[w.ID] <- e
		//dlog.Printf("[%v] Waiting for safe for epoch change %v\n", w.ID, e)
		x := <-w.coordinator.wsafe[w.ID]
		if x != e {
			log.Fatalf("Worker %v out of alignment; acked %v, got safe for %v\n", w.ID, e, x)
		}
		w.E.SetPhase(JOIN)
		//dlog.Printf("[%v] Entering join phase %v\n", w.ID, e)
		for i := 0; i < len(w.waiters.t); i++ {
			committed := false
			// TODO: On abort this transaction really should be
			// reissued by the client, but in our benchmarks the
			// client doesn't wait, so here we go.
			n := 0
			for !committed && n < 20 {
				r, err := w.doTxn2(w.waiters.t[i])
				if err == EABORT {
					n++
					committed = false
				} else {
					committed = true
					if w.waiters.t[i].W != nil {
						w.waiters.t[i].W <- struct {
							R *Result
							E error
						}{r, err}
					}
				}
			}
		}
		w.waiters.clear()
		w.E.SetPhase(SPLIT)
		//dlog.Printf("[%v] Sending done %v\n", w.ID, e)
		w.coordinator.wdone[w.ID] <- e
		//dlog.Printf("[%v] Awaiting go %v\n", w.ID, e)
		x = <-w.coordinator.wgo[w.ID]
		if x != e {
			log.Fatalf("Worker %v out of alignment; said done for %v, got go for %v\n", w.ID, e, x)
		}
		//dlog.Printf("[%v] Done transitioning %v\n", w.ID, e)
		end := time.Since(start)
		w.Nwait += end
		w.epoch = e
	}
}

// Periodically check if the epoch changed.  This is important because
// I might not always be receiving calls to One()
func (w *Worker) run() {
	duration := time.Duration(*PhaseLength) * time.Millisecond
	tm := time.NewTicker(duration).C
	_ = tm
	for {
		select {
		case <-w.done:
			return
		case <-tm:
			// This is necessary if all worker threads are blocked
			// waiting for stashed reads.
			if *SysType == DOPPEL {
				w.RLock()
				e := w.coordinator.GetEpoch()
				if e > w.epoch {
					w.RUnlock()
					w.transition()
				} else {
					w.RUnlock()
				}
			}
		case <-w.tickle:
			if *SysType == DOPPEL {
				w.transition()
			}
		}
	}
}

func (w *Worker) One(t Query) (*Result, error) {
	w.RLock()
	if *SysType == DOPPEL && w.next%1000 == 0 {
		e := w.coordinator.GetEpoch()
		if w.epoch != e {
			w.RUnlock()
			w.tickle <- e
			w.RLock()
		}
	}
	r, err := w.doTxn(t)
	w.RUnlock()
	return r, err
}

func (w *Worker) PreallocateRubis(nx, nb, start int) {
	w.LastKey = make([]int, 300)
	w.CurrKey = make([]int, 300)
	w.start = start

	for i := start; i < nx+start; i++ {
		x := uint64(i<<16) | uint64(w.ID)<<8 | uint64(i%CHUNKS)

		k := UserKey(uint64(x))
		w.store.CreateKey(k, &User{}, WRITE)
		k = NicknameKey(uint64(x))
		w.store.CreateKey(k, uint64(0), WRITE)
		k = RatingKey(uint64(x))
		w.store.CreateKey(k, int32(0), SUM)

		k = CommentKey(uint64(x))
		w.store.CreateKey(k, nil, WRITE)

		k = ItemKey(uint64(x))
		w.store.CreateKey(k, nil, WRITE)
		k = MaxBidKey(uint64(x))
		w.store.CreateKey(k, int32(0), MAX)
		k = MaxBidBidderKey(uint64(x))
		w.store.CreateKey(k, uint64(0), WRITE)
		k = NumBidsKey(uint64(x))
		w.store.CreateKey(k, int32(0), SUM)
		k = BidsPerItemKey(uint64(x))
		w.store.CreateKey(k, nil, LIST)

		k = BuyNowKey(uint64(x))
		w.store.CreateKey(k, &BuyNow{}, WRITE)
	}
	w.LastKey['i'] = nx
	w.LastKey['u'] = nx
	w.LastKey['d'] = nx
	w.LastKey['c'] = nx
	w.LastKey['k'] = nx

	for i := start; i < nb+start; i++ {
		x := uint64(i<<16) | uint64(w.ID)<<8 | uint64(i%CHUNKS)
		k := BidKey(uint64(x))
		w.store.CreateKey(k, nil, WRITE)

	}
	w.LastKey['b'] = nb
	var i, j uint64
	for i = 0; i < NUM_CATEGORIES; i++ {
		w.store.CreateKey(ItemsByCatKey(i), nil, LIST)
		for j = 0; j < NUM_REGIONS; j++ {
			w.store.CreateKey(ItemsByRegKey(j, i), nil, LIST)
		}
	}
	w.PreAllocated = true
}

func (w *Worker) NextKey(f rune) uint64 {
	if !w.PreAllocated {
		w.next++
		x := uint64(w.next<<16) | uint64(w.ID)<<8 | uint64(w.next%CHUNKS)
		return x
	}
	if w.LastKey[f] == w.CurrKey[f] {
		log.Fatalf("%v Ran out of preallocated keys for %v; %v %v", w.ID, strconv.QuoteRuneToASCII(f), w.CurrKey[f], w.LastKey[f])
	}
	y := uint64(w.CurrKey[f] + w.start)
	x := uint64(y<<16) | uint64(w.ID)<<8 | y%CHUNKS
	w.CurrKey[f]++
	return x
}

func (w *Worker) GiveBack(n uint64, r rune) {
	if w.PreAllocated {
		w.CurrKey[r]--
	}
}

func (w *Worker) nextTID() TID {
	w.next++
	x := uint64(w.next<<16) | uint64(w.ID)<<8 | uint64(w.next%CHUNKS)
	return TID(x)
}

func (w *Worker) commitTID() TID {
	return w.nextTID() | w.epoch
}

func (w *Worker) Store() *Store {
	return w.store
}
