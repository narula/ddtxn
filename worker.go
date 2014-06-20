package ddtxn

import (
	"ddtxn/dlog"
	"flag"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

const (
	DOPPEL = iota
	OCC
	LOCKING
)

var SysType = flag.Int("sys", DOPPEL, "Type of system to run\n")
var CountKeys = flag.Bool("ck", false, "Count keys accessed")

type TransactionFunc func(Query, *ETransaction) (*Result, error)

const (
	BUFFER     = 100000
	START_SIZE = 1000000
)

const (
	D_BUY = iota
	D_BUY_NC
	D_BID
	D_BID_NC
	D_READ_ONE
	RUBIS_REGISTER
	RUBIS_NEWITEM
	RUBIS_BID
	RUBIS_SEARCHCAT
	RUBIS_VIEW
	BIG_INCR
	BIG_RW
	LAST_TXN

	NABORTS
	NENOKEY
	NSTASHED
	NSAMPLES
	NGETKEYCALLS
	NDDWRITES
	LAST_STAT
)

type Worker struct {
	sync.RWMutex
	ID          int
	store       *Store
	coordinator *Coordinator
	local_store *LocalStore
	next        TID
	epoch       TID
	done        chan Query
	waiters     *TStore
	E           *ETransaction
	txns        []TransactionFunc

	// Stats
	Nstats       []int64
	Nwait        time.Duration
	Nwait2       time.Duration
	NKeyAccesses []int64
}

func (w *Worker) Register(fn int, transaction TransactionFunc) {
	w.txns[fn] = transaction
}

func NewWorker(id int, s *Store, c *Coordinator) *Worker {
	w := &Worker{
		ID:          id,
		store:       s,
		local_store: NewLocalStore(s),
		coordinator: c,
		Nstats:      make([]int64, LAST_STAT),
		epoch:       c.epochTID,
		done:        make(chan Query),
		txns:        make([]TransactionFunc, LAST_TXN),
	}
	if *SysType == DOPPEL {
		w.waiters = TSInit(START_SIZE)
	} else {
		w.waiters = TSInit(1)
	}
	w.local_store.phase = SPLIT
	w.E = StartTransaction(w)
	w.Register(D_BUY, BuyTxn)
	w.Register(D_BUY_NC, BuyNCTxn)
	w.Register(D_BID, BidTxn)
	w.Register(D_BID_NC, BidNCTxn)
	w.Register(D_READ_ONE, ReadTxn)
	w.Register(RUBIS_REGISTER, RegisterUserTxn)
	w.Register(RUBIS_NEWITEM, NewItemTxn)
	w.Register(RUBIS_BID, StoreBidTxn)
	w.Register(RUBIS_SEARCHCAT, SearchItemsCategTxn)
	w.Register(RUBIS_VIEW, ViewItemTxn)
	w.Register(BIG_INCR, BigIncrTxn)
	w.Register(BIG_RW, BigRWTxn)
	go w.run()
	return w
}

func (w *Worker) stashTxn(t Query) {
	w.waiters.Add(t)
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
	} else if err == EABORT {
		w.Nstats[NABORTS]++
	} else if err == ENOKEY {
		w.Nstats[NENOKEY]++
	}
	return x, err
}

func (w *Worker) transition(e TID) {
	w.epoch = e
	if *SysType == DOPPEL {
		w.local_store.phase = MERGE
		w.local_store.Merge()
		start := time.Now()
		w.coordinator.wepoch[w.ID] <- true
		<-w.coordinator.wsafe[w.ID]
		end := time.Since(start)
		w.Nwait += end
		w.local_store.phase = JOIN
		for i := 0; i < len(w.waiters.t); i++ {
			r, err := w.doTxn(w.waiters.t[i])
			if err == ESTASH {
				log.Fatalf("Stashing when trying to execute stashed\n")
			}
			if w.waiters.t[i].W != nil {
				w.waiters.t[i].W <- struct {
					R *Result
					E error
				}{r, err}
			}
		}
		w.waiters.clear()
		w.local_store.phase = SPLIT
		start = time.Now()
		w.coordinator.wdone[w.ID] <- true
		<-w.coordinator.wgo[w.ID]
		end = time.Since(start)
		w.Nwait2 += end
	}
}

// Periodically check if the epoch changed.  This is important because
// I might not always be receiving calls to One()
func (w *Worker) run() {
	duration := time.Duration(BUMP_EPOCH_MS*2) * time.Millisecond
	tm := time.NewTicker(duration).C
	_ = tm
	for {
		select {
		case x := <-w.done:
			if *SysType == DOPPEL {
				dlog.Printf("%v Received done\n", w.ID)
				w.Lock()
				w.local_store.phase = MERGE
				w.local_store.Merge()
				dlog.Printf("%v Done last merge, doing %v waiters\n", w.ID, len(w.waiters.t))
				w.local_store.phase = JOIN
				for i := 0; i < len(w.waiters.t); i++ {
					t := w.waiters.t[i]
					r, err := w.doTxn(t)
					if t.W != nil {
						t.W <- struct {
							R *Result
							E error
						}{r, err}
					}
				}
				w.waiters.clear()
				w.Unlock()
			}
			x.W <- struct {
				R *Result
				E error
			}{nil, nil}
			return
		case <-tm:
			// This is necessary if all worker threads are blocked
			// waiting for stashed reads, but there's a read race
			// condition on w.epoch with a call to transition() from
			// One().  I'm ok with that cause this is just a tickle.
			if *SysType == DOPPEL {
				e := w.coordinator.GetEpoch()
				if e > w.epoch {
					w.Lock()
					e := w.coordinator.GetEpoch()
					if e > w.epoch {
						w.transition(e)
					}
					w.Unlock()
				}
			}
		}
	}
}

func (w *Worker) One(t Query) (*Result, error) {
	w.RLock()
	if *SysType == DOPPEL {
		e := w.coordinator.GetEpoch()
		if w.epoch != e {
			w.transition(e)
		}
	}
	r, err := w.doTxn(t)
	w.RUnlock()
	return r, err
}

func (w *Worker) nextTID() TID {
	w.next++
	x := uint64(w.next<<16) | uint64(w.ID)<<8 | uint64(w.next%CHUNKS)
	return TID(x)
}

func (w *Worker) commitTID() TID {
	return w.nextTID() | w.epoch
}
