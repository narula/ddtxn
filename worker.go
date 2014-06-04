package ddtxn

import (
	"flag"
	"log"
	"math/rand"
	"runtime/debug"
	"time"
)

const (
	DOPPEL = iota
	OCC
	LOCKING
)

var SysType = flag.Int("sys", DOPPEL, "Type of system to run\n")

const (
	THRESHOLD  = 500
	RTHRESHOLD = 500
)

type TransactionFunc func(Query, *Worker) (*Result, error)

const (
	BUFFER     = 100000
	START_SIZE = 1000000
)

const (
	D_BUY = iota
	D_BUY_NC
	D_BID
	D_BID_NC
	D_READ_BUY
	LAST_TXN
)

type Worker struct {
	ID          int
	store       *Store
	local_store *LocalStore
	coordinator *Coordinator
	next        TID
	epoch       TID
	done        chan Query
	waiters     *TStore
	ctxn        *ETransaction
	// Stats
	Nstats     []int64
	Naborts    int64
	Nwait      time.Duration
	Nwait2     time.Duration
	txns       []TransactionFunc
	load       App
	local_seed uint32
}

func (w *Worker) Register(fn int, transaction TransactionFunc) {
	w.txns[fn] = transaction
}

func NewWorker(id int, s *Store, c *Coordinator, load App) *Worker {
	w := &Worker{
		ID:          id,
		store:       s,
		local_store: NewLocalStore(s),
		coordinator: c,
		Nstats:      make([]int64, LAST_TXN),
		epoch:       c.epochTID,
		done:        make(chan Query),
		txns:        make([]TransactionFunc, LAST_TXN),
		load:        load,
		local_seed:  uint32(rand.Intn(10000000)),
	}
	if *SysType == DOPPEL {
		w.waiters = TSInit(START_SIZE)
	} else {
		w.waiters = TSInit(1)
	}
	w.local_store.stash = true
	w.ctxn = StartTransaction(w)
	w.Register(D_BUY, BuyTxn)
	w.Register(D_BUY_NC, BuyNCTxn)
	w.Register(D_BID, BidTxn)
	w.Register(D_BID_NC, BidNCTxn)
	w.Register(D_READ_BUY, ReadBuyTxn)
	return w
}

func (w *Worker) stashTxn(t Query) {
	w.waiters.Add(t)
}

func (w *Worker) doTxn(t Query) {
	if t.TXN >= LAST_TXN {
		debug.PrintStack()
		log.Fatalf("Unknown transaction number %v\n", t.TXN)
	}
	w.ctxn.Reset()
	x, err := w.txns[t.TXN](t, w)
	if err == ESTASH {
		if w.local_store.stash == false {
			log.Fatalf("Stashing when I shouldn't be\n")
		}
		w.stashTxn(t)
		return
	}
	if t.W != nil {
		t.W <- x
		close(t.W)
	}
}

func (w *Worker) Transition(e TID) {
	w.epoch = TID(e)
	if *SysType == DOPPEL {
		w.local_store.Merge()
		start := time.Now()
		w.coordinator.wepoch[w.ID] <- true
		<-w.coordinator.wsafe[w.ID]
		end := time.Since(start)
		w.Nwait += end
		w.local_store.stash = false
		for i := 0; i < len(w.waiters.t); i++ {
			t := w.waiters.t[i]
			w.doTxn(t)
		}
		w.waiters.clear()
		w.local_store.stash = true
		start = time.Now()
		w.coordinator.wdone[w.ID] <- true
		<-w.coordinator.wgo[w.ID]
		end = time.Since(start)
		w.Nwait2 += end
	}
}

// epoch -> merged -> safe -> read -> readers done
func (w *Worker) Go() {
	for {
		select {
		case x := <-w.done:
			if *SysType == DOPPEL {
				w.local_store.Merge()
				w.local_store.stash = false
				for i := 0; i < len(w.waiters.t); i++ {
					t := w.waiters.t[i]
					w.doTxn(t)
				}
				w.waiters.clear()
			}
			x.W <- nil
			return
		default:
			t := w.load.MakeOne(w, &w.local_seed)
			w.doTxn(*t)
			if *SysType == DOPPEL {
				e := w.coordinator.GetEpoch()
				if w.epoch != e {
					w.Transition(e)
				}
			}
		}
	}
}

func (w *Worker) One() {
	t := w.load.MakeOne(w, &w.local_seed)
	w.doTxn(*t)
	if *SysType == DOPPEL {
		e := w.coordinator.GetEpoch()
		if w.epoch != e {
			w.Transition(e)
		}
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
