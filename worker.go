package ddtxn

import (
	"ddtxn/dlog"
	"flag"
	"log"
	"runtime/debug"
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

type TransactionFunc func(*Query, *Worker) (*Result, error)

const (
	BUFFER     = 10000
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
	Incoming    chan Query
	waiters     *TStore
	ctxn        *ETransaction
	// Stats
	Nstats  []int64
	Naborts int64

	txns []TransactionFunc
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
		Incoming:    make(chan Query, BUFFER),
		Nstats:      make([]int64, LAST_TXN),
		epoch:       c.epochTID,
		txns:        make([]TransactionFunc, LAST_TXN),
	}
	if *SysType == DOPPEL {
		w.waiters = TSInit(START_SIZE)
	} else {
		w.waiters = TSInit(1)
	}
	w.local_store.stash = true
	w.ctxn = StartTransaction(nil, w)
	w.Register(D_BUY, BuyTxn)
	w.Register(D_BUY_NC, BuyNCTxn)
	w.Register(D_BID, BidTxn)
	w.Register(D_BID_NC, BidNCTxn)
	w.Register(D_READ_BUY, ReadBuyTxn)
	go w.Go()
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
	w.ctxn.Reset(&t)
	x, err := w.txns[t.TXN](&t, w)
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

// epoch -> merged -> safe -> read -> readers done
func (w *Worker) Go() {
	for {
		select {

		// New epoch; copy over derived from previous epoch
		case msg := <-w.coordinator.wepoch[w.ID]:
			if *SysType == DOPPEL {
				w.local_store.Merge()
			}
			w.epoch = msg.T
			msg.C <- true
			msg = <-w.coordinator.wsafe[w.ID]
			w.local_store.stash = false
			for i := 0; i < len(w.waiters.t); i++ {
				t := w.waiters.t[i]
				w.doTxn(t)
			}
			w.waiters.clear()
			w.local_store.stash = true
			msg.C <- true
			<-w.coordinator.wgo[w.ID]
		// New transactions.  Do if possible.
		case t := <-w.Incoming:
			if t.TXN == LAST_TXN {
				if *SysType == DOPPEL {
					w.local_store.Merge()
					w.local_store.stash = false
					for i := 0; i < len(w.waiters.t); i++ {
						t := w.waiters.t[i]
						w.doTxn(t)
					}
					w.waiters.clear()
				}
				t.W <- nil
				dlog.Printf("%v Done\n", w.ID)
				return
			}
			w.doTxn(t)
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
