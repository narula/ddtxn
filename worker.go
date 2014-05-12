package ddtxn

import (
	"ddtxn/dlog"
	"flag"
	"log"
	"runtime/debug"
	"sync"
)

const (
	DOPPEL = iota
	OCC
	LOCKING
)

var SysType = flag.Int("sys", DOPPEL, "Type of system to run\n")

const (
	THRESHOLD = 500
)

type TransactionFunc func(*Query, *Worker) (*Result, error)

type Worker struct {
	ID          int
	store       *Store
	local_store *LocalStore
	coordinator *Coordinator
	dd          map[Key]bool
	candidates  map[Key]int
	next        TID
	epoch       TID
	Incoming    chan Query
	waiters     *TStore
	// Stats
	Nstats  []int64
	Naborts int64
	Ncopy   int64
	ddmu    sync.RWMutex

	txns []TransactionFunc
}

const (
	BUFFER     = 10000
	START_SIZE = 1000000
)

func (w *Worker) Register(fn int, transaction TransactionFunc) {
	w.txns[fn] = transaction
}

const (
	D_BUY = iota
	D_BID
	D_BID_NC
	D_READ_BUY
	LAST_TXN
)

func NewWorker(id int, s *Store, c *Coordinator) *Worker {
	w := &Worker{
		ID:          id,
		store:       s,
		local_store: NewLocalStore(s),
		coordinator: c,
		dd:          make(map[Key]bool),
		candidates:  make(map[Key]int),
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
	w.Register(D_BUY, BuyTxn)
	w.Register(D_BID, BidTxn)
	w.Register(D_BID_NC, BidNCTxn)
	w.Register(D_READ_BUY, ReadBuyTxn)
	go w.Go()
	return w
}

func (w *Worker) AddDD(keys map[Key]bool) {
	if len(keys) > 0 {
		dlog.Printf("w %d adding %v\n", w.ID, keys)
	}
	w.ddmu.Lock()
	for k, _ := range keys {
		w.dd[k] = true
	}
	w.ddmu.Unlock()
}

func (w *Worker) stashTxn(t Query) {
	w.waiters.Add(t)
}

func (w *Worker) doTxn(t Query) {
	if t.TXN >= LAST_TXN {
		debug.PrintStack()
		log.Fatalf("Unknown transaction number %v\n", t.TXN)
	}
	x, err := w.txns[t.TXN](&t, w)
	if err == ESTASH {
		w.stashTxn(t)
		return
	}
	if t.W != nil {
		t.W <- x
		close(t.W)
	}
}

func (w *Worker) SetDD(keys []Key) {
	if *SysType == DOPPEL {
		log.Fatalf("Why are you calling this\n")
	}
	w.dd = make(map[Key]bool, len(keys))
	for _, k := range keys {
		w.dd[k] = true
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
			for i := 0; i < len(w.waiters.t); i++ {
				t := w.waiters.t[i]
				w.doTxn(t)
			}
			w.waiters.clear()
			msg.C <- true
			<-w.coordinator.wgo[w.ID]
		// New transactions.  Do if possible.
		case t := <-w.Incoming:
			if t.TXN == LAST_TXN {
				if *SysType == DOPPEL {
					w.local_store.Merge()
					for i := 0; i < len(w.waiters.t); i++ {
						t := w.waiters.t[i]
						w.doTxn(t)
					}
					w.waiters.clear()
				}
				t.W <- nil
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
