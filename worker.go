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
var Dynamic = flag.Bool("dynamic", false, "Try to dynamically move data from dd to bd.  Only affects BUY benchmark.\n")

const (
	THRESHOLD = 500
)

type Worker struct {
	ID           int
	store        *Store
	coordinator  *Coordinator
	derived_sums map[Key]int32
	derived_max  map[Key]int32
	derived_bw   map[Key]Value
	derived_list map[Key][]Entry
	dd           map[Key]bool
	candidates   map[Key]int
	next         TID
	epoch        TID
	safe         TID
	Incoming     chan Transaction
	txn_funcs    []func([]interface{}) *Result
	waiters      *TStore
	// Stats
	Nstats   []int64
	Nreads   int64
	Nbuys    int64
	Nbids    int64
	Naborts  int64
	Ncopy    int64
	Nfound   int64
	Nentered int64
	ddmu     sync.RWMutex
	keymap   [](func(uint64) Key)

	lasts []uint64
	recs  []*BRecord
}

const (
	BUFFER     = 10000
	START_SIZE = 1000000
)

func NewWorker(id int, s *Store, c *Coordinator) *Worker {
	w := &Worker{
		ID:           id,
		store:        s,
		coordinator:  c,
		derived_sums: make(map[Key]int32),
		derived_max:  make(map[Key]int32),
		derived_bw:   make(map[Key]Value),
		derived_list: make(map[Key][]Entry),
		dd:           make(map[Key]bool),
		candidates:   make(map[Key]int),
		Incoming:     make(chan Transaction, BUFFER),
		Nstats:       make([]int64, LAST_TXN),
		lasts:        make([]uint64, 0, 3*(DEFAULT_LIST_SIZE+1)),
		recs:         make([]*BRecord, 0, 3*(DEFAULT_LIST_SIZE+1)),
		epoch:        c.epochTID,
	}
	if *SysType == DOPPEL && !*Global_writes {
		w.waiters = TSInit(START_SIZE)
	} else {
		w.waiters = TSInit(1)
	}
	go w.Go()
	return w
}

func (w *Worker) CopyDerived(e TID) {
	for k, v := range w.derived_sums {
		if *SysType == OCC {
			debug.PrintStack()
			log.Fatalf("Why is there derived data %v %v\n", k, v)
		}
		if v == 0 && w.ID != 0 {
			continue
		}
		if v == 0 {
			continue
		}
		d := w.store.getOrCreateTypedKey(k, int32(0), SUM)
		d.Apply(v)
		w.derived_sums[k] = 0
		w.Ncopy++
	}

	for k, v := range w.derived_max {
		if *SysType == OCC {
			debug.PrintStack()
			log.Fatalf("Why is there derived data %v %v\n", k, v)
		}

		if v == 0 && w.ID != 0 {
			continue
		}
		d := w.store.getOrCreateTypedKey(k, int32(0), MAX)
		d.Apply(v)
		w.Ncopy++
	}

	for k, v := range w.derived_bw {
		if *SysType == OCC {
			debug.PrintStack()
			log.Fatalf("Why is there derived data %v %v\n", k, v)
		}

		d := w.store.getOrCreateTypedKey(k, "", WRITE)
		d.Apply(v)
		w.Ncopy++
	}

	for k, v := range w.derived_list {
		if *SysType == OCC {
			debug.PrintStack()
			log.Fatalf("Why is there derived data %v %v\n", k, v)
		}
		if len(v) == 0 {
			continue
		}

		d := w.store.getOrCreateTypedKey(k, nil, LIST)
		d.Apply(v)
		delete(w.derived_list, k)
		w.Ncopy++
	}
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

func (w *Worker) stashTxn(t Transaction) {
	// Enqueue read for later
	t.T = w.epoch
	w.waiters.Add(t)
}

func (w *Worker) doTxn(t Transaction) {
	var x *Result
	switch t.TXN {
	case D_BUY:
		if *Dynamic && *SysType == DOPPEL {
			w.ddmu.RLock()
			ok, _ := w.dd[t.P]
			w.ddmu.RUnlock()
			if ok {
				x = w.BuyTxn(t.U, t.A, t.P)
			} else {
				// This product key is not derived data
				x = w.OCCBuyTxn(t.U, t.A, t.P)
			}
		} else {
			x = w.BuyTxn(t.U, t.A, t.P)
		}
	case D_BID:
		x = w.BidTxn(t.U, t.A, t.P, t.V)
	case D_READ_BUY:
		if *Dynamic && *SysType == DOPPEL {
			w.ddmu.RLock()
			ok, _ := w.dd[t.P]
			w.ddmu.RUnlock()
			if !ok {
				// This product key is not derived data
				x = w.ReadBuyTxn(t.P, t.U)
			} else {
				w.stashTxn(t)
				return
			}
		} else if *Global_writes {
			x = w.ReadBuyTxn(t.P, t.U)
		} else {
			w.stashTxn(t)
			return
		}
	case D_READ_FRESH:
		if *Dynamic && *SysType == DOPPEL {
			w.ddmu.RLock()
			ok, _ := w.dd[t.P]
			w.ddmu.RUnlock()
			if !ok {
				// This product key is not derived data
				x = w.ReadFreshKeyTxn(t.P)
			} else {
				w.stashTxn(t)
				return
			}
		} else if *Global_writes {
			x = w.ReadFreshKeyTxn(t.P)
		} else {
			w.stashTxn(t)
			return
		}
	case D_READ_NP:
		x = w.ReadFreshKeyTxn(t.P)
	case OCC_BUY:
		x = w.OCCBuyTxn(t.U, t.A, t.P)
	case OCC_READ_BUY:
		x = w.ReadBuyTxn(t.P, t.U)
	case OCC_READ_FRESH:
		x = w.ReadFreshKeyTxn(t.P)
	case LOCK_BUY:
		x = w.LockBuyTxn(t.U, t.A, t.P)
	case LOCK_READ:
		x = w.LockReadTxn(t.P)
	case LOCK_READ_BUY:
		x = w.LockReadBuyTxn(t.P, t.U)

		// RUBIS, always derived:
		// - items by category
		// - items by region
		// - user ratings
		// - numbids
		// - bid history
		// RUBIS, sometimes derived:
		// - max bid
		// - max bidder
	case RUBIS_BID:
		if *SysType == DOPPEL {
			x = w.TxnStoreBid(t.U1, t.U2, t.A)
		} else if *SysType == OCC {
			x = w.OCCStoreBid(t.U1, t.U2, t.A)
		}
	case RUBIS_BIDHIST:
		if *SysType == DOPPEL {
			w.stashTxn(t)
			return
		} else if *SysType == OCC {
			x = w.TxnViewBidHistory(t.U1, false)
		}
	case RUBIS_BUYNOW:
		x = w.TxnStoreBuyNow(t.U1, t.U2, t.U3)
	case RUBIS_COMMENT:
		x = w.OCCTxnStoreComment(t.U1, t.U2, t.U3, t.S1, t.U4)
	case RUBIS_NEWITEM:
		//		x = w.TxnNewItem(t.U1, t.S1, t.S2, t.U2, t.U3, t.U4, t.U5, t.U6, t.I, t.U7)
		x = w.TxnNewItemOCC(t.U1, t.S1, t.S2, t.U2, t.U3, t.U4, t.U5, t.U6, t.I, t.U7)
	case RUBIS_PUTBID:
		if *SysType == DOPPEL {
			w.stashTxn(t)
			return
		} else if *SysType == OCC {
			x = w.TxnPutBid(t.U1)
		}
	case RUBIS_PUTCOMMENT:
		x = w.TxnPutComment(t.U1, t.U2)
	case RUBIS_REGISTER:
		x = w.TxnRegisterUser(t.S1, t.U1)
	case RUBIS_SEARCHCAT:
		//		if *SysType == DOPPEL {
		//			w.stashTxn(t)
		//			return
		//		} else if *SysType == OCC {
		x = w.TxnSearchItemsCateg(t.U1, t.U2)
		//		}
	case RUBIS_SEARCHREG:
		//if *SysType == DOPPEL {
		//	w.stashTxn(t)
		//	return
		//} else if *SysType == OCC {
		x = w.TxnSearchItemsReg(t.U1, t.U2, t.U3)
		//}
	case RUBIS_VIEW:
		if *Dynamic && *SysType == DOPPEL {
			keys := make([]Key, 3)
			keys[0] = ItemKey(t.U1)
			keys[1] = MaxBidKey(t.U1)
			keys[2] = BidsPerItemKey(t.U1)
			w.ddmu.RLock()
			for i := 0; i < 3; i++ {
				ok, _ := w.dd[keys[i]]
				if ok {
					w.stashTxn(t)
					w.ddmu.RUnlock()
					return
				}
			}
			w.ddmu.RUnlock()
			x = w.TxnViewItem(t.U1, false)
		} else if *SysType == DOPPEL {
			w.stashTxn(t)
			return
		} else if *SysType == OCC {
			x = w.TxnViewItem(t.U1, false)
		}
	case RUBIS_PUTBID_OCC:
		x = w.TxnPutBid(t.U1)
	case RUBIS_VIEWUSER:
		x = w.TxnViewUserInfo(t.U1)
	case RUBIS_BID_OCC:
		x = w.OCCStoreBid(t.U1, t.U2, t.A)
	case RUBIS_COMMENT_OCC:
		x = w.OCCTxnStoreComment(t.U1, t.U2, t.U3, t.S1, t.U4)
	case RUBIS_NEWITEM_OCC:
		x = w.TxnNewItemOCC(t.U1, t.S1, t.S2, t.U2, t.U3, t.U4, t.U5, t.U6, t.I, t.U7)
	}
	if t.W != nil {
		t.W <- x
		close(t.W)
	}
}

func (w *Worker) doRead(t Transaction) {
	var x *Result
	if t.TXN == D_READ_FRESH {
		x = w.ReadFreshKeyTxn(t.P)
	} else if t.TXN == D_READ_BUY {
		x = w.ReadBuyTxn(t.P, t.U)
	} else if t.TXN == RUBIS_VIEW {
		x = w.TxnViewItem(t.U1, true)
	} else if t.TXN == RUBIS_BIDHIST {
		x = w.TxnViewBidHistory(t.U1, true)
	} else if t.TXN == RUBIS_PUTBID {
		x = w.TxnPutBid(t.U1)
	} else if t.TXN == RUBIS_PUTCOMMENT {
		x = w.TxnPutComment(t.U1, t.U2)
	} else if t.TXN == RUBIS_SEARCHCAT {
		x = w.TxnSearchItemsCateg(t.U1, t.U2)
	} else if t.TXN == RUBIS_SEARCHREG {
		x = w.TxnSearchItemsReg(t.U1, t.U2, t.U3)
	}
	if t.W != nil {
		t.W <- x
		close(t.W)
	}
}

func (w *Worker) SetDD(keys []Key) {
	if !*Dynamic && *SysType == DOPPEL {
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
			//dlog.Printf("[w %v] Epoch change %x->%x\n", w.ID, w.epoch, msg.T)
			if *SysType == DOPPEL && !*Global_writes {
				w.CopyDerived(w.epoch)
			}
			w.epoch = msg.T
			msg.C <- true
			msg = <-w.coordinator.wsafe[w.ID]
			//dlog.Printf("[w %v] Safe change %x->%x\n", w.ID, w.safe, msg.T)
			w.safe = msg.T
			for i := 0; i < len(w.waiters.t); i++ {
				t := w.waiters.t[i]
				w.doRead(t)
			}
			w.waiters.clear()
			msg.C <- true
			<-w.coordinator.wgo[w.ID]
		// New transactions.  Do if possible.
		case t := <-w.Incoming:
			if t.TXN == LAST_TXN {
				if *SysType == DOPPEL && !*Global_writes {
					w.CopyDerived(w.epoch)
					for i := 0; i < len(w.waiters.t); i++ {
						t := w.waiters.t[i]
						w.doRead(t)
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
