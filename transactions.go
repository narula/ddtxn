package ddtxn

import (
	"ddtxn/dlog"
	"flag"
)

// [0 7.6493694438701665]
// [1 10.419681620839363]
// [2 13.314037626628075]
// [3 14.347736200124043]
// [4 18.08972503617945]
// [5 28.364688856729373]
// [6 29.50175728757494]
// [7 33.22307215216043]
// [8 60.92619392185238]
// [9 72.23485631589827]
// [10 95.59644407690716]
// [11 99.99999999999999]

const (
	D_BUY = iota
	D_BID
	D_READ_BUY
	D_READ_FRESH
	D_READ_NP
	OCC_BUY
	OCC_BID
	OCC_READ_BUY
	OCC_READ_FRESH
	LOCK_BUY
	LOCK_READ_BUY
	LOCK_READ
	RUBIS_BID        // 12  7%
	RUBIS_BIDHIST    // 13  3%
	RUBIS_BUYNOW     // 14  3%
	RUBIS_COMMENT    // 15  1%
	RUBIS_NEWITEM    // 16  4%
	RUBIS_PUTBID     // 17  10%
	RUBIS_PUTCOMMENT // 18  1%
	RUBIS_REGISTER   // 19  4%
	RUBIS_SEARCHCAT  // 20  27%
	RUBIS_SEARCHREG  // 21  12%
	RUBIS_VIEW       // 22  23%
	RUBIS_VIEWUSER   // 23  4%
	RUBIS_BID_OCC
	RUBIS_COMMENT_OCC
	RUBIS_PUTBID_OCC
	RUBIS_VIEW_OCC
	RUBIS_NEWITEM_OCC
	LOCK_BID
	LAST_TXN
)

func IsRead(tx int) bool {
	if tx == D_READ_BUY || tx == D_READ_FRESH || tx == D_READ_NP || tx == OCC_READ_BUY ||
		tx == OCC_READ_FRESH || tx == LOCK_READ_BUY || tx == LOCK_READ {
		return true
	}
	if tx == RUBIS_BIDHIST || tx == RUBIS_PUTBID || tx == RUBIS_PUTCOMMENT || tx == RUBIS_SEARCHCAT ||
		tx == RUBIS_SEARCHREG || tx == RUBIS_VIEW || tx == RUBIS_VIEWUSER || tx == RUBIS_PUTBID_OCC ||
		tx == RUBIS_VIEW_OCC {
		return true
	}
	return false
}

const (
	NUM_USERS      = 1000000
	NUM_CATEGORIES = 20
	NUM_REGIONS    = 62
	NUM_ITEMS      = 33000
	BIDS_PER_ITEM  = 10
	NUM_COMMENTS   = 506000
	BUY_NOW        = .1 * NUM_ITEMS
	FEEDBACK       = .95 * NUM_ITEMS
)

var Allocate = flag.Bool("allocate", true, "Allocate and return results from transaction\n")
var Global_writes = flag.Bool("global", false, "When running, workers write to the central datastore every transaction\n")

// I tried keeping a slice of interfaces; the reflection was costly.
// Hard code in random parameter types to re-use for now.
// TODO: clean this up.
type Transaction struct {
	TXN int
	W   chan *Result
	T   TID
	U   Key
	P   Key
	B   Key
	A   int32
	V   string
	U1  uint64
	U2  uint64
	U3  uint64
	U4  uint64
	U5  uint64
	U6  uint64
	U7  uint64
	S1  string
	S2  string
	I   int
}

type Result struct {
	T TID
	V Value
	C bool // committed?
}

func (w *Worker) BuyTxn2(t *Transaction) *Result {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	user := t.U
	product := t.P
	amt := t.A
	tx := StartTransaction(t, w)
	tx.Write(user, "x")
	tx.Write(product, amt)
	tx.Commit()
	return r
}

// Microbenchmark BUY
func (w *Worker) BuyTxn(user Key, amt int32, product Key) *Result {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	br := w.store.getOrCreateKey(user)
	if !br.Lock() {
		dlog.Printf("Aborting\n")
		w.Naborts++
		return r
	}
	t := w.nextTID()
	var d *BRecord
	if *Global_writes {
		d = w.store.getOrCreateTypedKey(product, amt, SUM)
		if !d.Lock() {
			br.Unlock(0)
			dlog.Printf("Aborting\n")
			w.Naborts++
			return r
		}
	}

	// COMMIT POINT
	txid := t | w.epoch
	//br.value = product
	if *Global_writes {
		d.Apply(amt)
		d.Unlock(txid)
	} else {
		w.derived_sums[product] += amt
	}
	br.Unlock(txid)
	w.Nbuys++
	w.Nstats[D_BUY]++
	if *Allocate {
		r.C = true
	}
	return r
}

// Microbenchmark BID
func (w *Worker) BidTxn(bid Key, amt int32, max_bid Key, value string) *Result {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	br := w.store.getOrCreateKey(bid)
	if !br.Lock() {
		dlog.Printf("Aborting\n")
		w.Naborts++
		return r
	}
	w.ddmu.RLock()
	_, dd := w.dd[max_bid]
	w.ddmu.RUnlock()

	var d *BRecord
	if *Global_writes || (*Dynamic && *SysType == DOPPEL && !dd) {
		d = w.store.getOrCreateTypedKey(max_bid, amt, MAX)
		if !d.Lock() {
			br.Unlock(0)
			dlog.Printf("Aborting\n")
			w.Naborts++
			return r
		}
	}
	t := w.nextTID()
	txid := t | w.epoch
	br.value = value
	if *Global_writes || (*Dynamic && *SysType == DOPPEL && !dd) {
		d.Apply(amt)
		d.Unlock(txid)
	} else {
		if x, ok := w.derived_max[max_bid]; ok {
			if x < amt {
				w.derived_max[max_bid] = amt
			}
		} else {
			w.derived_max[max_bid] = amt
		}
	}
	br.Unlock(txid)
	dlog.Printf("Bid on %v\n", bid)
	w.Nbids++
	w.Nstats[D_BID]++
	if *Allocate {
		r.C = true
	}
	return r
}

func (w *Worker) ReadBuyTxn(product Key, user Key) *Result {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	t := w.nextTID()

	br := w.store.getOrCreateKey(user)
	ok, last := br.IsUnlocked()
	if !ok {
		dlog.Printf("Aborting\n")
		w.Naborts++
		return r
	}

	d := w.store.getOrCreateTypedKey(product, 0, SUM)
	ok1, last1 := d.IsUnlocked()
	if !ok1 {
		dlog.Printf("Aborting\n")
		w.Naborts++
		return r
	}

	v1 := d.int_value
	txid := t | w.epoch

	if !br.Verify(last) || !d.Verify(last1) {
		dlog.Printf("Aborting\n")
		w.Naborts++
		return r
	}
	if *Allocate {
		r = &Result{txid, v1, true}
	}
	w.Nreads++
	w.Nstats[D_READ_BUY]++
	return r
}

func (w *Worker) ReadFreshKeyTxn(product Key) *Result {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	var x Value
	d, err := w.store.getKey(product)
	if err != nil {
		dlog.Printf("No key for %v\n", product)
		// Equivalent to 0?
		return r
	}
	ok, last := d.IsUnlocked()
	if !ok {
		w.Naborts++
		return r
	}
	x = d.Value()
	t := w.nextTID()
	txid := t | w.epoch
	if !d.Verify(last) {
		w.Naborts++
		return r
	}
	if *Allocate {
		r.T = txid
		r.V = x
		r.C = true
	}
	w.Nreads++
	w.Nstats[D_READ_FRESH]++
	return r
}

func (w *Worker) OCCBuyTxn(user Key, amt int32, product Key) *Result {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	b := w.store.getOrCreateKey(product)
	if !b.Lock() {
		dlog.Printf("Aborting\n")
		w.Naborts++
		return r
	}
	ubr := w.store.getOrCreateKey(user)
	if !ubr.Lock() {
		dlog.Printf("Aborting\n")
		b.Unlock(0)
		w.Naborts++
		return r
	}

	t := w.nextTID()
	txid := w.epoch | t

	//ubr.value = product
	b.int_value = b.int_value + amt
	//dlog.Printf("%v %v Succeeding %v+%v=%v %x\n", user, product, x, amt, b.value, txid)
	ubr.Unlock(txid)
	b.Unlock(txid)
	w.Nbuys++
	w.Nstats[OCC_BUY]++
	if *Allocate {
		r = &Result{C: true}
	}
	return r
}

func (w *Worker) OCCBidTxn(bid Key, amt int32, max_bid Key, value string) *Result {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	bid_record := w.store.getOrCreateKey(bid)
	max_bid_record := w.store.getOrCreateKey(max_bid)
	ok, last := max_bid_record.IsUnlocked()
	if !ok {
		w.Naborts++
		return r
	}

	v := max_bid_record.value.(int32)

	if amt <= v {
		// Do not have a higher bid
		t := w.nextTID()
		txid := w.epoch | t

		if !bid_record.Lock() {
			w.Naborts++
			return r
		}
		// validate read
		if !max_bid_record.Verify(last) {
			bid_record.Unlock(txid)
			w.Naborts++
			return r
		}
		bid_record.value = value
		bid_record.Unlock(txid)
		w.Nbids++
		return r
	}

	// Have a higher bid, lock

	if !bid_record.Lock() {
		w.Naborts++
		return r
	}

	if !max_bid_record.Lock() {
		bid_record.Unlock(0)
		w.Naborts++
		return r
	}

	t := w.nextTID()
	txid := w.epoch | t

	// validate
	if !max_bid_record.Verify(last) {
		max_bid_record.Unlock(0)
		bid_record.Unlock(0)
		w.Naborts++
		return r
	}
	max_bid_record.value = amt
	max_bid_record.Unlock(txid)
	bid_record.Unlock(txid)
	w.Nbids++
	w.Nstats[OCC_BID]++
	if *Allocate {
		r.C = true
	}
	return r
}

func (w *Worker) LockBuyTxn(user Key, amt int32, product Key) *Result {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	u := w.store.getOrCreateKey(user)
	u.mu.Lock()
	b := w.store.getOrCreateKey(product)
	b.mu.Lock()
	v := b.int_value
	t := w.nextTID()
	txid := w.epoch | t
	_ = txid
	b.int_value = v + amt
	//u.value = product

	b.mu.Unlock()
	u.mu.Unlock()
	w.Nbuys++
	w.Nstats[LOCK_BUY]++
	if *Allocate {
		r = &Result{T: txid, C: true}
	}
	return r
}

func (w *Worker) LockBidTxn(user Key, amt int32, product Key) *Result {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	u := w.store.getOrCreateKey(user)
	u.mu.Lock()
	b := w.store.getOrCreateKey(product)
	b.mu.Lock()
	v := b.int_value
	t := w.nextTID()
	txid := w.epoch | t
	_ = txid
	if v < amt {
		b.int_value = amt
	}
	b.mu.Unlock()
	u.mu.Unlock()
	w.Nbids++
	w.Nstats[LOCK_BUY]++
	if *Allocate {
		r = &Result{T: txid, C: true}
	}
	return r
}

func (w *Worker) LockReadTxn(key Key) *Result {
	b := w.store.getOrCreateKey(key)
	b.mu.RLock()
	x := b.Value()
	t := w.nextTID()
	txid := w.epoch | t
	w.Nreads++
	var r *Result = nil
	if *Allocate {
		r = &Result{txid, x, true}
	}
	b.mu.RUnlock()
	w.Nstats[LOCK_READ]++
	return r
}

func (w *Worker) LockReadBuyTxn(product Key, user Key) *Result {
	b2 := w.store.getOrCreateKey(user)
	b2.mu.RLock()
	b := w.store.getOrCreateKey(product)
	b.mu.RLock()
	x := b.int_value
	t := w.nextTID()
	txid := w.epoch | t
	w.Nreads++
	var r *Result = nil
	if *Allocate {
		r = &Result{txid, x, true}
	}
	b.mu.RUnlock()
	b2.mu.RUnlock()
	w.Nstats[LOCK_READ_BUY]++
	return r
}
