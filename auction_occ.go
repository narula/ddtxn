package ddtxn

import (
	"ddtxn/dlog"
	"time"
)

func (w *Worker) TxnNewItemOCC(seller uint64, name string, desc string, sprice uint64,
	rprice uint64, bnow uint64, duration uint64, quan uint64, endd int, categ uint64) *Result {

	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	//who cares
	now := time.Now().Second()
	//now := 1

	n := w.nextTID()
	x := &Item{
		ID:        uint64(n),
		Name:      name,
		Seller:    seller,
		Desc:      desc,
		Sprice:    sprice,
		Rprice:    rprice,
		Buynow:    bnow,
		Dur:       duration,
		Qty:       quan,
		Startdate: now,
		Enddate:   endd,
		Categ:     categ,
	}
	un := ItemKey(uint64(n))
	unr := w.store.getOrCreateKey(un)

	uk := UserKey(int(seller))
	urec, err := w.store.getKey(uk)
	if err != nil {
		dlog.Printf("User doesn't exist %v\n", seller)
		return nil
	}

	ok, last := urec.IsUnlocked()
	if !ok {
		return nil
	}

	region := urec.value.(*User).Region

	ibck := ItemsByCatKey(categ)
	y := w.store.getOrCreateTypedKey(ibck, nil, LIST)

	regk := ItemsByRegKey(region, categ)
	y2 := w.store.getOrCreateTypedKey(regk, nil, LIST)

	if !unr.Lock() {
		return r
	}

	if !y.Lock() {
		w.Naborts++
		unr.Unlock(0)
		return r
	}

	if !y2.Lock() {
		w.Naborts++
		y.Unlock(0)
		unr.Unlock(0)
		return r
	}

	e := w.epoch
	txid := e | n

	if !urec.Verify(last) {
		w.Naborts++
		y2.Unlock(0)
		y.Unlock(0)
		unr.Unlock(0)
		return nil
	}

	unr.value = x
	y.AddOneToList(Entry{order: now, top: int(n)})
	y2.AddOneToList(Entry{order: now, top: int(n)})
	// update max bid?
	w.store.getOrCreateTypedKey(MaxBidKey(uint64(n)), int32(0), MAX)
	w.store.getOrCreateTypedKey(MaxBidBidderKey(uint64(n)), uint64(0), WRITE)
	w.store.getOrCreateTypedKey(NumBidsKey(uint64(n)), int32(0), SUM)
	w.store.getOrCreateTypedKey(BidsPerItemKey(uint64(n)), nil, LIST)

	y2.Unlock(txid)
	y.Unlock(txid)
	unr.Unlock(txid)

	if *Allocate {
		r = &Result{n, uint64(n), true}
		dlog.Printf("Registered item %v cat %v\n", n, categ)
	}
	w.Nstats[RUBIS_NEWITEM]++
	return r
}

func (w *Worker) OCCStoreBid(user, item uint64, price int32) *Result {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	// insert bid
	n := w.nextTID()
	bid := &Bid{
		ID:     uint64(n),
		Item:   item,
		Bidder: user,
		Price:  price,
	}
	un := BidKey(uint64(n))
	bidr := w.store.getOrCreateKey(un)

	// update max bid? add max to read set.
	high := MaxBidKey(item)
	m := w.store.getOrCreateTypedKey(high, int32(0), MAX)
	ok, last := m.IsUnlocked()
	if !ok {
		w.Naborts++
		return r
	}

	num := NumBidsKey(item)
	m3 := w.store.getOrCreateTypedKey(num, int32(0), SUM)
	bids := BidsPerItemKey(item)
	m4 := w.store.getOrCreateTypedKey(bids, nil, LIST)

	update := false
	// check to see if we're the max; start locking
	var m2 *BRecord
	if price > m.int_value {
		update = true
		if !m.Lock() {
			w.Naborts++
			return r
		}
		// if we're max update bidder too
		bidder := MaxBidBidderKey(item)
		m2 = w.store.getOrCreateTypedKey(bidder, uint64(0), WRITE)
		if !m2.Lock() {
			w.Naborts++
			m.Unlock(0)
			return r
		}
	}

	if !m3.Lock() {
		w.Naborts++
		if update {
			m2.Unlock(0)
			m.Unlock(0)
		}
		return r
	}

	if !m4.Lock() {
		w.Naborts++
		m3.Unlock(0)
		if update {
			m2.Unlock(0)
			m.Unlock(0)
		}
		return r
	}

	if !bidr.Lock() {
		if update {
			m.Unlock(0)
			m2.Unlock(0)
		}
		m3.Unlock(0)
		m4.Unlock(0)
		w.Naborts++
		return r
	}

	e := w.epoch
	txid := e | n

	if !update {
		if !m.Verify(last) {
			m4.Unlock(0)
			m3.Unlock(0)
			bidr.Unlock(0)
			w.Naborts++
			return r
		}
	}

	bidr.value = bid
	if update {
		m.int_value = price
		m2.value = user
	}
	m3.int_value = m3.int_value + 1
	m4.AddOneToList(Entry{int(bid.Price), un, 0})

	m4.Unlock(txid)
	m3.Unlock(txid)
	if update {
		m2.Unlock(txid)
		m.Unlock(txid)
	}
	bidr.Unlock(txid)
	if *Allocate {
		r = &Result{txid, uint64(n), true}
	}
	w.Nstats[RUBIS_BID]++
	return r
}

// XXX inserts are derived data?
// tx -- read: items, users; writes: comments, users
func (w *Worker) OCCTxnStoreComment(toid uint64, fromid uint64, item uint64, com string, rating uint64) *Result {
	tok := UserKey(int(toid))
	torec, err := w.store.getKey(tok)
	if err != nil {
		dlog.Printf("User doesn't exist: %v\n", toid)
		return nil
	}

	n := w.nextTID()

	comk := CommentKey(uint64(n))
	_, err = w.store.getKey(comk)
	if err == nil {
		dlog.Printf("Warning: comment already exists %v\n", n)
	}

	comrec := w.store.getOrCreateKey(comk)
	ncom := Comment{
		ID:      n,
		From:    fromid,
		To:      toid,
		Rating:  rating,
		Item:    item,
		Date:    42,
		Comment: com,
	}

	ok1, last1 := torec.IsUnlocked()
	if !ok1 {
		return nil
	}

	newrating := torec.value.(*User).Rating + rating

	if !torec.Lock() {
		return nil
	}
	if !comrec.Lock() {
		torec.Unlock(0)
		return nil
	}

	valid1, _ := w.store.getKey(tok)

	if valid1.Verify(last1) {
		comrec.Unlock(0)
		torec.Unlock(0)
		return nil
	}

	e := w.epoch
	txid := e | n

	torec.value.(*User).Rating = newrating
	comrec.value = ncom

	comrec.Unlock(txid)
	torec.Unlock(txid)

	var r *Result = nil
	if *Allocate {
		r = &Result{n, nil, true}
	}
	w.Nstats[RUBIS_COMMENT]++
	return r
}
