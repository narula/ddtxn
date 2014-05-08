package ddtxn

import "ddtxn/dlog"

func (w *Worker) LockTxnStoreBid(user, item uint64, price int32) *Result {
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
	bidr.mu.Lock()
	high := MaxBidKey(item)
	highr := w.store.getOrCreateTypedKey(high, int32(0), MAX)
	highr.mu.Lock()
	bidder := MaxBidBidderKey(item)
	bidderr := w.store.getOrCreateTypedKey(bidder, "", WRITE)
	bidderr.mu.Lock()
	num := NumBidsKey(item)
	numr := w.store.getOrCreateTypedKey(num, int32(0), SUM)
	numr.mu.Lock()
	bids := BidsPerItemKey(item)
	bidsr := w.store.getOrCreateTypedKey(bids, nil, LIST)
	bidsr.mu.Lock()

	e := w.epoch
	txid := e | n
	bidr.value = bid
	if highr.int_value < price {
		highr.int_value = price
		bidderr.value = user
	}
	numr.int_value = numr.int_value + 1
	bidsr.AddOneToList(Entry{int(bid.Price), un, 0})

	bidsr.mu.Unlock()
	numr.mu.Unlock()
	bidderr.mu.Unlock()
	highr.mu.Unlock()
	bidr.mu.Unlock()

	var r *Result = nil
	if *Allocate {
		r = &Result{txid, uint64(n), true}
	}
	w.Nstats[RUBIS_BID]++
	return r
}

// tx -- read: item, bidsperitem, writes:
func (w *Worker) LockTxnViewItem(id uint64) *Result {
	it := ItemKey(id)
	v, err := w.store.getKey(it)
	if err != nil {
		dlog.Printf("No such item %v\n", it)
		return nil
	}
	v.mu.RLock()
	mbk := MaxBidKey(id)
	mbs := MaxBidBidderKey(id)
	d1 := w.store.getOrCreateTypedKey(mbk, int32(0), MAX)
	d1.mu.RLock()
	max_bid := d1.int_value

	d2 := w.store.getOrCreateTypedKey(mbs, uint64(0), WRITE)
	d2.mu.RLock()
	bidder := d2.value

	v1, _ := w.store.getKey(it)
	v1.mu.RLock()

	// User info for seller, bidder?
	var r *Result = nil
	if *Allocate {
		// XXX
		iret := *v.value.(*Item)
		r = &Result{0, &struct {
			Item
			int32
			uint64
		}{iret, max_bid, bidder.(uint64)}, true}
	}

	v1.mu.RUnlock()
	d2.mu.RUnlock()
	d1.mu.RUnlock()
	w.Nstats[RUBIS_VIEW]++
	return r
}

// tx -- read: item, bidsperitem, writes:
func (w *Worker) LockTxnViewBidHistory(item uint64) *Result {
	_, err := w.store.getKey(ItemKey(item))
	if err != nil {
		dlog.Printf("No such item %v\n", item)
		return nil
	}

	bids := BidsPerItemKey(item)
	d, _ := w.store.getKey(bids)
	d.mu.RLock()

	listy := d.entries

	rbids := make([]Bid, len(listy))
	rnn := make([]string, len(listy))

	locks := make([]*BRecord, len(listy)*2)
	idx := 0
	for i := 0; i < len(listy); i += 1 {
		b, _ := w.store.getKey(listy[i].key)
		b.mu.RLock()
		locks[idx] = b
		idx++
		rbids[i] = *b.value.(*Bid)
		uk := UserKey(int(rbids[i].Bidder))
		d, _ := w.store.getKey(uk)
		d.mu.RLock()
		rnn[i] = d.value.(*User).Nickname
		locks[idx] = d
		idx++
	}

	// unlock backwards
	for i := len(locks) - 1; i >= 0; i-- {
		locks[i].mu.RUnlock()
	}

	d.mu.RUnlock()
	var r *Result = nil
	if *Allocate {
		r = &Result{0, &struct {
			bids []Bid
			nns  []string
		}{rbids, rnn}, true}
	}
	w.Nstats[RUBIS_BIDHIST]++
	return r
}

// XXX inserts are derived data?
// tx -- read: items, users; writes: comments, users
func (w *Worker) LockTxnStoreComment(toid uint64, fromid uint64, item uint64, com string, rating uint64) *Result {
	tok := UserKey(int(toid))
	torec, err := w.store.getKey(tok)
	if err != nil {
		dlog.Printf("User doesn't exist: %v\n", toid)
		return nil
	}
	torec.mu.Lock()

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
	comrec.mu.Lock()

	newrating := torec.value.(*User).Rating + rating

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
