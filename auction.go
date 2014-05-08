package ddtxn

import (
	"ddtxn/dlog"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

func GetTxns(skewed bool) []float64 {
	perc := make(map[float64]int)
	if skewed {
		perc = map[float64]int{
			10.0: RUBIS_SEARCHCAT,
			10.5: RUBIS_VIEW,
			5.47: RUBIS_SEARCHREG,
			4.97: RUBIS_PUTBID,
			40.0: RUBIS_BID,
			2.13: RUBIS_VIEWUSER,
			1.81: RUBIS_NEWITEM,
			1.8:  RUBIS_REGISTER,
			1.4:  RUBIS_BUYNOW,
			1.34: RUBIS_BIDHIST,
			.55:  RUBIS_PUTCOMMENT,
			.5:   RUBIS_COMMENT,
		}
	} else {
		perc = map[float64]int{
			13.4: RUBIS_SEARCHCAT,
			11.3: RUBIS_VIEW,
			5.47: RUBIS_SEARCHREG,
			4.97: RUBIS_PUTBID,
			3.7:  RUBIS_BID,
			2.13: RUBIS_VIEWUSER,
			1.81: RUBIS_NEWITEM,
			1.8:  RUBIS_REGISTER,
			1.4:  RUBIS_BUYNOW,
			1.34: RUBIS_BIDHIST,
			.55:  RUBIS_PUTCOMMENT,
			.5:   RUBIS_COMMENT,
		}
	}

	var sum float64
	for k, _ := range perc {
		sum += k
	}
	newperc := make(map[int]float64)
	for k, v := range perc {
		newperc[v] = 100 * k / sum
	}

	rates := make([]float64, len(perc))
	sum = 0
	for i := RUBIS_BID; i < RUBIS_BID_OCC; i++ {
		sum = sum + newperc[i]
		rates[i-RUBIS_BID] = sum
	}
	return rates
}

var pages []string

// 27 == Back
// 28 == End

type Transition struct {
	states  [][]float64
	prev    int
	current int
}

func (t *Transition) next() int {
	trans := t.states[t.current]
	end := rand.Float64()
	var cum float64
	var i int
	for i = 0; i < len(trans)-1; i++ {
		cum = cum + trans[i]
		if end < cum {
			break
		}
	}
	// Check for back
	if i == 28 {
		t.current = t.prev
	} else if i == 27 {
		t.current = 0
		t.prev = 0
	} else {
		t.prev = t.current
		t.current = i
	}
	return t.current
}

func parseTransitions(fn string) [][]float64 {
	content, err := ioutil.ReadFile(fn)
	if err != nil {
		log.Fatalf("Can't open transitions %v %v\n", fn, err)
	}
	lines := strings.Split(string(content), "\n")
	states := make([][]float64, 29)
	var st int
	start := false
	for _, line := range lines {
		if strings.HasPrefix(line, "From") {
			start = true
			pages = strings.Split(line, "\t")
			pages = pages[1 : len(pages)-1] // Last is wait time
			for i := range pages {
				states[i] = make([]float64, len(pages)+2)
			}
		} else if strings.HasPrefix(line, "Back probability") {
			probs := strings.Split(line, "\t")
			for i, _ := range pages {
				z, err := strconv.ParseFloat(probs[i+1], 32)
				if err != nil {
					log.Fatalf("err")
				}
				states[i][st] = z
			}
			st++
		} else if strings.HasPrefix(line, "End of Session") {
			probs := strings.Split(line, "\t")
			for i, _ := range pages {
				z, err := strconv.ParseFloat(probs[i+1], 32)
				if err != nil {
					log.Fatalf("err")
				}
				states[i][st] = z
			}
			break
		} else if start {
			probs := strings.Split(line, "\t")
			for i := range pages {
				z, err := strconv.ParseFloat(probs[i+1], 32)
				if err != nil {
					log.Fatalf("err")
				}
				states[i][st] = z
			}
			st++
		}

	}

	// The default transitions in RuBIS sum to > 1.  Oh well.  Doing
	// the same thing the rice code does.
	for i := 0; i < len(states); i++ {
		var sum float64
		for j := 0; j < len(states[i]); j++ {
			sum = sum + states[i][j]
		}
		if i < len(pages) {
			dlog.Println(i, pages[i], sum, "\t", states[i])
		}
	}
	return states
}

func parseTransitions2(fn string) [][]float64 {
	content, err := ioutil.ReadFile(fn)
	if err != nil {
		log.Fatalf("Can't open transitions %v %v\n", fn, err)
	}
	lines := strings.Split(string(content), "\n")
	states := make([][]float64, 29)
	var st int
	start := false
	for _, line := range lines {
		if strings.HasPrefix(line, "|From") {
			pages = strings.Split(line, "|")
			pages = pages[2 : len(pages)-3] // Last is wait time
			for i := range pages {
				states[i] = make([]float64, len(pages)+2)
			}
		} else if strings.HasPrefix(line, "|vvv") {
			continue
		} else if strings.HasPrefix(line, "|\"") {
			start = true
			continue
		} else if strings.HasPrefix(line, "+---") {
			continue
		} else if strings.HasPrefix(line, "|Back probability") {
			probs := strings.Split(line, "|")
			for i, _ := range pages {
				y := strings.TrimSpace(probs[i+2])
				z, err := strconv.ParseFloat(y, 32)
				if err != nil {
					log.Fatalf("Could not parse float err %v %v\n", err, probs[i+1])
				}
				states[i][st] = z
			}
			st++
		} else if strings.HasPrefix(line, "|End of ") {
			probs := strings.Split(line, "|")
			for i, _ := range pages {
				y := strings.TrimSpace(probs[i+2])
				z, err := strconv.ParseFloat(y, 32)
				if err != nil {
					log.Fatalf("Could not parse float2 err %v %v\n", err, probs[i+1])
				}
				states[i][st] = z
			}
			break
		} else if start {
			probs := strings.Split(line, "|")
			for i := range pages {
				y := strings.TrimSpace(probs[i+2])
				z, err := strconv.ParseFloat(y, 32)
				if err != nil {
					log.Fatalf("Could not parse float3 err %v %v\n", err, y)
				}
				states[i][st] = z
			}
			st++
		}

	}

	// The default transitions in RuBIS sum to > 1.  Oh well.  Doing
	// the same thing the rice code does.
	for i := 0; i < len(states); i++ {
		var sum float64
		for j := 0; j < len(states[i]); j++ {
			sum = sum + states[i][j]
		}
		if i < len(pages) {
			dlog.Println(i, pages[i], len(states[i]), "\t", states[i])
		}
	}
	return states
}

func MakeT(states [][]float64) *Transition {
	t := &Transition{
		states:  states,
		current: 0,
		prev:    0,
	}
	return t
}

type User struct {
	ID       uint64
	Name     string
	Nickname string
	Rating   uint64
	Region   uint64
}

type Item struct {
	ID        uint64
	Seller    uint64
	Qty       uint64
	Startdate int
	Enddate   int
	Name      string
	Desc      string
	Sprice    uint64
	Rprice    uint64
	Buynow    uint64
	Dur       uint64
	Categ     uint64
}

type Bid struct {
	ID     uint64
	Item   uint64
	Bidder uint64
	Price  int32
}

type BuyNow struct {
	BuyerID uint64
	ItemID  uint64
	Qty     uint64
	Date    int
}

type Comment struct {
	ID      TID
	From    uint64
	To      uint64
	Rating  uint64
	Item    uint64
	Date    uint64
	Comment string
}

// Base Data
// Users, Items, Bids
//
// Derived Data
// unique used nicknames?
// x max bid
// x max bid seller
// x num bids
// x list of bids per item
// list of bids per user (?)
// per-category list of items
// per-category-region list of items

func (w *Worker) TxnRegisterUser(nickname string, region uint64) *Result {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	n := w.nextTID()
	nick := SKey(nickname)
	user := &User{
		ID:       uint64(n),
		Name:     Randstr(10),
		Nickname: nickname,
		Region:   region,
	}
	u := UserKey(int(n))
	nick_rec, err := w.store.getKey(nick)
	if err != ENOKEY {
		// Someone else is using this nickname
		dlog.Printf("Nickname taken %v %v\n", nickname, nick)
		return r
	}
	user_rec := w.store.getOrCreateKey(u)
	if !user_rec.Lock() {
		dlog.Printf("Can't lock user %v %v\n", nickname, u)
		return r
	}

	nick_rec = w.store.getOrCreateKey(nick)
	if !nick_rec.Lock() {
		user_rec.Unlock(0)
		dlog.Printf("Can't lock nickname %v %v\n", nickname, nick)
		return r
	}
	e := w.epoch
	txid := e | n
	user_rec.value = user
	nick_rec.value = nickname
	nick_rec.Unlock(txid)
	user_rec.Unlock(txid)
	if *Allocate {
		r.V = uint64(n)
		r.C = true
		dlog.Printf("Registered user %v %v\n", nickname, n)
	}
	w.Nstats[RUBIS_REGISTER]++
	return r
}

func (w *Worker) TxnNewItem(seller uint64, name string, desc string, sprice uint64,
	rprice uint64, bnow uint64, duration uint64, quan uint64, endd int, categ uint64) *Result {

	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	//who cares
	now := time.Now().Second()
	//now := 1

	n := w.nextTID()
	xx := uint64(n)
	x := &Item{
		ID:        xx,
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
	un := ItemKey(xx)
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

	regk := ItemsByRegKey(region, categ)

	if !unr.Lock() {
		return nil
	}

	y := w.store.getOrCreateTypedKey(ibck, nil, LIST)
	if !y.Lock() {
		unr.Unlock(0)
		w.Naborts++
		return r
	}

	y2 := w.store.getOrCreateTypedKey(regk, nil, LIST)
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

	// Lock on item is essentially lock on this derived data for now
	w.store.getOrCreateTypedKey(MaxBidKey(xx), int32(0), MAX)
	w.store.getOrCreateTypedKey(MaxBidBidderKey(xx), uint64(0), WRITE)
	w.store.getOrCreateTypedKey(NumBidsKey(xx), int32(0), SUM)
	w.store.getOrCreateTypedKey(BidsPerItemKey(xx), nil, LIST)

	y2.Unlock(txid)
	y.Unlock(txid)
	unr.Unlock(txid)

	//_, ok = w.derived_list[ibck]
	//if !ok {
	//	w.derived_list[ibck] = make([]Entry, 1, DEFAULT_LIST_SIZE)
	//  w.derived_list[ibck][0] = Entry{order: now, top: int(n)}
	//} else {
	//	AddToList(ibck, w, Entry{order: now, top: int(n)})
	//	dlog.Printf("Added %v to %v\n", ibck, w.derived_list[ibck])
	//}

	// _, ok = w.derived_list[regk]
	// if !ok {
	// 	w.derived_list[regk] = make([]Entry, 1, DEFAULT_LIST_SIZE)
	// 	w.derived_list[regk][0] = Entry{order: now, top: int(n)}
	// } else {
	// 	AddToList(regk, w, Entry{order: now, top: int(n)})
	//}

	if *Allocate {
		r = &Result{n, xx, true}
		dlog.Printf("Registered item %v cat %v\n", n, categ)
	}
	w.Nstats[RUBIS_NEWITEM]++
	return r
}

func (w *Worker) TxnStoreBid(user, item uint64, price int32) *Result {
	dlog.Printf("storing bid\n")
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
	xx := uint64(n)
	xx = uint64(user)
	un := BidKey(xx)
	bidr := w.store.getOrCreateKey(un)

	// update max bid?
	high := MaxBidKey(item)
	bidder := MaxBidBidderKey(item)

	update := false
	global := false
	var m2 *BRecord
	var m *BRecord
	var last uint64
	if *Dynamic && *SysType == DOPPEL {
		w.ddmu.RLock()
		ok, _ := w.dd[high]
		w.ddmu.RUnlock()
		if !ok {
			dlog.Printf("Key is not derived data %v\n", high)
			// if this key is not derived data, then don't update it
			// locally
			global = true
			m = w.store.getOrCreateTypedKey(high, int32(0), MAX)
			ok, last = m.IsUnlocked()
			if !ok {
				w.Naborts++
				return r
			}
			// check to see if we're the max; start locking
			if price > m.int_value {
				update = true
				if !m.Lock() {
					w.Naborts++
					return r
				}
				// if we're max update bidder too
				m2 = w.store.getOrCreateTypedKey(bidder, uint64(0), WRITE)
				if !m2.Lock() {
					w.Naborts++
					m.Unlock(0)
					return r
				}
			}
		} else {
			dlog.Printf("Key is  derived data %v\n", high)
			// key is derived data
			x, ok := w.derived_max[high]
			if !ok || x < price {
				w.derived_max[high] = price
				w.derived_bw[bidder] = user
			}
		}
	} else {
		x, ok := w.derived_max[high]
		if !ok || x < price {
			w.derived_max[high] = price
			w.derived_bw[bidder] = user
		}
	}

	if !bidr.Lock() {
		if global && update {
			m2.Unlock(0)
			m.Unlock(0)
		}
		return r
	}

	e := w.epoch
	txid := e | n

	if global && !update {
		if !m.Verify(last) {
			bidr.Unlock(0)
			w.Naborts++
			return r
		}
	}

	if global && update {
		m.int_value = price
		m2.value = user
	}
	bidr.value = bid
	bidr.Unlock(txid)
	if global && update {
		m2.Unlock(txid)
		m.Unlock(txid)
	}

	// update # bids per item
	num := NumBidsKey(item)
	_, ok := w.derived_sums[num]
	if !ok {
		w.derived_sums[num] = 1
	} else {
		w.derived_sums[num] += 1
	}

	// add to item's bid list
	bids := BidsPerItemKey(item)
	_, ok = w.derived_list[bids]
	if !ok {
		w.derived_list[bids] = make([]Entry, 1, DEFAULT_LIST_SIZE)
		w.derived_list[bids][0] = Entry{int(bid.Price), un, 0}
	} else {
		AddToList(bids, w, Entry{int(bid.Price), un, 0})
	}
	if *Allocate {
		r = &Result{txid, uint64(n), true}
		dlog.Printf("%v Bid on %v %v\n", user, item, price)
	}
	w.Nstats[RUBIS_BID]++
	return r
}

// tx -- read: item, bidsperitem, writes:
func (w *Worker) TxnViewItem(id uint64, rophase bool) *Result {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	it := ItemKey(id)
	v, err := w.store.getKey(it)
	if err != nil {
		dlog.Printf("No such item %v\n", it)
		return r
	}
	mbk := MaxBidKey(id)
	mbs := MaxBidBidderKey(id)
	var d1 *BRecord
	var d2 *BRecord

	if rophase {
		d1, err = w.store.getKeyStatic(mbk)
		if err != nil {
			d1 = w.store.getOrCreateTypedKey(mbk, int32(0), MAX)
		}
		d2, err = w.store.getKeyStatic(mbs)
		if err != nil {
			d2 = w.store.getOrCreateTypedKey(mbs, uint64(0), WRITE)
		}
	} else {
		d1 = w.store.getOrCreateTypedKey(mbk, int32(0), MAX)
		d2 = w.store.getOrCreateTypedKey(mbs, uint64(0), WRITE)
	}
	ok, last := d1.IsUnlocked()
	max_bid := d1.int_value

	ok2, last2 := d1.IsUnlocked()
	bidder := d2.value
	v1, _ := w.store.getKey(it)
	ok3, last3 := v1.IsUnlocked()

	if !ok || !ok2 || !ok3 {
		dlog.Printf("Aborting, locked\n")
		w.Naborts++
		return r
		// abort
	}
	if !d1.Verify(last) || !d2.Verify(last2) || !v1.Verify(last3) {
		dlog.Printf("Aborting, validate\n")
		return nil
	}

	// User info for seller, bidder?
	if *Allocate {
		// XXX
		iret := *v.value.(*Item)
		r = &Result{0, &struct {
			Item
			int32
			uint64
		}{iret, max_bid, bidder.(uint64)}, true}
	}
	w.Nstats[RUBIS_VIEW]++
	return r
}

// tx -- read: item, bidsperitem, writes:
func (w *Worker) TxnViewBidHistory(item uint64, rophase bool) *Result {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	it := ItemKey(item)
	_, err := w.store.getKey(it)
	if err != nil {
		dlog.Printf("No such item %v\n", item)
		return r
	}

	bids := BidsPerItemKey(item)
	d, err := w.store.getKey(bids)
	if err != nil {
		dlog.Printf("No bids on %v\n", item)
		return r
	}
	ok, last := d.IsUnlocked()
	if !ok {
		dlog.Printf("Bid index locked\n")
		w.Naborts++
		return r
	}
	listy := d.entries

	var f func(Key) (*BRecord, error)
	if rophase {
		f = w.store.getKeyStatic
	} else {
		f = w.store.getKey
	}

	rbids := make([]Bid, len(listy))
	rnn := make([]string, len(listy))

	w.lasts = w.lasts[:len(listy)*2]
	w.recs = w.recs[:len(listy)*2]

	for i := 0; i < len(listy); i += 1 {
		b, _ := f(listy[i].key)
		ok, last := b.IsUnlocked()
		if !ok {
			w.Naborts++
			return r
		}
		rbids[i] = *b.value.(*Bid)
		w.lasts[i] = last
		w.recs[i] = b
		uk := UserKey(int(rbids[i].Bidder))
		d, err := f(uk)
		if err != nil {
			dlog.Printf("Bidder of this item doesn't exist %v %v %v\n", uk, item, rbids[i])
			w.Naborts++
			return r
		}
		ok2, last2 := d.IsUnlocked()
		if !ok2 {
			w.Naborts++
			return r
		}
		rnn[i] = d.value.(*User).Nickname
		w.lasts[i+len(listy)] = last2
		w.recs[i+len(listy)] = d
	}

	// validate
	for i := 0; i < len(listy); i += 1 {
		b := w.recs[i]
		if !b.Verify(w.lasts[i]) {
			w.Naborts++
			return r
		}
		d := w.recs[len(listy)+i]
		if !d.Verify(w.lasts[i+len(listy)]) {
			w.Naborts++
			return r
		}
	}

	if !d.Verify(last) {
		w.Naborts++
		return r
	}

	if *Allocate {
		r.T = 0
		r.V = &struct {
			bids []Bid
			nns  []string
		}{rbids, rnn}
		r.C = true
	}
	w.Nstats[RUBIS_BIDHIST]++
	return r
}

// tx -- read: users, writes:
func (w *Worker) TxnViewUserInfo(id uint64) *Result {
	uk := UserKey(int(id))
	d, err := w.store.getKey(uk)
	if err != nil {
		dlog.Printf("No such user %v\n", id)
		return nil
	}

	var r *Result = nil
	if *Allocate {
		r = &Result{0, d.value, true}
	}
	w.Nstats[RUBIS_VIEWUSER]++
	return r
}

func (w *Worker) AboutMe() {

}

// tx -- read: items, writes: items buynow
func (w *Worker) TxnStoreBuyNow(user uint64, item uint64, qty uint64) *Result {

	//who cares
	//now := time.Now().Second()
	now := 1
	bnrec := &BuyNow{
		BuyerID: user,
		ItemID:  item,
		Qty:     qty,
		Date:    now,
	}

	uk := UserKey(int(user))
	_, err := w.store.getKey(uk)
	if err != nil {
		dlog.Printf("No such user %v\n", user)
		return nil
	}

	ik := ItemKey(item)
	id, err := w.store.getKey(ik)
	if err != nil {
		dlog.Printf("No such item %v\n", item)
		return nil
	}
	if !id.Lock() {
		dlog.Printf("Item locked %v\n", item)
		return nil
	}

	n := w.nextTID()

	bnk := BuyNowKey(uint64(n))
	bn := w.store.getOrCreateKey(bnk)
	if !bn.Lock() {
		id.Unlock(0)
		dlog.Printf("Buy now key locked %v\n", bnk)
		return nil
	}

	itemrec := id.value.(*Item)
	maxqty := itemrec.Qty
	newq := maxqty - qty

	if maxqty < qty {
		dlog.Printf("Req. quantity > quantity %v %v\n", qty, maxqty)
		bn.Unlock(0)
		id.Unlock(0)
		return nil
	}

	e := w.epoch
	txid := e | n
	bn.value = bnrec

	if newq == 0 {
		itemrec.Enddate = now
		itemrec.Qty = 0
	} else {
		itemrec.Qty = newq
	}

	bn.Unlock(txid)
	id.Unlock(txid)

	var r *Result = nil
	if *Allocate {
		r = &Result{txid, qty, true}
	}
	w.Nstats[RUBIS_BUYNOW]++
	return r
}

// tx -- read: items, maxbid
func (w *Worker) TxnPutBid(item uint64) *Result {
	// "putbid" is a terribly misleading name since it doesn't bid. i'll
	// leave it be though to keep the names in synch with the reference
	// rubis code.

	ik := ItemKey(item)
	ir, err := w.store.getKey(ik)
	if err != nil {
		dlog.Printf("Item doesn't exist: %v\n", item)
		return nil
	}

	uk := UserKey(int(ir.value.(*Item).Seller))
	urec, err := w.store.getKey(uk)
	if err != nil {
		dlog.Printf("User doesn't exist: %v\n", ir.value.(*Item).Seller)
		return nil
	}
	ok, last := urec.IsUnlocked()

	nick := urec.value.(*User).Nickname

	maxbk := MaxBidKey(item)
	maxbrec, err := w.store.getKey(maxbk)
	if err != nil {
		dlog.Printf("No max bid for item: %v\n", item)
		return nil
	}
	ok2, last2 := maxbrec.IsUnlocked()

	maxb := maxbrec.int_value

	bck := BidsPerItemKey(item)
	birec, err := w.store.getKey(bck)
	if err != nil {
		dlog.Printf("No bid list for item: %v\n", item)
		return nil
	}
	ok3, last3 := birec.IsUnlocked()
	bc := len(birec.entries)

	if !ok || !ok2 || !ok3 {
		// abort
		return nil
	}

	// validate
	if !urec.Verify(last) || !maxbrec.Verify(last2) || !birec.Verify(last3) {
		return nil
	}
	var r *Result = nil
	if *Allocate {
		r = &Result{0, &struct {
			string
			int32
			int
		}{nick, maxb, bc}, true}
	}
	w.Nstats[RUBIS_PUTBID]++
	return r
}

// tx -- read: items, users
func (w *Worker) TxnPutComment(toid uint64, item uint64) *Result {
	tok := UserKey(int(toid))
	torec, err := w.store.getKey(tok)
	if err != nil {
		dlog.Printf("User doesn't exist: %v\n", toid)
		return nil
	}

	ok1, last1 := torec.IsUnlocked()

	nickname := torec.value.(*User).Nickname

	itk := ItemKey(item)
	itrec, err := w.store.getKey(itk)
	if err != nil {
		dlog.Printf("Item doesn't exist: %v\n", itk)
		return nil
	}

	ok2, last2 := itrec.IsUnlocked()

	itemname := itrec.value.(*Item).Name

	if !ok1 || !ok2 {
		return nil
	}

	if !torec.Verify(last1) || !itrec.Verify(last2) {
		return nil
	}

	var r *Result = nil
	if *Allocate {
		r = &Result{0, &struct {
			nick  string
			iname string
		}{nickname, itemname}, true}
	}
	w.Nstats[RUBIS_PUTCOMMENT]++
	return r
}

// XXX inserts are derived data?
// tx -- read: items, users; writes: comments, users
func (w *Worker) TxnStoreComment(toid uint64, fromid uint64, item uint64, com string, rating uint64) *Result {
	n := w.nextTID()
	comk := CommentKey(uint64(n))
	_, err := w.store.getKey(comk)
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

	if !comrec.Lock() {
		w.Naborts++
		return nil
	}

	rkey := RatingKey(toid)

	_, ok := w.derived_sums[rkey]
	if !ok {
		w.derived_sums[rkey] = int32(rating)
	} else {
		w.derived_sums[rkey] += int32(rating)
	}
	e := w.epoch
	txid := e | n

	comrec.value = ncom

	comrec.Unlock(txid)

	var r *Result = nil
	if *Allocate {
		r = &Result{n, nil, true}
	}
	w.Nstats[RUBIS_COMMENT]++
	return r
}

func (w *Worker) TxnSearchItemsCateg(categ uint64, num uint64) *Result {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	if num > 10 {
		log.Fatalf("Only 10 search items are currently supported.\n")
	}

	ibck := ItemsByCatKey(categ)

	ibcrec, err := w.store.getKey(ibck)
	if err != nil {
		dlog.Printf("No index for category %v\n", ibck)
		return r
	}
	ok, last := ibcrec.IsUnlocked()
	if !ok {
		dlog.Printf("index locked")
		return r
	}

	if err != nil {
		dlog.Printf("Category doesn't exist %v\n", categ)
		return r
	}

	listy := ibcrec.entries

	if len(listy) > 10 {
		dlog.Printf("Only 10 search items are currently supported %v %v\n", len(listy), listy)
	}

	var ret []*Item
	var maxb []int32
	var numb []int32

	if *Allocate {
		ret = make([]*Item, len(listy))
		maxb = make([]int32, len(listy))
		numb = make([]int32, len(listy))
	}
	lasts := w.lasts[:len(listy)*3]
	recs := w.recs[:len(listy)*3]

	for i := 0; i < len(listy); i += 1 {
		k := uint64(listy[i].top)
		xx := ItemKey(k)
		recs[i], err = w.store.getKey(xx)
		if err != nil {
			dlog.Printf("Item in list doesn't exist %v\n", k)
			return r
		}
		ok := false
		ok, lasts[i] = recs[i].IsUnlocked()
		if !ok {
			w.Naborts++
			return r
		}
		if *Allocate {
			ret[i] = recs[i].Value().(*Item)
		}

		mb := MaxBidKey(k)
		recs[i+len(listy)], err = w.store.getKey(mb)
		if err != nil {
			dlog.Printf("No max bid key %v\n", k)
		} else {
			ok, lasts[i+len(listy)] = recs[i+len(listy)].IsUnlocked()
			if !ok {
				w.Naborts++
				return r
			}
			if *Allocate {
				maxb[i] = recs[i+len(listy)].int_value
			}
		}

		nbb := NumBidsKey(k)
		recs[i+2*len(listy)], err = w.store.getKey(nbb)
		if err != nil {
			dlog.Printf("No number of bids key %v\n", k)
			recs[i+2*len(listy)] = nil
		} else {
			ok, lasts[i+2*len(listy)] = recs[i+2*len(listy)].IsUnlocked()
			if !ok {
				w.Naborts++
				return r
			}
			if *Allocate {
				numb[i] = recs[i+2*len(listy)].int_value
			}
		}
	}

	for i := 0; i < len(recs); i += 1 {
		if recs[i] != nil {
			if !recs[i].Verify(lasts[i]) {
				w.Naborts++
				return r
			}
		}
	}
	if !ibcrec.Verify(last) {
		w.Naborts++
		return r
	}
	w.lasts = w.lasts[:0]
	w.recs = w.recs[:0]
	//dlog.Printf("Category search %v ", categ)
	//for i := 0; i < len(ret); i++ {
	//	dlog.Printf("%v %v", *ret[i], numb[i])
	//}
	//dlog.Printf("\n")

	if *Allocate {
		r = &Result{
			0,
			&struct {
				items   []*Item
				maxbids []int32
				numbids []int32
			}{ret, maxb, numb},
			true,
		}
	}
	w.Nstats[RUBIS_SEARCHCAT]++
	return r
}

func (w *Worker) TxnSearchItemsReg(region uint64, categ uint64, num uint64) *Result {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	if num > 10 {
		log.Fatalf("Only 10 search items are currently supported.\n")
	}
	ibrk := ItemsByRegKey(region, categ)
	ibrrec, err := w.store.getKey(ibrk)

	if err != nil {
		dlog.Printf("Region doesn't exist %v\n", categ)
		return nil
	}

	listy := ibrrec.entries

	var ret []*Item
	var maxb []int32
	var numb []int32

	if *Allocate {
		ret = make([]*Item, len(listy))
		maxb = make([]int32, len(listy))
		numb = make([]int32, len(listy))
	}
	lasts := w.lasts[:len(listy)*3]
	recs := w.recs[:len(listy)*3]

	for i := 0; i < len(listy); i += 1 {
		k := uint64(listy[i].top)
		xx := ItemKey(k)
		recs[i], err = w.store.getKey(xx)
		if err != nil {
			dlog.Printf("Item in list doesn't exist %v\n", k)
			return r
		}
		ok := false
		ok, lasts[i] = recs[i].IsUnlocked()
		if !ok {
			return r
		}
		if *Allocate {
			ret[i] = recs[i].value.(*Item)
		}

		mb := MaxBidKey(k)
		recs[i+len(listy)], err = w.store.getKey(mb)
		if err != nil {
			dlog.Printf("No max bid key %v\n", k)
			return r
		}
		ok, lasts[i+len(listy)] = recs[i+len(listy)].IsUnlocked()
		if !ok {
			return r
		}
		if *Allocate {
			maxb[i] = recs[i+len(listy)].int_value
		}

		nbb := NumBidsKey(k)
		recs[i+2*len(listy)], err = w.store.getKey(nbb)
		if err != nil {
			dlog.Printf("No number of bids key %v\n", k)
			return r
		}
		ok, lasts[i+2*len(listy)] = recs[i+2*len(listy)].IsUnlocked()
		if !ok {
			return r
		}
		if *Allocate {
			numb[i] = recs[i+2*len(listy)].int_value
		}
	}

	for i := 0; i < len(recs); i += 1 {
		if !recs[i].Verify(lasts[i]) {
			return r
		}
	}

	w.lasts = w.lasts[:0]
	w.recs = w.recs[:0]
	if *Allocate {
		r.T = 0
		r.V = &struct {
			items   []*Item
			maxbids []int32
			numbids []int32
		}{ret, maxb, numb}
		r.C = true
	}
	w.Nstats[RUBIS_SEARCHREG]++
	return r
}
