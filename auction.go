package ddtxn

import (
	"ddtxn/dlog"
	"time"
)

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

func RegisterUserTxn(t Query, w *Worker) (*Result, error) {
	nickname := t.S1
	region := t.U1

	var r *Result = nil
	n := w.nextTID()
	nick := SKey(nickname)
	user := &User{
		ID:       uint64(n),
		Name:     Randstr(10),
		Nickname: nickname,
		Region:   region,
	}
	u := UserKey(int(n))
	tx := w.ctxn
	_, err := tx.Read(nick)

	if err != ENOKEY {
		// Someone else is using this nickname
		dlog.Printf("Nickname taken %v %v\n", nickname, nick)
		return nil, EABORT
	}
	tx.Write(u, user, WRITE)
	tx.Write(nick, nickname, WRITE)

	if tx.Commit() == 0 {
		return nil, EABORT
	}
	if *Allocate {
		r = &Result{uint64(n), true}
		dlog.Printf("Registered user %v %v\n", nickname, n)
	}
	w.Nstats[RUBIS_REGISTER]++
	return r, nil
}

func NewItemTxn(t Query, w *Worker) (*Result, error) {
	var r *Result = nil
	now := time.Now().Second()

	tx := w.ctxn
	n := w.nextTID()
	xx := uint64(n)
	x := &Item{
		ID:        xx,
		Name:      t.S1,
		Seller:    t.U1,
		Desc:      t.S2,
		Sprice:    t.U2,
		Rprice:    t.U3,
		Buynow:    t.U4,
		Dur:       t.U5,
		Qty:       t.U6,
		Startdate: now,
		Enddate:   t.I,
		Categ:     t.U7,
	}
	urec, err := tx.Read(UserKey(int(t.U1)))
	if err != nil {
		dlog.Printf("User doesn't exist %v\n", t.U1)
		tx.Abort()
		return nil, EABORT
	}
	region := urec.value.(*User).Region
	val := Entry{order: now, top: int(n)}
	tx.Write(ItemKey(xx), x, WRITE)
	tx.Write(ItemsByCatKey(x.Categ), val, LIST)
	tx.Write(ItemsByRegKey(region, x.Categ), val, LIST)
	tx.WriteInt32(MaxBidKey(xx), int32(0), MAX)
	tx.Write(MaxBidBidderKey(xx), uint64(0), WRITE)
	tx.WriteInt32(NumBidsKey(xx), int32(0), SUM)
	tx.Write(BidsPerItemKey(xx), nil, LIST)

	if tx.Commit() == 0 {
		return r, EABORT
	}

	if *Allocate {
		r = &Result{xx, true}
		dlog.Printf("Registered item %v cat %v\n", n, x.Categ)
	}
	w.Nstats[RUBIS_NEWITEM]++
	return r, nil
}

func StoreBidTxn(t Query, w *Worker) (*Result, error) {
	var r *Result = nil
	user := t.U1
	item := t.U2
	price := t.A
	// insert bid
	n := w.nextTID()
	bid := &Bid{
		ID:     uint64(n),
		Item:   item,
		Bidder: user,
		Price:  price,
	}
	tx := w.ctxn
	bid_key := BidKey(uint64(n))
	tx.Write(bid_key, bid, WRITE)

	// update max bid?
	high := MaxBidKey(item)
	bidder := MaxBidBidderKey(item)
	max, err := tx.Read(high)
	if err == ENOKEY {
		dlog.Printf("No max key for item? %v\n", item)
		return nil, EABORT
	}
	if price > max.Value().(int32) {
		tx.WriteInt32(high, price, MAX)
		tx.Write(bidder, user, WRITE)
	}

	// update # bids per item
	tx.WriteInt32(NumBidsKey(item), 1, SUM)

	// add to item's bid list
	tx.Write(BidsPerItemKey(item), Entry{int(bid.Price), bid_key, 0}, LIST)

	if tx.Commit() == 0 {
		dlog.Printf("Bid abort %v\n", t)
		return r, EABORT
	}

	if *Allocate {
		r = &Result{uint64(n), true}
		dlog.Printf("%v Bid on %v %v\n", user, item, price)
	}
	w.Nstats[RUBIS_BID]++
	return r, nil
}
