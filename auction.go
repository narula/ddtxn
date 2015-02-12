package ddtxn

import (
	"fmt"
	"log"
	"time"

	"github.com/narula/dlog"
)

const (
	NUM_USERS      = 1000000
	NUM_CATEGORIES = 20
	NUM_REGIONS    = 62
	NUM_ITEMS      = 533000
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
	ID      uint64
	From    uint64
	To      uint64
	Rating  uint64
	Item    uint64
	Date    uint64
	Comment string
}

func RegisterUserTxn(t Query, tx ETransaction) (*Result, error) {
	region := t.U1
	nickname := t.U2
	var r *Result = nil

	var n uint64
	var nick Key

	if !*Allocate || nickname == 0 {
		n = tx.UID('u')
		nick = NicknameKey(tx.UID('d'))
	} else {
		n = nickname
		nick = NicknameKey(n)
	}
	u := UserKey(n)
	user := &User{
		ID:       n,
		Name:     "xxxxxxx",
		Nickname: string(nickname),
		Region:   region,
	}
	tx.MaybeWrite(nick)
	br, err := tx.Read(nick)
	var val uint64 = 0
	if br != nil && br.exists {
		val = br.Value().(uint64)
	}

	if err != ENOKEY && val != 0 {
		// Someone else is using this nickname
		dlog.Printf("Nickname taken %v %v\n", nickname, nick)
		tx.Abort()
		return nil, ENORETRY
	}
	tx.Write(u, user, WRITE)
	tx.Write(nick, nickname, WRITE)

	if tx.Commit() == 0 {
		dlog.Printf("RegisterUser() Abort\n")
		return nil, EABORT
	}
	if *Allocate {
		r = &Result{uint64(n)}
		// dlog.Printf("Registered user %v %v\n", nickname, n)
	}
	return r, nil
}

func NewItemTxn(t Query, tx ETransaction) (*Result, error) {
	var r *Result = nil
	now := time.Now().Second()
	var n uint64
	if t.T != 0 {
		n = uint64(t.T)
	} else {
		n = tx.UID('i')
	}
	item := ItemKey(n)
	x := &Item{
		ID:        n,
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
	urec, err := tx.Read(UserKey(t.U1))
	if err != nil {
		if err == ESTASH {
			dlog.Printf("User stashed %v\n", UserKey(t.U1))
			return nil, ESTASH
		} else if err == EABORT {
			return nil, EABORT
		} else if err == ENOKEY {
			fmt.Printf("NewItemTxn(): User doesn't exist %v\n", t.U1)
			if tx.Commit() == 0 {
				return nil, EABORT
			} else {
				return nil, ENORETRY
			}
		} else {
			log.Fatalf("err: %v\n", err)
		}
	}
	region := urec.value.(*User).Region
	val := Entry{order: now, top: int(n), key: ItemKey(n)}
	tx.Write(item, x, WRITE)
	err = tx.WriteList(ItemsByCatKey(x.Categ), val, LIST)
	if err != nil {
		tx.Abort()
		dlog.Printf("NewItemTxn(): Error putting item in cat list %v! %v %v\n", n, x.Categ, err)
		return nil, err
	}
	err = tx.WriteList(ItemsByRegKey(region, x.Categ), val, LIST)
	if err != nil {
		tx.Abort()
		dlog.Printf("NewItemTxn(): Error putting item in reg list %v! %v %v\n", n, region, err)
		return nil, err
	}
	err = tx.WriteInt32(NumBidsKey(n), int32(0), SUM)
	if err != nil {
		tx.Abort()
		dlog.Printf("NewItemTxn(): Error writing num bids for item %v! %v\n", n, err)
		return nil, err
	}
	err = tx.WriteInt32(MaxBidKey(n), int32(0), MAX)
	if err != nil {
		tx.Abort()
		dlog.Printf("NewItemTxn(): Error creating new item %v! %v\n", n, err)
		return nil, err
	}
	err = tx.WriteOO(MaxBidBidderKey(n), 0, uint64(0), OOWRITE)
	if err != nil {
		tx.Abort()
		dlog.Printf("NewItemTxn(): Error writing max bidder for item %v! %v\n", n, err)
		return nil, err
	}
	if tx.Commit() == 0 {
		dlog.Printf("NewItemTxn(): Abort item %v!\n", n)
		return r, EABORT
	}

	if *Allocate {
		r = &Result{n}
		//dlog.Printf("Registered new item %v %v\n", x, n)
	}
	return r, nil
}

// TODO: Check and see if I need more tx.MaybeWrite()s
func StoreBidTxn(t Query, tx ETransaction) (*Result, error) {
	var r *Result = nil
	user := t.U1
	item := t.U2
	price := int32(t.U3)
	if price < 0 {
		log.Fatalf("price %v %v", price, t.U3)
	}
	// insert bid
	n := tx.UID('b')
	bid_key := BidKey(n)
	bid := &Bid{
		ID:     uint64(n),
		Item:   item,
		Bidder: user,
		Price:  price,
	}
	tx.Write(bid_key, bid, WRITE)

	// update # bids per item
	err := tx.WriteInt32(NumBidsKey(item), 1, SUM)
	if err != nil {
		tx.RelinquishKey(n, 'b')
		tx.Abort()
		dlog.Printf("StoreBidTxn(): Couldn't write numbids for item %v; %v\n", item, err)
		return nil, err
	}

	// update max bid?
	high := MaxBidKey(item)
	tx.MaybeWrite(high)
	err = tx.WriteInt32(high, price, MAX)
	if err != nil {
		tx.RelinquishKey(n, 'b')
		dlog.Println("Aborting because of max")
		tx.Abort()
		dlog.Printf("StoreBidTxn(): Couldn't write maxbid for item %v; %v\n", item, err)
		return nil, err
	}
	bidder := MaxBidBidderKey(item)
	err = tx.WriteOO(bidder, price, user, OOWRITE)
	if err != nil {
		tx.RelinquishKey(n, 'b')
		dlog.Println("Aborting because of max oowrite")
		tx.Abort()
		dlog.Printf("StoreBidTxn(): Couldn't write maxbidder for item %v; %v\n", item, err)
		return nil, err
	}
	// add to item's bid list
	e := Entry{int(bid.Price), bid_key, 0}
	err = tx.WriteList(BidsPerItemKey(item), e, LIST)
	if err != nil {
		tx.RelinquishKey(n, 'b')
		tx.Abort()
		dlog.Printf("StoreBidTxn(): Error adding to bids per item key %v! %v\n", item, err)
		return nil, err
	}
	if tx.Commit() == 0 {
		tx.RelinquishKey(n, 'b')
		dlog.Printf("StoreBidTxn(): Abort item %v\n", item)
		return r, EABORT
	}

	if *Allocate {
		r = &Result{uint64(n)}
		// dlog.Printf("User %v Bid on item %v for %v dollars\n", user, item, price)
	}
	return r, nil
}

func StoreCommentTxn(t Query, tx ETransaction) (*Result, error) {
	touser := t.U1
	fromuser := t.U2
	item := t.U3
	comment_s := t.S1
	rating := t.U4

	n := tx.UID('c')
	com := CommentKey(n)
	comment := &Comment{
		ID:      n,
		From:    fromuser,
		To:      touser,
		Rating:  rating,
		Comment: comment_s,
		Item:    item,
		Date:    11,
	}
	tx.Write(com, comment, WRITE)

	rkey := RatingKey(touser)
	err := tx.WriteInt32(rkey, int32(rating), SUM)
	if err != nil {
		dlog.Printf("Comment abort %v\n", t)
		tx.Abort()
		return nil, err
	}

	if tx.Commit() == 0 {
		dlog.Printf("Comment abort %v\n", t)
		return nil, EABORT
	}
	var r *Result = nil
	if *Allocate {
		r = &Result{uint64(n)}
		dlog.Printf("%v Comment %v %v\n", touser, fromuser, item)
	}
	return r, nil
}

func StoreBuyNowTxn(t Query, tx ETransaction) (*Result, error) {
	now := 1
	user := t.U1
	item := t.U2
	qty := t.U3
	bnrec := &BuyNow{
		BuyerID: user,
		ItemID:  item,
		Qty:     qty,
		Date:    now,
	}
	uk := UserKey(t.U1)
	br, err := tx.Read(uk)
	if err != nil {
		if err == ESTASH {
			dlog.Printf("User  %v stashed\n", t.U1)
			return nil, ESTASH
		} else if err == EABORT {
			return nil, EABORT
		} else if err == ENOKEY {
			dlog.Printf("StoreBuyNowTxn(): No user? %v\n", t.U1)
			if tx.Commit() == 0 {
				return nil, EABORT
			} else {
				return nil, ENORETRY
			}
		} else {
			log.Fatalf("err: %v\n", err)
		}
	}
	_ = br.Value().(*User)
	ik := ItemKey(item)
	tx.MaybeWrite(ik)
	irec, err := tx.Read(ik)
	if err != nil {
		if err == ESTASH {
			dlog.Printf("StoreBuyNowTxn(): Item key  %v stashed\n", item)
			return nil, ESTASH
		} else if err == EABORT {
			return nil, EABORT
		} else if err == ENOKEY {
			dlog.Printf("StoreBuyNowTxn(): No item? %v\n", item)
			if tx.Commit() == 0 {
				return nil, EABORT
			} else {
				return nil, ENORETRY
			}
		} else {
			log.Fatalf("err: %v\n", err)
		}
	}
	itemv := irec.Value().(*Item)
	maxqty := itemv.Qty
	newq := maxqty - qty

	if maxqty < qty {
		dlog.Printf("StoreBuyNowTxn(): Req quantity > quantity %v %v\n", qty, maxqty)
		tx.Abort()
		return nil, ENORETRY
	}
	bnk := BuyNowKey(tx.UID('k'))
	tx.Write(bnk, bnrec, WRITE)

	if newq == 0 {
		itemv.Enddate = now
		itemv.Qty = 0
	} else {
		itemv.Qty = newq
	}

	tx.Write(ik, itemv, WRITE)
	if tx.Commit() == 0 {
		return nil, EABORT
	}

	var r *Result = nil
	if *Allocate {
		r = &Result{qty}
	}
	return r, nil
}

func ViewBidHistoryTxn(t Query, tx ETransaction) (*Result, error) {
	item := t.U1
	ik := ItemKey(item)
	br, err := tx.Read(ik)
	if err != nil {
		if err == ESTASH {
			dlog.Printf("Item key  %v stashed\n", item)
			return nil, ESTASH
		} else if err == EABORT {
			return nil, EABORT
		} else if err == ENOKEY {
			dlog.Printf("ViewBidTxn: No item? %v err: %v\n", item, err)
			if tx.Commit() == 0 {
				return nil, EABORT
			} else {
				return nil, ENORETRY
			}
		} else {
			log.Fatalf("view bid err %v\n", err)
		}
	}

	_ = br.Value().(*Item)
	bids := BidsPerItemKey(item)
	brec, err := tx.Read(bids)
	if err != nil {
		if err == ESTASH {
			dlog.Printf("BidsPerItem key  %v stashed\n", item)
			return nil, ESTASH
		} else if err == EABORT {
			return nil, EABORT
		} else if err == ENOKEY {
			dlog.Printf("No bids for item %v\n", item)
			if tx.Commit() == 0 {
				return nil, EABORT
			} else {
				return nil, nil
			}
		} else {
			log.Fatalf("err %v\n", err)
		}
	}
	listy := brec.entries

	var rbids []Bid
	var rnn []string

	if *Allocate {
		rbids = make([]Bid, len(listy))
		rnn = make([]string, len(listy))
	}

	for i := 0; i < len(listy); i++ {
		b, err := tx.Read(listy[i].key)
		if err != nil {
			if err == ESTASH {
				dlog.Printf("ViewBidHist() key stashed %v\n", listy[i].key)
				return nil, ESTASH
			} else if err == EABORT {
				return nil, EABORT
			} else if err == ENOKEY {
				dlog.Printf("ViewBidHist() No such key %v\n", listy[i].key)
				if tx.Commit() == 0 {
					return nil, EABORT
				} else {
					return nil, ENORETRY
				}
			} else {
				log.Fatalf("err %v\n", err)
			}
		}
		bid := b.Value().(*Bid)
		if *Allocate {
			rbids[i] = *bid
		}
		uk := UserKey(bid.Bidder)
		u, err := tx.Read(uk)
		if err != nil {
			if err == ESTASH {
				dlog.Printf("ViewBidHist() user stashed %v\n", uk)
				return nil, ESTASH
			} else if err == EABORT {
				return nil, EABORT
			} else if err == ENOKEY {
				dlog.Printf("ViewBidHist() Viewing bid %v and user doesn't exist?! %v\n", listy[i].key, uk)
				if tx.Commit() == 0 {
					return nil, EABORT
				} else {
					return nil, ENORETRY
				}
			} else {
				log.Fatalf("err %v\n", err)
			}
		}
		if *Allocate {
			rnn[i] = u.Value().(*User).Nickname
		}
	}

	if tx.Commit() == 0 {
		return nil, EABORT
	}
	var r *Result = nil
	if *Allocate {
		r = &Result{
			&struct {
				bids []Bid
				nns  []string
			}{rbids, rnn}}
	}
	return r, nil
}

func ViewUserInfoTxn(t Query, tx ETransaction) (*Result, error) {
	uk := UserKey(t.U1)
	urec, err := tx.Read(uk)
	if err != nil {
		if err == ESTASH {
			dlog.Printf("User  %v stashed\n", t.U1)
			return nil, ESTASH
		} else if err == EABORT {
			return nil, EABORT
		} else if err == ENOKEY {
			dlog.Printf("No user? %v\n", t.U1)
			if tx.Commit() == 0 {
				return nil, EABORT
			} else {
				return nil, ENORETRY
			}
		} else {
			log.Fatalf("view user err: %v\n", err)
		}
	}
	_ = urec.Value().(*User)
	if tx.Commit() == 0 {
		return nil, EABORT
	}
	var r *Result = nil
	if *Allocate {
		r = &Result{urec.Value()}
	}
	return r, nil
}

func PutBidTxn(t Query, tx ETransaction) (*Result, error) {
	item := t.U1

	ik := ItemKey(item)
	irec, err := tx.Read(ik)
	if err != nil {
		if err == ESTASH {
			dlog.Printf("Item key  %v stashed\n", item)
			return nil, ESTASH
		} else if err == EABORT {
			return nil, EABORT
		} else if err == ENOKEY {
			dlog.Printf("PutBidTxn: No item? %v\n", item)
			if tx.Commit() == 0 {
				return nil, EABORT
			} else {
				return nil, ENORETRY
			}
		} else {
			log.Fatalf("err %v\n", err)
		}
	}
	tok := UserKey(irec.Value().(*Item).Seller)
	torec, err := tx.Read(tok)
	if err != nil {
		if err == ESTASH {
			dlog.Printf("User key for user %v stashed\n", tok)
			return nil, ESTASH
		} else if err == EABORT {
			return nil, EABORT
		} else if err == ENOKEY {
			dlog.Printf("No user? %v\n", tok)
			if tx.Commit() == 0 {
				return nil, EABORT
			} else {
				return nil, ENORETRY
			}
		} else {
			log.Fatalf("err %v\n", err)
		}
	}
	nickname := torec.Value().(*User).Nickname

	numbk := NumBidsKey(item)
	numbrec, err := tx.Read(numbk)
	if err != nil {
		if err == ESTASH {
			dlog.Printf("Num bids key for item %v stashed\n", item)
			return nil, ESTASH
		} else if err == EABORT {
			return nil, EABORT
		} else if err == ENOKEY {
			dlog.Printf("No num bids? %v\n", item)
			if tx.Commit() == 0 {
				return nil, EABORT
			} else {
				return nil, ENORETRY
			}
		} else {
			log.Fatalf("err %v\n", err)
		}
	}
	nb := numbrec.int_value

	maxbk := MaxBidKey(item)
	maxbrec, err := tx.Read(maxbk)
	if err != nil {
		if err == ESTASH {
			dlog.Printf("Max bid key for item %v stashed\n", item)
			return nil, ESTASH
		} else if err == EABORT {
			return nil, EABORT
		} else if err == ENOKEY {
			dlog.Printf("No max bid? %v\n", item)
			if tx.Commit() == 0 {
				return nil, EABORT
			} else {
				return nil, ENORETRY
			}
		} else {
			log.Fatalf("err %v\n", err)
		}
	}
	maxb := maxbrec.int_value

	if tx.Commit() == 0 {
		return nil, EABORT
	}
	var r *Result = nil
	if *Allocate {
		r = &Result{
			&struct {
				nick string
				max  int32
				numb int32
			}{nickname, maxb, nb},
		}
	}
	return r, nil
}

func PutCommentTxn(t Query, tx ETransaction) (*Result, error) {
	var r *Result = nil
	touser := t.U1
	item := t.U2
	tok := UserKey(touser)
	torec, err := tx.Read(tok)
	if err != nil {
		if err == ESTASH {
			dlog.Printf("User key for user %v stashed\n", touser)
			return nil, ESTASH
		} else if err == EABORT {
			return nil, EABORT
		} else if err == ENOKEY {
			dlog.Printf("No user? %v\n", touser)
			if tx.Commit() == 0 {
				return nil, EABORT
			} else {
				return nil, ENORETRY
			}
		} else {
			log.Fatalf("err %v\n", err)
		}
	}
	nickname := torec.Value().(*User).Nickname
	ik := ItemKey(item)
	irec, err := tx.Read(ik)
	if err != nil {
		if err == ESTASH {
			dlog.Printf("Item key  %v stashed\n", item)
			return nil, ESTASH
		} else if err == EABORT {
			return nil, EABORT
		} else if err == ENOKEY {
			dlog.Printf("PutCommentTxn: No item? %v\n", item)
			if tx.Commit() == 0 {
				return nil, EABORT
			} else {
				return nil, ENORETRY
			}
		} else {
			log.Fatalf("err %v\n", err)
		}
	}
	itemname := irec.Value().(*Item).Name
	if tx.Commit() == 0 {
		return r, EABORT
	}
	if *Allocate {
		r = &Result{
			&struct {
				nick  string
				iname string
			}{nickname, itemname},
		}
	}
	return r, nil
}

func SearchItemsCategTxn(t Query, tx ETransaction) (*Result, error) {
	categ := t.U1
	num := t.U2
	var r *Result = nil
	if num > 10 {
		log.Fatalf("Only 10 search items are currently supported.\n")
	}
	ibck := ItemsByCatKey(categ)
	ibcrec, err := tx.Read(ibck)
	if err != nil {
		if err == ESTASH {
			return nil, ESTASH
		}
		if err == EABORT {
			return nil, EABORT
		}
		if err == ENOKEY {
			dlog.Printf("SearchItemsCategTxn: No key %v\n", ibck)
			if tx.Commit() == 0 {
				return nil, EABORT
			} else {
				return nil, ENORETRY
			}
		} else {
			log.Fatalf("err %v\n", err)
		}
	}
	listy := ibcrec.entries
	_ = listy

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

	var br *BRecord
	for i := 0; i < len(listy); i++ {
		k := uint64(listy[i].top)
		br, err = tx.Read(ItemKey(k))
		if err != nil {
			if err == ESTASH {
				return nil, ESTASH
			} else if err == EABORT {
				return nil, EABORT
			} else if err == ENOKEY {
				dlog.Printf("No item key %v\n", k)
				continue
			}
		} else {
			val2 := br.Value().(*Item)
			_ = *val2
			if *Allocate {
				ret[i] = val2
			}
		}
		br, err = tx.Read(NumBidsKey(k))
		if err != nil {
			if err == ESTASH {
				return nil, ESTASH
			} else if err == EABORT {
				return nil, EABORT
			} else if err == ENOKEY {
				dlog.Printf("No number of bids key %v\n", k)
			}
		} else {
			val4 := br.int_value
			_ = val4
			if *Allocate {
				numb[i] = val4
			}
		}
		br, err = tx.Read(MaxBidKey(k))
		if err != nil {
			if err == ESTASH {
				return nil, ESTASH
			} else if err == EABORT {
				return nil, EABORT
			} else if err == ENOKEY {
				dlog.Printf("No max bid key %v\n", k)
			}
		} else {
			val3 := br.int_value
			_ = val3
			if *Allocate {
				maxb[i] = val3
			}
		}
	}

	if tx.Commit() == 0 {
		return r, EABORT
	}
	if *Allocate {
		r = &Result{
			&struct {
				items   []*Item
				maxbids []int32
				numbids []int32
			}{ret, maxb, numb},
		}
		//dlog.Printf("Searched categ %v\n", categ)
	}
	return r, nil
}

func SearchItemsRegionTxn(t Query, tx ETransaction) (*Result, error) {
	region := t.U1
	categ := t.U2
	num := t.U3
	var r *Result = nil
	if num > 10 {
		log.Fatalf("Only 10 search items are currently supported.\n")
	}
	ibrk := ItemsByRegKey(region, categ)
	ibrrec, err := tx.Read(ibrk)

	if err != nil {
		if err == ESTASH {
			return nil, ESTASH
		} else if err == EABORT {
			return nil, EABORT
		} else if err == ENOKEY {
			dlog.Printf("No index for cat/region %v/%v %v\n", region, categ, ibrk)
			if tx.Commit() == 0 {
				return nil, EABORT
			} else {
				return nil, ENORETRY
			}
		} else {
			log.Fatalf("err: %v\n", err)
		}
	}

	listy := ibrrec.entries
	_ = listy

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

	var br *BRecord
	for i := 0; i < len(listy); i++ {
		k := uint64(listy[i].top)
		br, err = tx.Read(ItemKey(k))
		if err != nil {
			if err == ESTASH {
				return nil, ESTASH
			}
			if err == EABORT {
				return nil, EABORT
			}
			if err == ENOKEY {
				dlog.Printf("Item in list doesn't exist %v; %v\n", k, listy[i])
				continue
			} else {
				log.Fatalf("err: %v\n", err)
			}
		} else {
			val2 := br.Value().(*Item)
			_ = *val2
			if *Allocate {
				ret[i] = val2
			}
		}
		br, err = tx.Read(NumBidsKey(k))
		if err != nil {
			if err == ESTASH {
				return nil, ESTASH
			} else if err == EABORT {
				return nil, EABORT
			} else if err == ENOKEY {
				dlog.Printf("No number of bids key %v\n", k)
			} else {
				log.Fatalf("err: %v\n", err)
			}
		} else {
			val4 := br.int_value
			_ = val4
			if *Allocate {
				numb[i] = val4
			}
		}
		br, err = tx.Read(MaxBidKey(k))
		if err != nil {
			if err == ESTASH {
				return nil, ESTASH
			} else if err == EABORT {
				return nil, EABORT
			} else if err == ENOKEY {
				dlog.Printf("No max bid key %v\n", k)
			} else {
				log.Fatalf("err: %v\n", err)
			}
		} else {
			val3 := br.int_value
			_ = val3
			if *Allocate {
				maxb[i] = val3
			}
		}
	}

	if tx.Commit() == 0 {
		return r, EABORT
	}
	if *Allocate {
		r = &Result{
			&struct {
				items   []*Item
				maxbids []int32
				numbids []int32
			}{ret, maxb, numb},
		}
	}
	return r, nil
}

func ViewItemTxn(t Query, tx ETransaction) (*Result, error) {
	var r *Result = nil
	id := t.U1
	item, err := tx.Read(ItemKey(id))
	if err != nil {
		if err == ESTASH {
			return nil, ESTASH
		} else if err == EABORT {
			return nil, EABORT
		} else if err == ENOKEY {
			if tx.Commit() == 0 {
				return nil, EABORT
			} else {
				return nil, ENORETRY
			}
		} else {
			log.Fatalf("err: %v\n", err)
		}
	}
	val := item.Value().(*Item)
	_ = val
	maxbid, err := tx.Read(MaxBidKey(id))
	if err != nil {
		if err == ESTASH {
			return nil, ESTASH
		} else if err == EABORT {
			return nil, EABORT
		} else if err == ENOKEY {
			if tx.Commit() == 0 {
				return nil, EABORT
			} else {
				return nil, ENORETRY
			}
		} else {
			log.Fatalf("err: %v\n", err)
		}
	}
	val2 := maxbid.int_value
	_ = val2
	maxbidder, err := tx.Read(MaxBidBidderKey(id))

	if err != nil {
		if err == ESTASH {
			return nil, ESTASH
		} else if err == EABORT {
			return nil, EABORT
		} else if err == ENOKEY {
			if tx.Commit() == 0 {
				return nil, EABORT
			} else {
				return nil, ENORETRY
			}
		} else {
			log.Fatalf("err: %v\n", err)
		}
	}
	xx := maxbidder.Value().(Overwrite)
	val3 := xx.v.(uint64)
	if tx.Commit() == 0 {
		return r, EABORT
	}
	if *Allocate {
		r = &Result{&struct {
			Item
			int32
			uint64
		}{*val, val2, val3}}
	}
	return r, nil
}

func GetTxns(bidrate float64) []float64 {
	perc := make(map[float64]int)
	if bidrate >= 20 {
		perc = map[float64]int{
			10.0: RUBIS_SEARCHCAT,
			10.5: RUBIS_VIEW,
			5.47: RUBIS_SEARCHREG,
			4.97: RUBIS_PUTBID,
			// RUBIS_BID,
			2.13: RUBIS_VIEWUSER,
			1.81: RUBIS_NEWITEM,
			1.8:  RUBIS_REGISTER,
			1.4:  RUBIS_BUYNOW,
			1.34: RUBIS_VIEWBIDHIST,
			.55:  RUBIS_PUTCOMMENT,
			.5:   RUBIS_COMMENT,
		}
	} else {
		perc = map[float64]int{
			13.4: RUBIS_SEARCHCAT,
			11.3: RUBIS_VIEW,
			5.47: RUBIS_SEARCHREG,
			4.97: RUBIS_PUTBID,
			// RUBIS_BID,
			2.13: RUBIS_VIEWUSER,
			1.81: RUBIS_NEWITEM,
			1.8:  RUBIS_REGISTER,
			1.4:  RUBIS_BUYNOW,
			1.34: RUBIS_VIEWBIDHIST,
			.55:  RUBIS_PUTCOMMENT,
			.5:   RUBIS_COMMENT,
		}
	}

	perc[bidrate] = RUBIS_BID
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
	for i := RUBIS_BID; i < BIG_INCR; i++ {
		sum = sum + newperc[i]
		rates[i-RUBIS_BID] = sum
	}
	return rates
}
