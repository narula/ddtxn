package apps

import (
	"ddtxn"
	"ddtxn/dlog"
	"fmt"
	"sync/atomic"
)

type Buy struct {
	nproducts      int
	nbidders       int
	portion_sz     int
	nworkers       int
	read_rate      int
	contended_rate int
	bidder_keys    []ddtxn.Key
	product_keys   []ddtxn.Key
	validate       []int32
}

func InitBuy(s *ddtxn.Store, np, nb, portion_sz, nw, rr, crr int) *Buy {
	b := &Buy{
		nproducts:      np,
		nbidders:       nb,
		portion_sz:     portion_sz,
		nworkers:       nw,
		read_rate:      rr,
		contended_rate: crr,
		bidder_keys:    make([]ddtxn.Key, nb),
		product_keys:   make([]ddtxn.Key, np),
		validate:       make([]int32, np),
	}

	for i := 0; i < np; i++ {
		k := ddtxn.ProductKey(i)
		b.product_keys[i] = k
		s.CreateKey(k, int32(0), ddtxn.SUM)
	}
	// Uncontended keys
	for i := np; i < nb/10; i++ {
		k := ddtxn.ProductKey(i)
		s.CreateKey(k, int32(0), ddtxn.SUM)
	}
	for i := 0; i < nb; i++ {
		k := ddtxn.UserKey(i)
		b.bidder_keys[i] = k
		s.CreateKey(k, "x", ddtxn.WRITE)
	}
	return b
}

func (b *Buy) MakeOne(w int, local_seed *uint32, txn *ddtxn.Query) {
	lb := int(ddtxn.RandN(local_seed, uint32(b.portion_sz/4)))
	bidder := lb + w*b.portion_sz
	amt := int32(ddtxn.RandN(local_seed, 10))
	product := int(ddtxn.RandN(local_seed, uint32(b.nproducts)))
	x := int(ddtxn.RandN(local_seed, uint32(100)))
	if x < b.read_rate {
		if x >= b.contended_rate {
			// Contended read
			txn.K1 = b.product_keys[product]
		} else {
			// Uncontended read
			txn.K1 = b.bidder_keys[bidder]
		}
		txn.TXN = ddtxn.D_READ_BUY
	} else {
		txn.K1 = b.bidder_keys[bidder]
		txn.K2 = b.product_keys[product]
		txn.A = amt
		txn.TXN = ddtxn.D_BUY
	}
}

func (b *Buy) Add(t ddtxn.Query) {
	if t.TXN == ddtxn.D_BUY || t.TXN == ddtxn.D_BUY_NC {
		x := ddtxn.UndoCKey(t.K2)
		atomic.AddInt32(&b.validate[x], t.A)
	}
}

func (b *Buy) Validate(s *ddtxn.Store, nitr int) bool {
	good := true
	zero_cnt := 0
	for j := 0; j < b.nproducts; j++ {
		var x int32
		k := b.product_keys[j]
		v, err := s.Get(k)
		if err != nil {
			if b.validate[j] != 0 {
				fmt.Printf("Validating key %v failed; store: none should have: %v\n", k, b.validate[j])
				good = false
			}
			continue
		}
		if *ddtxn.SysType != ddtxn.DOPPEL {
			x = v.Value().(int32)
		} else {
			x = v.Value().(int32)
			dlog.Printf("Validate: %v %v\n", k, x)
		}
		if x != b.validate[j] {
			fmt.Printf("Validating key %v failed; store: %v should have: %v\n", k, x, b.validate[j])
			good = false
		}
		if x == 0 {
			dlog.Printf("Saying x is zero %v %v\n", x, zero_cnt)
			zero_cnt++
		}
	}
	if zero_cnt == b.nproducts && nitr > 10 {
		fmt.Printf("Bad: all zeroes!\n")
		dlog.Printf("Bad: all zeroes!\n")
		good = false
	}
	return good
}
