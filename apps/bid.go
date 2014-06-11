package apps

import (
	"ddtxn"
	"ddtxn/dlog"
	"ddtxn/stats"
	"fmt"
	"sync/atomic"
	"time"
)

type Bid struct {
	nproducts      int
	nbidders       int
	portion_sz     int
	nworkers       int
	read_rate      int
	contended_rate int
	bidder_keys    []ddtxn.Key
	product_keys   []ddtxn.Key
	validate       []int32
	lhr            []*stats.LatencyHist
	lhw            []*stats.LatencyHist
	sp             uint32
}

func InitBid(s *ddtxn.Store, np, nb, nw, rr, crr int, ngo int) *Bid {
	b := &Bid{
		nproducts:      np,
		nbidders:       nb,
		nworkers:       nw,
		read_rate:      rr,
		contended_rate: crr,
		bidder_keys:    make([]ddtxn.Key, nb),
		product_keys:   make([]ddtxn.Key, np),
		validate:       make([]int32, np),
		lhr:            make([]*stats.LatencyHist, ngo),
		lhw:            make([]*stats.LatencyHist, ngo),
		portion_sz:     nb / nw,
		sp:             uint32(nb / nw / 4),
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

func (b *Bid) SetupLatency(nincr int64, nbuckets int64, ngo int) {
	for i := 0; i < ngo; i++ {
		b.lhr[i] = stats.MakeLatencyHistogram(nincr, nbuckets)
		b.lhw[i] = stats.MakeLatencyHistogram(nincr, nbuckets)
	}
}

func (b *Bid) MakeOne(w int, local_seed *uint32, txn *ddtxn.Query) {
	rnd := ddtxn.RandN(local_seed, b.sp)
	lb := int(rnd)
	bidder := lb + w*b.portion_sz
	amt := int32(ddtxn.RandN(local_seed, 10))
	product := int(ddtxn.RandN(local_seed, uint32(b.nproducts)))
	x := int(ddtxn.RandN(local_seed, 100))
	if x < b.read_rate {
		if x >= b.contended_rate {
			// Contended read
			txn.K1 = b.product_keys[product]
		} else {
			// Uncontended read
			txn.K1 = b.bidder_keys[bidder]
		}
		txn.TXN = ddtxn.D_READ_ONE
	} else {
		txn.K1 = b.bidder_keys[bidder]
		txn.K2 = b.product_keys[product]
		txn.A = amt
		txn.TXN = ddtxn.D_BID
	}
}

func (b *Bid) Add(t ddtxn.Query) {
	if t.TXN == ddtxn.D_BID {
		x, _ := ddtxn.UndoCKey(t.K2)
		for t.A > b.validate[x] {
			v := atomic.LoadInt32(&b.validate[x])
			done := atomic.CompareAndSwapInt32(&b.validate[x], v, t.A)
			if done {
				break
			}
		}
	}
}

func (b *Bid) Validate(s *ddtxn.Store, nitr int) bool {
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

func (b *Bid) Time(t *ddtxn.Query, txn_end time.Duration, n int) {
	if t.TXN == ddtxn.D_READ_ONE {
		b.lhr[n].AddOne(txn_end.Nanoseconds())
	} else {
		b.lhw[n].AddOne(txn_end.Nanoseconds())
	}
}

func (b *Bid) LatencyString(ngo int) (string, string) {
	for i := 1; i < ngo; i++ {
		b.lhr[0].Combine(b.lhr[i])
		b.lhw[0].Combine(b.lhw[i])
	}
	return fmt.Sprint("Read 25: %v\nRead 50: %v\nRead 75: %v\nRead 99: %v\n", b.lhr[0].GetPercentile(25), b.lhr[0].GetPercentile(50), b.lhr[0].GetPercentile(75), b.lhr[0].GetPercentile(99)), fmt.Sprint("Write 25: %v\nWrite 50: %v\nWrite 75: %v\nWrite 99: %v\n", b.lhw[0].GetPercentile(25), b.lhw[0].GetPercentile(50), b.lhw[0].GetPercentile(75), b.lhw[0].GetPercentile(99))
}
