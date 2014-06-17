package apps

import (
	"ddtxn"
	"ddtxn/dlog"
	"ddtxn/stats"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

type Rubis struct {
	nproducts       int
	nbidders        int
	portion_sz      int
	nworkers        int
	read_rate       int
	ncontended_rate int
	validate        []int32
	lhr             []*stats.LatencyHist
	lhw             []*stats.LatencyHist
	sp              uint32
}

func InitRubis(s *ddtxn.Store, np, nb, nw, ngo int, ex *ddtxn.ETransaction) *Rubis {
	b := &Rubis{
		nproducts: np,
		nbidders:  nb,
		nworkers:  nw,
		validate:  make([]int32, np),
		lhr:       make([]*stats.LatencyHist, ngo),
		lhw:       make([]*stats.LatencyHist, ngo),
		sp:        uint32(nb / nw),
	}
	for i := 0; i < nb; i++ {
		q := ddtxn.Query{
			T:  ddtxn.TID(i),
			S1: fmt.Sprintf("xxx%d", i),
			U1: uint64(rand.Intn(ddtxn.NUM_REGIONS)),
		}
		ddtxn.RegisterUserTxn(q, ex)
		ex.Reset()
	}
	for i := 0; i < ddtxn.NUM_ITEMS; i++ {
		q := ddtxn.Query{
			T:  ddtxn.TID(i),
			S1: "xxx",
			S2: "lovely",
			U1: uint64(rand.Intn(nb)),
			U2: 100,
			U3: 100,
			U4: 1000,
			U5: 1000,
			U6: 1,
			I:  37,
			U7: uint64(rand.Intn(ddtxn.NUM_CATEGORIES)),
		}
		ddtxn.NewItemTxn(q, ex)
		ex.Reset()
		// Allocate keys for every combination of user and product
		// bids. This is to avoid using read locks during execution by
		// guaranteeing the map of keys won't change.
		for j := 0; j < nb; j++ {
			k := ddtxn.PBidKey(uint64(i), uint64(j))
			s.CreateKey(k, "", ddtxn.WRITE)
		}
	}
	return b
}

func (b *Rubis) SetupLatency(nincr int64, nbuckets int64, ngo int) {
	for i := 0; i < ngo; i++ {
		b.lhr[i] = stats.MakeLatencyHistogram(nincr, nbuckets)
		b.lhw[i] = stats.MakeLatencyHistogram(nincr, nbuckets)
	}
}

func (b *Rubis) MakeOne(w int, local_seed *uint32, txn *ddtxn.Query) {
	x := int(ddtxn.RandN(local_seed, 100))
	if x < b.read_rate {
		rnd := ddtxn.RandN(local_seed, b.sp)
		lb := int(rnd)
		bidder := lb + w*int(b.sp)
		product := ddtxn.RandN(local_seed, uint32(b.nproducts))
		txn.U1 = uint64(bidder)
		txn.U2 = uint64(product)
		txn.A = int32(ddtxn.RandN(local_seed, 10))
		txn.TXN = ddtxn.RUBIS_BID

	} else {
		txn.TXN = ddtxn.RUBIS_SEARCHCAT
		txn.U1 = uint64(ddtxn.RandN(local_seed, uint32(ddtxn.NUM_CATEGORIES)))
		txn.U2 = 5
	}
}

func (b *Rubis) Add(t ddtxn.Query) {
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

func (b *Rubis) Validate(s *ddtxn.Store, nitr int) bool {
	good := true
	zero_cnt := 0
	for j := 0; j < b.nproducts; j++ {
		var x int32
		k := ddtxn.ProductKey(j)
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

func (b *Rubis) Time(t *ddtxn.Query, txn_end time.Duration, n int) {
	if t.TXN == ddtxn.D_READ_ONE {
		b.lhr[n].AddOne(txn_end.Nanoseconds())
	} else {
		b.lhw[n].AddOne(txn_end.Nanoseconds())
	}
}

func (b *Rubis) LatencyString(ngo int) (string, string) {
	for i := 1; i < ngo; i++ {
		b.lhr[0].Combine(b.lhr[i])
		b.lhw[0].Combine(b.lhw[i])
	}
	return fmt.Sprint("Read 25: %v\nRead 50: %v\nRead 75: %v\nRead 99: %v\n", b.lhr[0].GetPercentile(25), b.lhr[0].GetPercentile(50), b.lhr[0].GetPercentile(75), b.lhr[0].GetPercentile(99)), fmt.Sprint("Write 25: %v\nWrite 50: %v\nWrite 75: %v\nWrite 99: %v\n", b.lhw[0].GetPercentile(25), b.lhw[0].GetPercentile(50), b.lhw[0].GetPercentile(75), b.lhw[0].GetPercentile(99))
}
