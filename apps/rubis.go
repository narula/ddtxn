package apps

import (
	"ddtxn"
	"ddtxn/dlog"
	"ddtxn/stats"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

var Skewed = flag.Bool("skew", false, "Rubis-C (skewed workload) or not, default Rubis-B")

type Rubis struct {
	padding    [128]byte
	nproducts  int
	nbidders   int
	portion_sz int
	nworkers   int
	ngo        int
	maxes      []int32
	num_bids   []int32
	ratings    map[uint64]int32
	users      []uint64
	products   []uint64
	lhr        []*stats.LatencyHist
	lhw        []*stats.LatencyHist
	sp         uint32
	rates      []float64
	padding1   [128]byte
}

func (b *Rubis) Init(np, nb, nw, ngo int) {
	b.nproducts = np
	b.nbidders = nb
	b.nworkers = nw
	b.ngo = ngo
	b.maxes = make([]int32, np)
	b.num_bids = make([]int32, np)
	b.ratings = make(map[uint64]int32)
	b.lhr = make([]*stats.LatencyHist, ngo)
	b.lhw = make([]*stats.LatencyHist, ngo)
	b.sp = uint32(nb / nw)
	b.rates = ddtxn.GetTxns(*Skewed)
	b.users = make([]uint64, nb)
	b.products = make([]uint64, ddtxn.NUM_ITEMS)
}

func (b *Rubis) Populate(s *ddtxn.Store, ex ddtxn.ETransaction) {
	for i := 0; i < b.nbidders; i++ {
		q := ddtxn.Query{
			S1: fmt.Sprintf("xxx%d", i),
			U1: uint64(rand.Intn(ddtxn.NUM_REGIONS)),
		}
		r, err := ddtxn.RegisterUserTxn(q, ex)
		if err != nil {
			log.Fatalf("Could not create user %v %v\n", q.S1, q.U1)
		}
		b.users[i] = r.V.(uint64)
		ex.Reset()
	}
	for i := 0; i < ddtxn.NUM_ITEMS; i++ {
		q := ddtxn.Query{
			S1: "xxx",
			S2: "lovely",
			U1: b.users[rand.Intn(b.nbidders)],
			U2: 100,
			U3: 100,
			U4: 1000,
			U5: 1000,
			U6: 1,
			U7: uint64(rand.Intn(ddtxn.NUM_CATEGORIES)),
		}
		r, err := ddtxn.NewItemTxn(q, ex)
		if err != nil {
			log.Fatalf("Could not create items %v %v\n", i, err)
		}
		b.products[i] = r.V.(uint64)
		ex.Reset()
		if !*ddtxn.UseRLocks {
			// Allocate keys for every combination of user and product
			// bids. This is to avoid using read locks during execution by
			// guaranteeing the map of keys won't change.
			if i < b.nproducts {
				for j := 0; j < b.nbidders; j++ {
					k := ddtxn.PairBidKey(uint64(j), uint64(i))
					s.CreateKey(k, "", ddtxn.WRITE)
				}
			}
		}
	}
	if *Latency {
		b.SetupLatency(100, 1000000, b.ngo)
	}
}

func (b *Rubis) SetupLatency(nincr int64, nbuckets int64, ngo int) {
	for i := 0; i < ngo; i++ {
		b.lhr[i] = stats.MakeLatencyHistogram(nincr, nbuckets)
		b.lhw[i] = stats.MakeLatencyHistogram(nincr, nbuckets)
	}
}

func (b *Rubis) MakeOne(w int, local_seed *uint32, txn *ddtxn.Query) {
	x := float64(ddtxn.RandN(local_seed, 100))
	if x < b.rates[0] {
		txn.TXN = ddtxn.RUBIS_BID
		bidder := b.users[int(ddtxn.RandN(local_seed, b.sp))+w*int(b.sp)]
		product := b.products[ddtxn.RandN(local_seed, uint32(b.nproducts))]
		txn.U1 = uint64(bidder)
		txn.U2 = uint64(product)
		txn.A = int32(ddtxn.RandN(local_seed, 10))
	} else if x < b.rates[1] {
		txn.TXN = ddtxn.RUBIS_VIEWBIDHIST
		product := b.products[ddtxn.RandN(local_seed, uint32(b.nproducts))]
		txn.U1 = uint64(product)
	} else if x < b.rates[2] {
		txn.TXN = ddtxn.RUBIS_BUYNOW
		bidder := b.users[int(ddtxn.RandN(local_seed, b.sp))+w*int(b.sp)]
		product := b.products[ddtxn.RandN(local_seed, uint32(b.nproducts))]
		txn.U1 = uint64(bidder)
		txn.U2 = uint64(product)
		txn.A = int32(ddtxn.RandN(local_seed, 10))
	} else if x < b.rates[3] {
		txn.TXN = ddtxn.RUBIS_COMMENT
		u1 := b.users[int(ddtxn.RandN(local_seed, b.sp))+w*int(b.sp)]
		u2 := b.users[int(ddtxn.RandN(local_seed, b.sp))+w*int(b.sp)]
		product := b.products[ddtxn.RandN(local_seed, uint32(b.nproducts))]
		txn.U1 = uint64(u1)
		txn.U2 = uint64(u2)
		txn.U3 = uint64(product)
		txn.S1 = "xxxx"
		txn.U4 = 1
	} else if x < b.rates[4] {
		txn.TXN = ddtxn.RUBIS_NEWITEM
		bidder := b.users[int(ddtxn.RandN(local_seed, b.sp))+w*int(b.sp)]
		amt := uint64(ddtxn.RandN(local_seed, 10))
		txn.U1 = uint64(bidder)
		txn.S1 = "yyyy"
		txn.S2 = "zzzz"
		txn.U2 = amt
		txn.U3 = amt
		txn.U4 = amt
		txn.U5 = 1
		txn.U6 = 1
		txn.U7 = uint64(ddtxn.RandN(local_seed, uint32(ddtxn.NUM_CATEGORIES)))
	} else if x < b.rates[5] {
		txn.TXN = ddtxn.RUBIS_PUTBID
		product := b.products[ddtxn.RandN(local_seed, uint32(b.nproducts))]
		txn.U1 = uint64(product)
	} else if x < b.rates[6] {
		txn.TXN = ddtxn.RUBIS_PUTCOMMENT
		product := b.products[ddtxn.RandN(local_seed, uint32(b.nproducts))]
		bidder := b.users[int(ddtxn.RandN(local_seed, b.sp))+w*int(b.sp)]
		txn.U1 = uint64(bidder)
		txn.U2 = uint64(product)
	} else if x < b.rates[7] {
		txn.TXN = ddtxn.RUBIS_REGISTER
		txn.U1 = uint64(ddtxn.RandN(local_seed, uint32(ddtxn.NUM_REGIONS)))
		txn.S1 = fmt.Sprintf("xxx%d", ddtxn.RandN(local_seed, 1000000000))
	} else if x < b.rates[8] {
		txn.TXN = ddtxn.RUBIS_SEARCHCAT
		txn.U1 = uint64(ddtxn.RandN(local_seed, uint32(ddtxn.NUM_CATEGORIES)))
		txn.U2 = 5
	} else if x < b.rates[9] {
		txn.TXN = ddtxn.RUBIS_SEARCHREG
		txn.U1 = uint64(ddtxn.RandN(local_seed, uint32(ddtxn.NUM_REGIONS)))
		txn.U2 = uint64(ddtxn.RandN(local_seed, uint32(ddtxn.NUM_CATEGORIES)))
		txn.U3 = 5
	} else if x < b.rates[10] {
		txn.TXN = ddtxn.RUBIS_VIEW
		product := b.products[ddtxn.RandN(local_seed, uint32(b.nproducts))]
		txn.U1 = uint64(product)
	} else if x < b.rates[11] {
		txn.TXN = ddtxn.RUBIS_VIEWUSER
		bidder := b.users[int(ddtxn.RandN(local_seed, b.sp))+w*int(b.sp)]
		txn.U1 = uint64(bidder)
	} else {
		log.Fatalf("No such transaction\n")
	}
}

func (b *Rubis) Add(t ddtxn.Query) {
	if t.TXN == ddtxn.RUBIS_BID {
		x := t.U2
		atomic.AddInt32(&b.num_bids[x], 1)
		for t.A > b.maxes[x] {
			v := atomic.LoadInt32(&b.maxes[x])
			done := atomic.CompareAndSwapInt32(&b.maxes[x], v, t.A)
			if done {
				break
			}
		}
	} else if t.TXN == ddtxn.RUBIS_COMMENT {
		b.ratings[t.U1] += 1
	}
}

func (b *Rubis) Validate(s *ddtxn.Store, nitr int) bool {
	good := true
	zero_cnt := 0
	for k, rat := range b.ratings {
		key := ddtxn.RatingKey(k)
		v, err := s.Get(key)
		if err != nil {
			fmt.Printf("Validating key %v failed; store: doesn't have rating for user %v: %v\n", key, k, err)
			good = false
			continue
		}
		r := v.Value().(int32)
		if r != rat {
			fmt.Printf("Validating key %v failed; store: has different rating for user %v (%v vs. %v): %v\n", key, k, rat, r, err)
			good = false
			continue
		}
	}
	for j := 0; j < b.nproducts; j++ {
		var x int32
		k := ddtxn.MaxBidKey(uint64(j))
		v, err := s.Get(k)
		if err != nil {
			if b.maxes[j] != 0 {
				fmt.Printf("Validating key %v failed; store: none should have: %v\n", k, b.maxes[j])
				good = false
			}
			continue
		}
		x = v.Value().(int32)
		if x != b.maxes[j] {
			fmt.Printf("Validating key %v failed; store: %v should have: %v\n", k, x, b.maxes[j])
			good = false
		}
		if x == 0 {
			dlog.Printf("Saying x is zero %v %v\n", x, zero_cnt)
			zero_cnt++
		}
		k = ddtxn.NumBidsKey(uint64(j))
		v, err = s.Get(k)
		if err != nil {
			if b.maxes[j] != 0 {
				fmt.Printf("Validating key %v failed for max bid; store: none should have: %v\n", k, b.num_bids[j])
				good = false
			}
			continue
		}
		x = v.Value().(int32)
		if x != b.num_bids[j] {
			fmt.Printf("Validating key %v failed for number of bids; store: %v should have: %v\n", k, x, b.num_bids[j])
			good = false
		}
		if x == 0 {
			dlog.Printf("Saying x is zero %v %v\n", x, zero_cnt)
			zero_cnt++
		}

	}
	if zero_cnt == 2*b.nproducts && nitr > 10 {
		fmt.Printf("Bad: all zeroes!\n")
		dlog.Printf("Bad: all zeroes!\n")
		good = false
	}
	if good {
		dlog.Printf("Validate succeeded\n")
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

func (b *Rubis) LatencyString() (string, string) {
	if !*Latency {
		return "", ""
	}
	for i := 1; i < b.ngo; i++ {
		b.lhr[0].Combine(b.lhr[i])
		b.lhw[0].Combine(b.lhw[i])
	}
	return fmt.Sprintf("Read 25: %v\nRead 50: %v\nRead 75: %v\nRead 99: %v\n", b.lhr[0].GetPercentile(25), b.lhr[0].GetPercentile(50), b.lhr[0].GetPercentile(75), b.lhr[0].GetPercentile(99)), fmt.Sprintf("Write 25: %v\nWrite 50: %v\nWrite 75: %v\nWrite 99: %v\n", b.lhw[0].GetPercentile(25), b.lhw[0].GetPercentile(50), b.lhw[0].GetPercentile(75), b.lhw[0].GetPercentile(99))
}
