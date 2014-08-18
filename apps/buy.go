package apps

import (
	"ddtxn"
	"ddtxn/dlog"
	"ddtxn/stats"
	"fmt"
	"sync/atomic"
	"time"
)

type Buy struct {
	padding         [128]byte
	sp              uint32
	read_rate       int
	nproducts       int
	ncontended_rate int
	nbidders        int
	nworkers        int
	ngo             int
	validate        []int32
	lhr             []*stats.LatencyHist
	lhw             []*stats.LatencyHist
	padding1        [128]byte
}

func (b *Buy) Init(np, nb, nw, rr, ngo int, ncrr float64) {
	b.nproducts = np
	b.nbidders = nb
	b.nworkers = nw
	b.ngo = ngo
	b.read_rate = rr
	b.ncontended_rate = int(ncrr * float64(rr))
	b.validate = make([]int32, np)
	b.lhr = make([]*stats.LatencyHist, ngo)
	b.lhw = make([]*stats.LatencyHist, ngo)
	b.sp = uint32(nb / nw)
}

func (b *Buy) Populate(s *ddtxn.Store, ex *ddtxn.ETransaction) {
	var op ddtxn.KeyType = ddtxn.SUM
	for i := 0; i < b.nproducts; i++ {
		k := ddtxn.ProductKey(i)
		s.CreateKey(k, int32(0), op)
	}
	// Uncontended keys
	for i := b.nproducts; i < b.nbidders/10; i++ {
		k := ddtxn.ProductKey(i)
		s.CreateKey(k, int32(0), ddtxn.SUM)
	}
	dlog.Printf("Created products")
	for i := 0; i < b.nbidders; i++ {
		k := ddtxn.UserKey(i)
		s.CreateKey(k, "x", ddtxn.WRITE)
	}
	dlog.Printf("Created bidders")
	if *Latency {
		b.SetupLatency(100, 1000000, b.ngo)
	}
	dlog.Printf("Done with Populate")
}

func (b *Buy) SetupLatency(nincr int64, nbuckets int64, ngo int) {
	for i := 0; i < ngo; i++ {
		b.lhr[i] = stats.MakeLatencyHistogram(nincr, nbuckets)
		b.lhw[i] = stats.MakeLatencyHistogram(nincr, nbuckets)
	}
}

// Calls rand 4 times
func (b *Buy) MakeOne(w int, local_seed *uint32, txn *ddtxn.Query) {
	rnd := ddtxn.RandN(local_seed, b.sp/8)
	lb := int(rnd)
	bidder := lb + w*int(b.sp)
	amt := int32(ddtxn.RandN(local_seed, 10))
	product := int(ddtxn.RandN(local_seed, uint32(b.nproducts)))
	x := int(ddtxn.RandN(local_seed, 100))
	if x < b.read_rate {
		if x > b.ncontended_rate {
			// Contended read
			txn.K1 = ddtxn.ProductKey(product)
		} else {
			// Uncontended read
			txn.K1 = ddtxn.UserKey(bidder)
		}
		txn.TXN = ddtxn.D_READ_ONE
	} else {
		txn.K1 = ddtxn.UserKey(bidder)
		txn.K2 = ddtxn.ProductKey(product)
		txn.A = amt
		txn.TXN = ddtxn.D_BUY
	}
}

func (b *Buy) Add(t ddtxn.Query) {
	if t.TXN == ddtxn.D_BUY {
		x, _ := ddtxn.UndoCKey(t.K2)
		atomic.AddInt32(&b.validate[x], t.A)
	}
}

func (b *Buy) Validate(s *ddtxn.Store, nitr int) bool {
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
		x = v.Value().(int32)
		dlog.Printf("Validate: %v %v\n", k, x)
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

func (b *Buy) Time(t *ddtxn.Query, txn_end time.Duration, n int) {
	if t.TXN == ddtxn.D_READ_ONE {
		b.lhr[n].AddOne(txn_end.Nanoseconds())
	} else {
		b.lhw[n].AddOne(txn_end.Nanoseconds())
	}
}

func (b *Buy) LatencyString() (string, string) {
	if !*Latency {
		return "", ""
	}
	for i := 1; i < b.ngo; i++ {
		b.lhr[0].Combine(b.lhr[i])
		b.lhw[0].Combine(b.lhw[i])
	}
	return fmt.Sprintf("Read 25: %v\nRead 50: %v\nRead 75: %v\nRead 99: %v\n", b.lhr[0].GetPercentile(25), b.lhr[0].GetPercentile(50), b.lhr[0].GetPercentile(75), b.lhr[0].GetPercentile(99)), fmt.Sprintf("Write 25: %v\nWrite 50: %v\nWrite 75: %v\nWrite 99: %v\n", b.lhw[0].GetPercentile(25), b.lhw[0].GetPercentile(50), b.lhw[0].GetPercentile(75), b.lhw[0].GetPercentile(99))
}
