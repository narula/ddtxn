package apps

import (
	"ddtxn"
	"ddtxn/dlog"
	"flag"
	"fmt"
	"math/rand"
	"sync/atomic"
)

var ZipfDist = flag.Float64("zipf", 1, "Zipfian distribution theta. -1 means use -contention instead")
var partition = flag.Bool("partition", false, "Whether or not to partition the non-contended keys amongst the cores")

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
	z               []*ddtxn.Zipf
	padding1        [128]byte
}

func (b *Buy) Init(np, nb, nw, rr, ngo int, ncrr float64) {
	b.nproducts = np
	b.nbidders = nb
	b.nworkers = nw
	b.ngo = ngo
	b.read_rate = rr
	b.ncontended_rate = int(ncrr * float64(rr))
	b.validate = make([]int32, nb)
	b.sp = uint32(nb / nw)
	if *ZipfDist != -1 {
		b.z = make([]*ddtxn.Zipf, nw)
		for i := 0; i < nw; i++ {
			r := rand.New(rand.NewSource(int64(i * 38748767)))
			b.z[i] = ddtxn.NewZipf(r, *ZipfDist, 1, uint64(b.nbidders-1))
		}
	}
	dlog.Printf("Read rate %v, not contended: %v\n", b.read_rate, b.ncontended_rate)
}

func (b *Buy) Populate(s *ddtxn.Store, ex *ddtxn.ETransaction) {
	for i := 0; i < b.nbidders; i++ {
		k := ddtxn.ProductKey(i)
		s.CreateKey(k, int32(0), ddtxn.SUM)
	}
	dlog.Printf("Created products")
	for i := 0; i < b.nbidders; i++ {
		k := ddtxn.UserKey(uint64(i))
		s.CreateKey(k, "x", ddtxn.WRITE)
	}
	dlog.Printf("Created bidders")
	dlog.Printf("Done with Populate")
}

// Calls rand 4 times
func (b *Buy) MakeOne(w int, local_seed *uint32, sp uint32, txn *ddtxn.Query) {
	var bidder int
	var product int
	if *partition {
		rnd := ddtxn.RandN(local_seed, sp/8)
		lb := int(rnd)
		bidder = lb + w*int(sp)
	} else {
		bidder = int(ddtxn.RandN(local_seed, uint32(b.nbidders)))
	}
	x := int(ddtxn.RandN(local_seed, 100))
	if *ZipfDist > 0 {
		product = int(b.z[w].Uint64())
	} else {
		product = int(ddtxn.RandN(local_seed, uint32(b.nproducts)))
	}
	if x < b.read_rate {
		if x > b.ncontended_rate {
			// Contended read; use Zipfian distribution or np
			txn.K1 = ddtxn.UserKey(uint64(bidder))
			txn.K2 = ddtxn.ProductKey(product)
		} else {
			// (Hopefully) uncontended read.  Random product.
			product = int(ddtxn.RandN(local_seed, uint32(b.nbidders)))
			txn.K1 = ddtxn.UserKey(uint64(bidder))
			txn.K2 = ddtxn.ProductKey(product)
		}
		txn.TXN = ddtxn.D_READ_TWO
	} else {
		amt := int32(ddtxn.RandN(local_seed, 10))
		txn.K1 = ddtxn.UserKey(uint64(bidder))
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
		if x != b.validate[j] {
			fmt.Printf("Validating key %v failed; store: %v should have: %v\n", k, x, b.validate[j])
			good = false
		}
		if x == 0 {
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
