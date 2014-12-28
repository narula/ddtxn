package apps

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/narula/ddtxn"
	"github.com/narula/ddtxn/dlog"
)

type Rubis struct {
	sync.Mutex
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
	sp         uint32
	rates      []float64
	pidIdx     map[uint64]int
	zip        []*ddtxn.Zipf
	zipfd      float64
	padding1   [128]byte
}

func (b *Rubis) Init(np, nb, nw, ngo int, zipfd, bidrate float64) {
	if ddtxn.NUM_ITEMS > np {
		b.nproducts = np
		b.products = make([]uint64, ddtxn.NUM_ITEMS)
	} else {
		b.nproducts = ddtxn.NUM_ITEMS
		b.products = make([]uint64, b.nproducts)
	}
	b.nbidders = nb
	b.nworkers = nw
	b.ngo = ngo
	b.maxes = make([]int32, b.nproducts)
	b.num_bids = make([]int32, b.nproducts)
	b.pidIdx = make(map[uint64]int, b.nproducts)
	b.ratings = make(map[uint64]int32)
	b.sp = uint32(nb / nw)
	b.rates = ddtxn.GetTxns(bidrate)
	b.users = make([]uint64, nb)
	b.zipfd = zipfd
	b.zip = make([]*ddtxn.Zipf, nw)
}

func (b *Rubis) Populate(s *ddtxn.Store, c *ddtxn.Coordinator) {
	tmp := *ddtxn.Allocate
	tmp2 := *dlog.Debug
	*ddtxn.Allocate = true
	*dlog.Debug = false
	for wi := 0; wi < b.nworkers; wi++ {
		w := c.Workers[wi]
		ex := w.E
		for i := b.sp * uint32(wi); i < b.sp*(uint32(wi+1)); i++ {
			q := ddtxn.Query{
				U1: uint64(rand.Intn(ddtxn.NUM_REGIONS)),
				U2: 0,
			}
			r, err := ddtxn.RegisterUserTxn(q, ex)
			if err != nil {
				log.Fatalf("Could not create user %v; err:%v\n", i, err)
			}
			b.users[i] = r.V.(uint64)
			if b.users[i] == 0 {
				fmt.Printf("Created user 0; index; %v\n", i)
			}
			ex.Reset()
		}

		if b.zipfd > 0 {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			b.zip[wi] = ddtxn.NewZipf(r, b.zipfd, 1, uint64(b.nproducts-1))
		}
	}
	chunk := ddtxn.NUM_ITEMS / b.nworkers
	for wi := 0; wi < b.nworkers; wi++ {
		w := c.Workers[wi]
		ex := w.E
		nx := rand.Intn(b.nbidders)
		for i := chunk * wi; i < chunk*(wi+1); i++ {
			q := ddtxn.Query{
				T:  ddtxn.TID(i + 1),
				S1: "xxx",
				S2: "lovely",
				U1: b.users[nx],
				U2: 100,
				U3: 100,
				U4: 1000,
				U5: 1000,
				U6: 1,
				U7: uint64(rand.Intn(ddtxn.NUM_CATEGORIES)),
			}
			r, err := ddtxn.NewItemTxn(q, ex)
			if err != nil {
				fmt.Printf("%v Could not create item index %v error: %v user_id: %v user index: %v nb: %v\n", wi, i, err, q.U1, nx, b.nbidders)
				continue
			}
			v := r.V.(uint64)
			b.products[i] = v
			b.pidIdx[v] = i
			k := ddtxn.BidsPerItemKey(v)
			w.Store().CreateKey(k, nil, ddtxn.LIST)
			ex.Reset()
		}
	}
	*ddtxn.Allocate = tmp
	*dlog.Debug = tmp2
	fmt.Println("Done with Init()")
}

func (b *Rubis) PopulateBids(s *ddtxn.Store, c *ddtxn.Coordinator) {
	tmp := *ddtxn.Allocate
	tmp2 := *dlog.Debug
	*ddtxn.Allocate = true
	*dlog.Debug = false
	chunk := ddtxn.NUM_ITEMS / b.nworkers
	b.nbidders = 1
	b.users = make([]uint64, 1)
	ex := c.Workers[0].E
	q := ddtxn.Query{
		U1: uint64(rand.Intn(ddtxn.NUM_REGIONS)),
		U2: 0,
	}
	r, err := ddtxn.RegisterUserTxn(q, ex)
	if err != nil {
		log.Fatalf("Could not create user; err:%v\n", err)
	}
	b.users[0] = r.V.(uint64)
	if b.users[0] == 0 {
		fmt.Printf("Created user 0; index;\n")
	}
	ex.Reset()

	for wi := 0; wi < b.nworkers; wi++ {
		w := c.Workers[wi]
		ex := w.E
		nx := rand.Intn(b.nbidders)
		for i := chunk * wi; i < chunk*(wi+1); i++ {
			q := ddtxn.Query{
				T:  ddtxn.TID(i + 1),
				S1: "xxx",
				S2: "lovely",
				U1: b.users[nx],
				U2: 100,
				U3: 100,
				U4: 1000,
				U5: 1000,
				U6: 1,
				U7: uint64(rand.Intn(ddtxn.NUM_CATEGORIES)),
			}
			r, err := ddtxn.NewItemTxn(q, ex)
			if err != nil {
				fmt.Printf("%v Could not create item index %v error: %v user_id: %v user index: %v nb: %v\n", wi, i, err, q.U1, nx, b.nbidders)
				continue
			}
			v := r.V.(uint64)
			b.products[i] = v
			b.pidIdx[v] = i
			k := ddtxn.BidsPerItemKey(v)
			w.Store().CreateKey(k, nil, ddtxn.LIST)
			ex.Reset()
		}
		if b.zipfd > 0 {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			b.zip[wi] = ddtxn.NewZipf(r, b.zipfd, 1, uint64(b.nproducts-1))
		}
	}
	*ddtxn.Allocate = tmp
	*dlog.Debug = tmp2
}

func (b *Rubis) MakeBid(w int, local_seed *uint32, txn *ddtxn.Query) {
	txn.TXN = ddtxn.RUBIS_BID
	bidder := b.users[0]
	var x uint32
	if b.zipfd > 0 {
		x = uint32(b.zip[w].Uint64())
	} else {
		x = ddtxn.RandN(local_seed, uint32(b.nproducts))
	}
	if x >= uint32(len(b.products)) {
		log.Fatalf("Huh %v %v %v\n", x, len(b.products), b.nproducts)
	}
	product := b.products[x]
	txn.U1 = uint64(bidder)
	if product == 0 {
		log.Fatalf("store bid ID 0? %v %v", x, b.products[x])
	}
	txn.U2 = uint64(product)
	txn.U3 = uint64(time.Now().UnixNano())
}

func (b *Rubis) MakeOne(w int, local_seed *uint32, txn *ddtxn.Query) {
	x := float64(ddtxn.RandN(local_seed, 100))
	if x < b.rates[0] {
		txn.TXN = ddtxn.RUBIS_BID
		//bidder := b.users[int(ddtxn.RandN(local_seed, b.sp))+w*int(b.sp)]
		bidder := b.users[int(ddtxn.RandN(local_seed, uint32(b.nbidders)))]
		//product := b.products[ddtxn.RandN(local_seed, uint32(b.nproducts))]
		var x uint32
		if b.zipfd > 0 {
			x = uint32(b.zip[w].Uint64())
		} else {
			x = ddtxn.RandN(local_seed, uint32(b.nproducts))
		}
		product := b.products[x]
		txn.U1 = uint64(bidder)
		if product == 0 {
			log.Fatalf("store bid ID 0? %v %v", x, b.products[x])
		}
		txn.U2 = uint64(product)
		//txn.U3 = uint64(ddtxn.RandN(local_seed, 10))
		txn.U3 = uint64(time.Now().UnixNano()) & 0x000efff
	} else if x < b.rates[1] {
		txn.TXN = ddtxn.RUBIS_VIEWBIDHIST
		x := ddtxn.RandN(local_seed, uint32(b.nproducts))
		product := b.products[x]
		if product == 0 {
			log.Fatalf("view bid hist ID 0? %v %v", x, b.products[x])
		}
		txn.U1 = uint64(product)
	} else if x < b.rates[2] {
		txn.TXN = ddtxn.RUBIS_BUYNOW
		//bidder := b.users[int(ddtxn.RandN(local_seed, b.sp))+w*int(b.sp)]
		bidder := b.users[int(ddtxn.RandN(local_seed, uint32(b.nbidders)))]
		x := ddtxn.RandN(local_seed, uint32(b.nproducts))
		product := b.products[x]
		if product == 0 {
			log.Fatalf("buy now ID 0? %v %v", x, b.products[x])
		}
		txn.U1 = uint64(bidder)
		txn.U2 = uint64(product)
		txn.A = int32(ddtxn.RandN(local_seed, 10))
	} else if x < b.rates[3] {
		txn.TXN = ddtxn.RUBIS_COMMENT
		//		u1 := b.users[int(ddtxn.RandN(local_seed, b.sp))+w*int(b.sp)]
		u1 := b.users[int(ddtxn.RandN(local_seed, uint32(b.nbidders)))]
		//		u2 := b.users[int(ddtxn.RandN(local_seed, b.sp))+w*int(b.sp)]
		u2 := b.users[int(ddtxn.RandN(local_seed, uint32(b.nbidders)))]
		x := ddtxn.RandN(local_seed, uint32(b.nproducts))
		product := b.products[x]
		if product == 0 {
			log.Fatalf("comment ID 0? %v %v", x, b.products[x])
		}
		txn.U1 = uint64(u1)
		txn.U2 = uint64(u2)
		txn.U3 = uint64(product)
		txn.S1 = "xxxx"
		txn.U4 = 1
	} else if x < b.rates[4] {
		txn.TXN = ddtxn.RUBIS_NEWITEM
		//		bidder := b.users[int(ddtxn.RandN(local_seed, b.sp))+w*int(b.sp)]
		bidder := b.users[int(ddtxn.RandN(local_seed, uint32(b.nbidders)))]
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
		x := ddtxn.RandN(local_seed, uint32(b.nproducts))
		product := b.products[x]
		if product == 0 {
			log.Fatalf("put bid ID 0? %v %v", x, b.products[x])
		}
		txn.U1 = uint64(product)
	} else if x < b.rates[6] {
		txn.TXN = ddtxn.RUBIS_PUTCOMMENT
		x := ddtxn.RandN(local_seed, uint32(b.nproducts))
		product := b.products[x]
		if product == 0 {
			log.Fatalf("put comment ID 0? %v %v", x, b.products[x])
		}
		//		bidder := b.users[int(ddtxn.RandN(local_seed, b.sp))+w*int(b.sp)]
		bidder := b.users[int(ddtxn.RandN(local_seed, uint32(b.nbidders)))]
		txn.U1 = uint64(bidder)
		txn.U2 = uint64(product)
	} else if x < b.rates[7] {
		txn.TXN = ddtxn.RUBIS_REGISTER
		txn.U1 = uint64(ddtxn.RandN(local_seed, uint32(ddtxn.NUM_REGIONS)))
		txn.U2 = uint64(ddtxn.RandN(local_seed, 1000000000))
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
		x := ddtxn.RandN(local_seed, uint32(b.nproducts))
		product := b.products[x]
		if product == 0 {
			log.Fatalf("view ID 0? %v %v", x, b.products[x])
		}
		txn.U1 = uint64(product)
	} else if x < b.rates[11] {
		txn.TXN = ddtxn.RUBIS_VIEWUSER
		//		bidder := b.users[int(ddtxn.RandN(local_seed, b.sp))+w*int(b.sp)]
		bidder := b.users[int(ddtxn.RandN(local_seed, uint32(b.nbidders)))]
		txn.U1 = uint64(bidder)
	} else {
		log.Fatalf("No such transaction\n")
	}
}

func (b *Rubis) Add(t ddtxn.Query) {
	if t.TXN == ddtxn.RUBIS_BID {
		x := b.pidIdx[t.U2]
		atomic.AddInt32(&b.num_bids[x], 1)
		for int32(t.U3) > b.maxes[x] {
			v := atomic.LoadInt32(&b.maxes[x])
			done := atomic.CompareAndSwapInt32(&b.maxes[x], v, int32(t.U3))
			if done {
				break
			}
		}
	} else if t.TXN == ddtxn.RUBIS_COMMENT {
		b.Lock()
		b.ratings[t.U1] += 1
		b.Unlock()
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
	for i := 0; i < b.nproducts; i++ {
		j := b.products[i]
		var x int32
		k := ddtxn.MaxBidKey(j)
		v, err := s.Get(k)
		if err != nil {
			if b.maxes[i] != 0 {
				fmt.Printf("Validating key %v failed; store: none should have: %v\n", k, b.maxes[i])
				good = false
			}
			continue
		}
		x = v.Value().(int32)
		if x != b.maxes[i] {
			fmt.Printf("Validating key %v failed; store: %v should have: %v\n", k, x, b.maxes[i])
			good = false
		}
		if x == 0 {
			dlog.Printf("Saying x is zero %v %v\n", x, zero_cnt)
			zero_cnt++
		}
		k = ddtxn.NumBidsKey(j)
		v, err = s.Get(k)
		if err != nil {
			if b.maxes[i] != 0 {
				fmt.Printf("Validating key %v failed for max bid; store: none should have: %v\n", k, b.num_bids[i])
				good = false
			}
			continue
		}
		x = v.Value().(int32)
		if x != b.num_bids[i] {
			fmt.Printf("Validating key %v failed for number of bids; store: %v should have: %v\n", k, x, b.num_bids[i])
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

func (b *Rubis) PreAllocate(coord *ddtxn.Coordinator, bidrate float64, rounds bool) {
	prealloc := time.Now()
	users_per_worker := 100000.0
	bids_per_worker := 200000.0
	if bidrate > 20 {
		users_per_worker = 100000
		bids_per_worker = 2000000
	} else if b.nworkers <= 4 {
		users_per_worker = users_per_worker * 1.5
		bids_per_worker = bids_per_worker * 1.5
	} else if b.nworkers == 10 {
		users_per_worker = 1000
		bids_per_worker = 1000
	} else if b.nworkers >= 70 {
		users_per_worker = users_per_worker * 1.5
		bids_per_worker = bids_per_worker * 1.5
	} else if b.nworkers >= 50 {
		//			users_per_worker = users_per_worker * .75
		//			bids_per_worker = bids_per_worker * .75
	} else if b.nworkers == 20 {
		// want to be same sized map as 80 to test hypothesis
		//			bids_per_worker *= 5
		//			users_per_worker *= 2
		users_per_worker = users_per_worker * 4 * 1.5
		bids_per_worker = bids_per_worker * 4 * 1.5
	}
	fmt.Printf("%v bids, %v users\n", bids_per_worker*float64(b.nworkers), users_per_worker*float64(b.nworkers))
	if rounds {
		parallelism := 10
		rounds := b.nworkers / parallelism
		if rounds == 0 {
			rounds = 1
		}
		for j := 0; j < rounds; j++ {
			fmt.Printf("Doing round %v\n", j)
			var wg sync.WaitGroup
			for i := j * parallelism; i < (j+1)*parallelism; i++ {
				if i >= b.nworkers {
					break
				}
				wg.Add(1)
				go func(i int) {
					coord.Workers[i].PreallocateRubis(int(users_per_worker), int(bids_per_worker), b.nbidders)
					wg.Done()
				}(i)
			}
			wg.Wait()
		}
	} else {
		var wg sync.WaitGroup
		for i := 0; i < b.nworkers; i++ {
			wg.Add(1)
			go func(i int) {
				coord.Workers[i].PreallocateRubis(int(users_per_worker), int(bids_per_worker), b.nbidders)
				wg.Done()
			}(i)
		}
		wg.Wait()
	}
	fmt.Printf("Allocation took %v\n", time.Since(prealloc))
}
