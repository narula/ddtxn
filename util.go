package ddtxn

import (
	crand "crypto/rand"
	"ddtxn/dlog"
	"fmt"
	"log"
	"math/rand"
	"sync"
)

func CKey(x uint64, ch rune) Key {
	var b [16]byte
	var i uint64
	for i = 0; i < 8; i++ {
		b[i] = byte((x >> (i * 8)))
	}
	b[8] = byte(ch)
	return Key(b)
}

func TKey(x uint64, y uint64) Key {
	var b [16]byte
	var i uint64
	for i = 0; i < 8; i++ {
		b[i] = byte((x >> (i * 8)))
	}
	for i = 8; i < 16; i++ {
		b[i] = byte((x >> (i * 8)))
	}
	return Key(b)
}

func SKey(s string) Key {
	var b [16]byte
	end := 16
	if len(s) < 16 {
		end = len(s)
	}
	for i := 0; i < end; i++ {
		b[i] = s[i]
	}
	return Key(b)
}

func UserKey(bidder int) Key {
	return CKey(uint64(bidder), 'u')
}

func DistUserKey(bidder int, total int) Key {
	return UserKey(bidder)
}

func DistProductKey(product int, total int) Key {
	return ProductKey(product)
}

func DistBidKey(bid int, total int) Key {
	return BidKey(uint64(bid))
}

func BidKey(id uint64) Key {
	return CKey(id, 'b')
}

func ItemKey(item uint64) Key {
	return CKey(item, 'i')
}

func ProductKey(product int) Key {
	return CKey(uint64(product), 'p')
}

func MaxBidKey(item uint64) Key {
	return CKey(item, 'm')
}

func NumBidsKey(item uint64) Key {
	return CKey(item, 'n')
}

func BidsPerItemKey(item uint64) Key {
	return CKey(item, 'p')
}

func MaxBidBidderKey(item uint64) Key {
	return CKey(item, 'a')
}

func BuyNowKey(item uint64) Key {
	return CKey(item, 'n')
}

func CommentKey(item uint64) Key {
	return CKey(item, 'c')
}

func ItemsByCatKey(item uint64) Key {
	return CKey(item, 't')
}

func ItemsByRegKey(region uint64, categ uint64) Key {
	return TKey(region, categ)
}

func RatingKey(user uint64) Key {
	return CKey(user, 'c')
}

func RandN(seed *uint32, n uint32) uint32 {
	*seed = *seed*1103515245 + 12345
	return ((*seed & 0x7fffffff) % (n * 2) / 2)
}

func Randstr(sz int) string {
	alphanum := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, sz)
	crand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bytes)
}

func (s *Store) LoadRubis(c *Coordinator, nusers int, nitems int) ([]uint64, []uint64) {
	old := *Allocate
	*Allocate = true

	for i := 0; i < NUM_CATEGORIES; i++ {
		for j := 0; j < NUM_REGIONS; j++ {
			ibrk := ItemsByRegKey(uint64(j), uint64(i))
			c.Workers[0].store.getOrCreateTypedKey(ibrk, nil, LIST)
		}
	}

	users := make([]uint64, nusers)
	items := make([]uint64, nitems)
	var wg sync.WaitGroup

	for i := 0; i < nusers; i++ {
		wg.Add(1)
		w := c.Workers[i%len(c.Workers)]
		go func(w *Worker, i int) {
			tx := Transaction{
				TXN: RUBIS_REGISTER,
				S1:  Randstr(7),
				U1:  uint64(rand.Intn(NUM_REGIONS)),
				W:   make(chan *Result),
			}
			w.Incoming <- tx
			r := <-tx.W
			if r == nil || !r.C {
				wg.Done()
				return
			}
			users[i] = r.V.(uint64)
			dlog.Printf("Created user %v\n", users[i])
			wg.Done()
		}(w, i)
	}
	wg.Wait()

	var wg2 sync.WaitGroup
	for i := 0; i < nitems; i++ {
		w := c.Workers[i%len(c.Workers)]
		wg2.Add(1)
		go func(w *Worker, i int) {
			price := uint64(rand.Intn(100))
			tx := Transaction{
				TXN: RUBIS_NEWITEM,
				U1:  users[rand.Intn(nusers)],
				S1:  Randstr(5),
				S2:  Randstr(10),
				U2:  price,
				U3:  price,
				U4:  price,
				U5:  100,
				U6:  100,
				I:   100,
				U7:  uint64(rand.Intn(NUM_CATEGORIES)),
				W:   make(chan *Result),
			}
			if *SysType == OCC {
				tx.TXN = RUBIS_NEWITEM_OCC
			}
			w.Incoming <- tx
			r := <-tx.W
			if r == nil || !r.C {
				wg2.Done()
				return
			}
			items[i] = r.V.(uint64)
			dlog.Printf("Created item %v\n", items[i])
			wg2.Done()
		}(w, i)
	}
	wg2.Wait()
	var wg3 sync.WaitGroup
	for i := 0; i < 100; i++ {
		w := c.Workers[i%len(c.Workers)]
		wg3.Add(1)
		go func(w *Worker, i int) {
			price := int32(rand.Intn(100))
			tx := Transaction{
				TXN: RUBIS_BID,
				U1:  users[rand.Intn(nusers)],
				A:   price,
				U2:  items[rand.Intn(nitems)],
				W:   make(chan *Result),
			}
			if *SysType == OCC {
				tx.TXN = RUBIS_BID_OCC
			}
			w.Incoming <- tx
			r := <-tx.W
			if r == nil || !r.C {
				wg3.Done()
				return
			}
			wg3.Done()
		}(w, i)
	}
	wg3.Wait()
	for _, w := range c.Workers {
		w.Nstats[RUBIS_BID] = 0
		w.Nstats[RUBIS_NEWITEM] = 0
		w.Nstats[RUBIS_REGISTER] = 0
	}
	*Allocate = old
	dlog.Printf("Done loading\n")
	return users, items
}

func ValidateRubis(s *Store, users []uint64, items []uint64) {
	max_per_product := make([]int32, len(items))
	for i := 0; i < len(items); i++ {
		k := MaxBidKey(items[i])
		v, err := s.getKey(k)
		if err != nil {
			dlog.Printf("Missing key %v\n", k)
			continue
		}
		max_per_product[i] = v.int_value
	}
	for i := 0; i < len(items); i++ {
		bids := BidsPerItemKey(items[i])
		d, err := s.getKey(bids)
		if err != nil {
			dlog.Printf("Missing key %v\n", bids)
			continue
		}
		lst := d.entries
		for j := 0; j < len(lst); j++ {
			b, err := s.getKey(lst[j].key)
			if err != nil {
				dlog.Printf("Missing key %v\n", lst[j].key)
				continue
			}
			bid := *b.value.(*Bid)
			if bid.Price > max_per_product[i] {
				log.Fatalf("Failed validate %v %v %v %v\n", bids, d.entries, bid, max_per_product[i])
			}
		}
	}
}

func (s *Store) LoadBuy(keys []Key, m map[Key]Value) {
	for _, k := range keys {
		s.getOrCreateTypedKey(k, int32(0), SUM)
	}
	for k, v := range m {
		b, err := s.getKey(k)
		if err == ENOKEY {
			b = MakeBR(k, v, WRITE)
			s.store[k[0]].rows[k] = b
		}
		b.value = v
	}
	dlog.Printf("Done loading\n")
}

// For BUY and MAX tests
func loadStore(nb, np int) *Store {
	s := NewStore()
	// Load
	dd := make([]Key, np)
	m := make(map[Key]Value)
	for i := 0; i < np; i++ {
		dd[i] = ProductKey(i)
	}
	for i := 0; i < nb; i++ {
		m[UserKey(i)] = "x"
	}
	s.LoadBuy(dd, m)
	return s
}

func Validate(c *Coordinator, s *Store, nkeys int, nproducts int, val []int32, n int) bool {
	good := true
	dlog.Printf("Validate start, store at %x\n", c.GetEpoch())
	zero_cnt := 0
	for j := 0; j < nproducts; j++ {
		var x int32
		k := ProductKey(j)
		v, _ := s.getKey(k)
		if *SysType != DOPPEL {
			x = v.value.(int32)
		} else {
			x = v.int_value
			dlog.Printf("Validate: %v %v\n", k, x)
		}
		if x != val[j] {
			fmt.Printf("Validating key %v failed; store: %v should have: %v\n", k, x, val[j])
			good = false
		}
		if x == 0 {
			dlog.Printf("Saying x is zero %v %v\n", x, zero_cnt)
			zero_cnt++
		}
	}
	if zero_cnt == nproducts && n > 10 {
		fmt.Printf("Bad: all zeroes!\n")
		dlog.Printf("Bad: all zeroes!\n")
		good = false
	}
	return good
}

func DistValidate(c *Coordinator, s *Store, nkeys int, nproducts int, val []int32, n int) bool {
	good := true
	dlog.Printf("Validate start, store at %x\n", c.GetEpoch())
	zero_cnt := 0
	for j := 0; j < nproducts; j++ {
		var x int32
		k := DistProductKey(j, nproducts)
		v, _ := s.getKey(k)
		x = v.int_value
		dlog.Printf("Validate: %v %v\n", k, x)
		if x != val[j] {
			fmt.Printf("Validating key %v failed; store: %v should have: %v\n", k, x, val[j])
			good = false
		}
		if x == 0 {
			dlog.Printf("Saying x is zero %v %v\n", x, zero_cnt)
			zero_cnt++
		}
	}
	if zero_cnt == nproducts && n > 10 {
		fmt.Printf("Bad: all zeroes!\n")
		dlog.Printf("Bad: all zeroes!\n")
		good = false
	}
	return good
}

func PrintLockCounts(s *Store, nkeys int, nproducts int, dist bool) {
	for _, chunk := range s.store {
		for k, v := range chunk.rows {
			if v.locked > 0 {
				fmt.Printf("%v\t:%v\n", k, v.locked)
			}
		}
	}
}
