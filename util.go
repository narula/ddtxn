package ddtxn

import (
	crand "crypto/rand"
	"ddtxn/dlog"
	"fmt"
	"math"
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

func UndoCKey(k Key) uint64 {
	b := [16]byte(k)
	var x uint64
	var i uint64
	for i = 0; i < 8; i++ {
		v := uint32(b[i])
		x = x + uint64(v<<(i*8))
	}
	return x
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

// For BUY and MAX tests
func loadStore(nb, np int) *Store {
	s := NewStore()
	// Load
	for i := 0; i < np; i++ {
		s.CreateKey(ProductKey(i), int32(0), SUM)
	}
	for i := 0; i < nb; i++ {
		s.CreateKey(UserKey(i), "x", WRITE)
	}
	return s
}

func Validate(c *Coordinator, s *Store, nkeys int, nproducts int, val []int32, n int) bool {
	good := true
	dlog.Printf("Validate start, store at %x\n", c.GetEpoch())
	zero_cnt := 0
	for j := 0; j < nproducts; j++ {
		var x int32
		k := ProductKey(j)
		v, err := s.getKey(k)
		if err != nil {
			if val[j] != 0 {
				fmt.Printf("Validating key %v failed; store: none should have: %v\n", k, val[j])
				good = false
			}
			continue
		}
		if *SysType != DOPPEL {
			x = v.Value().(int32)
		} else {
			x = v.Value().(int32)
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

func PrintLockCounts(s *Store) {
	fmt.Println()
	for _, chunk := range s.store {
		for k, v := range chunk.rows {
			if v.conflict > 0 {
				fmt.Printf("%v\t:%v\n", k, v.conflict)
			}
		}
	}
}

func StddevChunks(nc []int64) (int64, float64) {
	var total int64
	var n int64
	var i int64
	n = int64(len(nc))
	for i = 0; i < n; i++ {
		total += nc[i]
	}
	mean := total / n
	variances := make([]int64, n)

	for i = 0; i < n; i++ {
		x := nc[i] - mean
		if x < 0 {
			x = x * -1
		}
		x = x * x
		variances[i] = x
	}

	var stddev int64
	for i = 0; i < n; i++ {
		stddev += variances[i]
	}
	return mean, math.Sqrt(float64(stddev / n))
}
