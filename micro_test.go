package ddtxn

import (
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/narula/ddtxn/dlog"
)

func BenchmarkMany(b *testing.B) {
	runtime.GOMAXPROCS(8)
	b.StopTimer()
	nb := 10000
	np := 100
	n := 8
	s := NewStore()
	// Load
	for i := 0; i < np; i++ {
		s.CreateKey(ProductKey(i), int32(0), SUM)
	}
	for i := 0; i < nb; i++ {
		s.CreateKey(UserKey(uint64(i)), "x", WRITE)
	}
	c := NewCoordinator(n, s)
	val := make([]int32, np)

	var wg sync.WaitGroup
	b.StartTimer()
	for p := 0; p < n; p++ {
		wg.Add(1)
		go func(id int) {
			w := c.Workers[id]
			for i := 0; i < b.N/3; i++ {
				p := ProductKey(i % np)
				u := UserKey(uint64(i % nb))
				amt := int32(rand.Intn(100))
				tx := Query{TXN: D_BUY, K1: u, A: amt, K2: p, W: nil, T: 0}
				_, err := w.One(tx)
				if err == nil {
					atomic.AddInt32(&val[i%np], amt)
				}
			}
			wg.Done()
		}(p)
	}
	wg.Wait()
	dlog.Printf("done\n")
	b.StopTimer()
	c.Finish()
	Validate(c, s, nb, np, val, b.N)
}

func BenchmarkBuy(b *testing.B) {
	runtime.GOMAXPROCS(8)
	b.StopTimer()
	nb := 10000
	np := 100
	n := 8
	s := NewStore()
	// Load
	for i := 0; i < np; i++ {
		s.CreateKey(ProductKey(i), int32(0), MAX)
	}
	for i := 0; i < nb; i++ {
		s.CreateKey(UserKey(uint64(i)), "x", WRITE)
	}
	c := NewCoordinator(n, s)
	val := make([]int32, np)

	var wg sync.WaitGroup
	b.StartTimer()
	for p := 0; p < n; p++ {
		wg.Add(1)
		go func(id int) {
			w := c.Workers[id]
			for i := 0; i < b.N/3; i++ {
				p := ProductKey(i % np)
				u := UserKey(uint64(i % nb))
				amt := int32(rand.Intn(100))
				tx := Query{TXN: D_BUY, K1: u, A: amt, K2: p, W: nil, T: 0}
				_, err := w.One(tx)
				if err == nil {
					atomic.AddInt32(&val[i%np], amt)
				}
			}
			wg.Done()
		}(p)
	}
	dlog.Printf("Waiting on outer\n")
	wg.Wait()
	dlog.Printf("done\n")
	b.StopTimer()
	c.Finish()
	Validate(c, s, nb, np, val, b.N)
	//PrintLockCounts(s, nb, np, false)
}

func BenchmarkRead(b *testing.B) {
	runtime.GOMAXPROCS(4)
	b.StopTimer()
	nb := 10000
	np := 100
	n := 4

	s := NewStore()
	// Load
	for i := 0; i < np; i++ {
		s.CreateKey(ProductKey(i), int32(0), SUM)
	}
	for i := 0; i < nb; i++ {
		s.CreateKey(UserKey(uint64(i)), "x", WRITE)
	}

	c := NewCoordinator(n, s)
	val := make([]int32, np)
	read_rate := 50

	var wg sync.WaitGroup
	b.StartTimer()
	for p := 0; p < n; p++ {
		wg.Add(1)
		go func(id int) {
			w := c.Workers[id]
			for i := 0; i < b.N/3; i++ {
				p := ProductKey(i % np)
				u := UserKey(uint64(i % nb))
				amt := int32(rand.Intn(100))
				var tx Query
				rr := rand.Intn(100)
				if rr >= read_rate {
					tx = Query{TXN: D_BUY, K1: u, K2: p, A: amt, W: nil, T: 0}
					_, err := w.One(tx)
					if err == nil {
						atomic.AddInt32(&val[i%np], amt)
					}
				} else {
					tx = Query{TXN: D_READ_ONE, K1: p, W: make(chan struct {
						R *Result
						E error
					}), T: 0}
					_, err := w.One(tx)
					if err == ESTASH {
						dlog.Printf("client [%v] waiting for %v; epoch %v\n", w.ID, i%np, w.epoch)
						<-tx.W
					}
				}
			}
			wg.Done()
		}(p)
	}
	dlog.Printf("Waiting on outer\n")
	wg.Wait()
	b.StopTimer()
	c.Finish()
	Validate(c, s, nb, np, val, b.N)
}

func BenchmarkList(b *testing.B) {
	x := Entry{0, SKey("z"), 0}
	lr := MakeBR(SKey("x"), x, LIST)
	v := make([]Entry, 1)
	for i := 0; i < b.N; i++ {
		v[0].order = i
		v[0].key = SKey(strconv.Itoa(i))
		lr.Apply(v)
	}
}
