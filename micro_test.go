package ddtxn

import (
	"ddtxn/dlog"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

func BenchmarkMany(b *testing.B) {
	b.StopTimer()
	nb := 10000
	np := 100
	n := 8
	s := loadStore(nb, np)
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
				u := UserKey(i % nb)
				amt := int32(rand.Intn(100))
				tx := Query{TXN: D_BUY, K1: u, A: amt, K2: p, W: nil, T: 0}
				r, err := w.One(tx)
				if r != nil && err == nil {
					atomic.AddInt32(&val[i%np], amt)
				}
			}
			dlog.Printf("%d Done\n", id)
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

func BenchmarkBid(b *testing.B) {
	b.StopTimer()
	nb := 10000
	np := 100
	n := 8
	s := loadStore(nb, np)
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
				u := UserKey(i % nb)
				amt := int32(rand.Intn(100))
				tx := Query{TXN: D_BID, K1: u, A: amt, K2: p, S1: "xx", W: nil, T: 0}
				r, err := w.One(tx)
				if r != nil && err == nil {
					// Change to CAS
					done := false
					for !done {
						x := atomic.LoadInt32(&val[i%np])
						if amt > x {
							done = atomic.CompareAndSwapInt32(&val[i%np], x, amt)
						} else {
							done = true
						}
					}
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

func BenchmarkBidNC(b *testing.B) {
	b.StopTimer()
	nb := 10000
	np := 100
	n := 8
	s := NewStore()
	//loadStore(nb, np)
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
				u := UserKey(i % nb)
				amt := int32(rand.Intn(100))
				tx := Query{TXN: D_BID_NC, K1: u, A: amt, K2: p, S1: "xx", W: nil, T: 0}
				r, err := w.One(tx)
				if r != nil && err == nil {
					// Change to CAS
					done := false
					for !done {
						x := atomic.LoadInt32(&val[i%np])
						if amt > x {
							done = atomic.CompareAndSwapInt32(&val[i%np], x, amt)
						} else {
							done = true
						}
					}
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
	b.StopTimer()
	nb := 10000
	np := 100
	n := 8
	s := loadStore(nb, np)
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
				u := UserKey(i % nb)
				amt := int32(rand.Intn(100))
				var tx Query
				rr := rand.Intn(100)
				if rr >= read_rate {
					tx = Query{TXN: D_BUY, K1: u, K2: p, A: amt, W: nil, T: 0}
					r, err := w.One(tx)
					if r != nil && err == nil {
						atomic.AddInt32(&val[i%np], amt)
					}
				} else {
					tx = Query{TXN: D_READ_ONE, K1: p, W: make(chan struct {
						R *Result
						E error
					}), T: 0}
					_, err := w.One(tx)
					if err == ESTASH {
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
