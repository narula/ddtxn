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
			var wg_inner sync.WaitGroup
			w := c.Workers[id]
			for i := 0; i < b.N/3; i++ {
				p := ProductKey(i % np)
				u := UserKey(i % nb)
				amt := int32(rand.Intn(100))
				wg_inner.Add(1)
				tx := Transaction{TXN: D_BUY, U: u, A: amt, P: p, W: make(chan *Result), T: 0}
				w.Incoming <- tx
				go func(c chan *Result, i int, np int, amt int32) {
					r := <-c
					if r != nil && r.C {
						atomic.AddInt32(&val[i%np], amt)
					}
					dlog.Printf("Got result %v\n", r)
					wg_inner.Done()
				}(tx.W, i, np, amt)
			}
			dlog.Printf("Waiting on inner %d\n", id)
			wg_inner.Wait()
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
			var wg_inner sync.WaitGroup
			w := c.Workers[id]
			for i := 0; i < b.N/3; i++ {
				p := ProductKey(i % np)
				u := UserKey(i % nb)
				amt := int32(rand.Intn(100))
				wg_inner.Add(1)
				var tx Transaction
				rr := rand.Intn(100)
				val_txn := false
				if rr >= read_rate {
					tx = Transaction{TXN: D_BUY, U: u, P: p, A: amt, W: make(chan *Result), T: 0}
					val_txn = true
				} else {
					tx = Transaction{TXN: D_READ_FRESH, P: p, W: make(chan *Result), T: 0}
				}
				w.Incoming <- tx
				go func(c chan *Result, i int, np int, amt int32, val_txn bool) {
					r := <-c
					if val_txn && r != nil && r.C {
						atomic.AddInt32(&val[i%np], amt)
					}
					wg_inner.Done()
				}(tx.W, i, np, amt, val_txn)
			}
			dlog.Printf("Waiting on inner %d\n", id)
			wg_inner.Wait()
			dlog.Printf("%d Done\n", id)
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

func BenchmarkBid(b *testing.B) {
	nu := 10000
	np := 1000
	s := NewStore()
	c := NewCoordinator(1, s)
	w := c.Workers[0]
	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		p := uint64(rand.Intn(np))
		u := uint64(rand.Intn(nu))
		amt := int32(rand.Intn(100))
		var tx Transaction
		tx = Transaction{TXN: RUBIS_BID, U1: u, U2: p, A: amt, W: make(chan *Result)}
		w.Incoming <- tx
		wg.Add(1)
		go func(c chan *Result) {
			<-c
			wg.Done()
		}(tx.W)
	}
	wg.Wait()
}

func BenchmarkRegisterUser(b *testing.B) {
	s := NewStore()
	c := NewCoordinator(1, s)
	w := c.Workers[0]
	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		nickname := Randstr(8)
		var tx Transaction
		tx = Transaction{TXN: RUBIS_REGISTER, S1: nickname, W: make(chan *Result)}
		w.Incoming <- tx
		wg.Add(1)
		go func(c chan *Result) {
			<-c
			wg.Done()
		}(tx.W)
	}
	wg.Wait()
}
