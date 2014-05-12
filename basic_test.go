package ddtxn

import (
	"ddtxn/dlog"
	"testing"
)

func TestBasic(t *testing.T) {
	s := NewStore()
	c := NewCoordinator(1, s)
	w := c.Workers[0]
	s.LoadBuy([]Key{ProductKey(4), ProductKey(5)}, map[Key]Value{UserKey(1): "u1", UserKey(2): "u2", UserKey(3): "u3"})

	tx := Query{TXN: D_BUY, K1: UserKey(1), A: int32(5), K2: ProductKey(4), W: make(chan *Result), T: 0}
	w.Incoming <- tx
	r := <-tx.W

	// Fresh read test
	tx = Query{TXN: D_READ_BUY, K1: ProductKey(4), W: make(chan *Result), T: 0}
	w.Incoming <- tx
	r = <-tx.W
	if r.V.(int32) != 5 {
		t.Errorf("Wrong answer %v\n", r)
	}

	// Bidding
	tx = Query{TXN: D_BID, K1: BidKey(5), K2: MaxBidKey(5), W: make(chan *Result), A: 27, S1: "bid on x"}
	w.Incoming <- tx
	r = <-tx.W

	tx = Query{TXN: D_READ_BUY, K1: MaxBidKey(5), W: make(chan *Result)}
	w.Incoming <- tx
	r = <-tx.W
	if r.V.(int32) != 27 {
		t.Errorf("Wrong answer %v\n", r)
	}
	tx = Query{TXN: D_BID, K1: BidKey(5), K2: MaxBidKey(5), W: make(chan *Result), A: 29, S1: "bid on x"}
	w.Incoming <- tx
	r = <-tx.W
	tx = Query{TXN: D_READ_BUY, K1: MaxBidKey(5), W: make(chan *Result)}
	w.Incoming <- tx
	r = <-tx.W
	if r.V.(int32) != 29 {
		t.Errorf("Wrong answer %v\n", r)
	}
}

func TestRandN(t *testing.T) {
	var seed uint32 = uint32(1)
	dlog.Printf("seed %v\n", seed)
	for i := 0; i < 10; i++ {
		x := RandN(&seed, 100000)
		// No idea how to test a random number generator, just look at the results for now.
		dlog.Println(x, seed)
		_ = x
	}
}

func TestRandN2(t *testing.T) {
	n := 0
	var local_seed uint32 = uint32(n + 1)
	portion_sz := 100
	dlog.Printf("LOCAL: %v\n", local_seed)
	j := 0
	for {
		select {
		default:
			var bidder int
			rand := RandN(&local_seed, uint32(portion_sz))
			lb := int(rand)
			bidder = lb + n*portion_sz
			amt := int(RandN(&local_seed, 10))
			dlog.Printf("%v rand: %v bidder: %v local: %v amt: %v\n", n, rand, bidder, local_seed, amt)
			j++
			if j > 100 {
				return
			}
		}
	}
}

func TestListRecord(t *testing.T) {
	br := MakeBR(SKey("x"), Entry{1, SKey("y"), 0}, LIST)
	new_entries := make([]Entry, 5)
	for i := 4; i > 0; i-- {
		new_entries[4-i] = Entry{i, SKey("x"), 0}
	}
	br.Apply(new_entries)

	for i := 4; i > 0; i-- {
		new_entries[4-i] = Entry{i * 3, SKey("z"), 0}
	}
	br.Apply(new_entries)

	var x int = br.entries[0].order
	for i := 1; i < DEFAULT_LIST_SIZE; i++ {
		if br.entries[i].order > x {
			t.Errorf("Bad list %v\n", br.entries)
		}
		x = br.entries[i].order
	}
}

func TestTStore(t *testing.T) {
	ts := TSInit(10)
	if len(ts.t) != 0 {
		t.Errorf("Should have 0 length\n")
	}
	ts.Add(Query{K2: SKey("product")})
	if ts.t[0].K2 != SKey("product") {
		t.Errorf("Wrong value %v\n", ts.t)
	}
}
