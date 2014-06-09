package ddtxn

import (
	"ddtxn/dlog"
	"testing"
)

func TestBasic(t *testing.T) {
	s := NewStore()
	c := NewCoordinator(1, s)
	w := c.Workers[0]
	s.CreateKey(ProductKey(4), int32(0), SUM)
	s.CreateKey(ProductKey(5), int32(0), SUM)
	s.CreateKey(UserKey(1), "u1", WRITE)
	s.CreateKey(UserKey(2), "u2", WRITE)
	s.CreateKey(UserKey(3), "u3", WRITE)
	tx := Query{TXN: D_BUY, K1: UserKey(1), A: int32(5), K2: ProductKey(4), W: nil, T: 0}

	r, err := w.One(tx)
	_ = err
	// Fresh read test
	tx = Query{TXN: D_READ_ONE, K1: ProductKey(4), W: make(chan *Result), T: 0}
	r, err = w.One(tx)
	dlog.Printf("[test] Returned from one\n")
	if r.V.(int32) != 5 {
		t.Errorf("Wrong answer %v\n", r)
	}

	// Bidding
	tx = Query{TXN: D_BID, K1: BidKey(5), K2: MaxBidKey(5), W: nil, A: 27, S1: "bid on x"}
	r, err = w.One(tx)

	tx = Query{TXN: D_READ_ONE, K1: MaxBidKey(5), W: make(chan *Result)}
	r, err = w.One(tx)
	if r.V.(int32) != 27 {
		t.Errorf("Wrong answer %v\n", r)
	}
	tx = Query{TXN: D_BID, K1: BidKey(5), K2: MaxBidKey(5), W: make(chan *Result), A: 29, S1: "bid on x"}
	r, err = w.One(tx)
	tx = Query{TXN: D_READ_ONE, K1: MaxBidKey(5), W: make(chan *Result)}
	r, err = w.One(tx)
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

func TestUndoKey(t *testing.T) {
	var x uint64 = 655647
	k := CKey(x, 'p')
	if y := UndoCKey(k); y != x {
		t.Errorf("Mismatch: %v %v %v\n", x, k, y)
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

func TestStddev(t *testing.T) {
	x := make([]int64, 10)
	for i := 0; i < 10; i++ {
		x[i] = int64(i)
	}
	mean, stddev := StddevChunks(x)
	_ = mean
	_ = stddev
}

func TestAuction(t *testing.T) {
	s := NewStore()
	c := NewCoordinator(1, s)
	w := c.Workers[0]
	myname := "johnny appleseed"
	tx := Query{TXN: RUBIS_REGISTER, S1: myname, U1: 1}
	r, err := w.One(tx)
	if err != nil {
		t.Errorf("Register\n")
	}
	jaid := r.V.(uint64)

	tx = Query{TXN: RUBIS_NEWITEM, U1: jaid, S1: "burrito", S2: "slightly used burrito",
		U2: 1,   // initial price
		U3: 2,   // reserve price
		U4: 5,   // buy now price
		U5: 100, // duration
		U6: 10,  // quantity
		I:  42,  // enddate
		U7: 1,   // category
		W:  make(chan *Result)}
	r, err = w.One(tx)
	if err != nil {
		t.Errorf("New item\n")
	}
	burrito := r.V.(uint64)

	tx = Query{TXN: RUBIS_BID, U1: jaid, U2: burrito, A: 20}
	r, err = w.One(tx)
	if err != nil {
		t.Errorf("Bid %v\n", err)
	}
	tx = Query{TXN: D_READ_ONE, K1: MaxBidKey(burrito)}
	r, err = w.One(tx)
	if err != nil {
		t.Errorf("Get bid %v\n", err)
	}
	if r.V.(int32) != 20 {
		t.Errorf("Wrong max bid %v\n", r)
	}
}
