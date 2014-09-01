package ddtxn

import (
	"ddtxn/dlog"
	"fmt"
	"testing"
)

func TestBasic(t *testing.T) {
	s := NewStore()
	c := NewCoordinator(1, s)
	w := c.Workers[0]
	s.CreateKey(ProductKey(4), int32(0), SUM)
	s.CreateKey(ProductKey(5), int32(0), WRITE)
	s.CreateKey(UserKey(1), "u1", WRITE)
	s.CreateKey(UserKey(2), "u2", WRITE)
	s.CreateKey(UserKey(3), "u3", WRITE)
	tx := Query{TXN: D_BUY, K1: UserKey(1), A: int32(5), K2: ProductKey(4), W: nil, T: 0}

	r, err := w.One(tx)
	_ = err
	// Fresh read test
	tx = Query{TXN: D_READ_ONE, K1: ProductKey(4), W: make(chan struct {
		R *Result
		E error
	}), T: 0}
	r, err = w.One(tx)
	dlog.Printf("[test] Returned from one\n")
	if r.V.(int32) != 5 {
		t.Errorf("Wrong answer %v\n", r)
	}
}

func TestRandN(t *testing.T) {
	var seed uint32 = uint32(1)
	dlog.Printf("seed %v\n", seed)
	for i := 0; i < 1000; i++ {
		x := RandN(&seed, 10)
		// No idea how to test a random number generator, just look at the results for now.
		dlog.Println(x, seed)
		_ = x
	}
}

func Test2RandN(t *testing.T) {
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

func TestStddev(t *testing.T) {
	x := make([]int64, 100)
	for i := 0; i < 100; i++ {
		x[i] = int64(i)
	}
	mean, stddev := StddevChunks(x)
	if mean != 49 {
		t.Errorf("Wrong mean %v\n", mean)
	}
	if stddev > 29 || stddev < 28 {
		t.Errorf("Wrong stddev %v\n", stddev)
	}
	mean, stddev = StddevKeys(x) // ignores 0s
	if mean != 49 {
		t.Errorf("Wrong mean %v\n", mean)
	}
	if stddev > 29 || stddev < 28 {
		t.Errorf("Wrong stddev %v\n", stddev)
	}
}

func TestAuction(t *testing.T) {
	s := NewStore()
	c := NewCoordinator(1, s)
	w := c.Workers[0]
	myname := uint64(12345)
	tx := Query{TXN: RUBIS_REGISTER, U2: myname, U1: 1}
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
		W:  nil}
	r, err = w.One(tx)
	if err != nil {
		t.Fatalf("New item %v\n", err)
	}
	burrito := r.V.(uint64)

	tx = Query{TXN: RUBIS_BID, U1: jaid, U2: burrito, A: 20}
	r, err = w.One(tx)
	if err != nil {
		t.Fatalf("Bid %v\n", err)
	}
	tx = Query{TXN: D_READ_ONE, K1: MaxBidKey(burrito)}
	r, err = w.One(tx)
	if err != nil {
		t.Errorf("Get bid %v\n", err)
	}
	if r.V.(int32) != 20 {
		t.Errorf("Wrong max bid %v\n", r)
	}
	tx = Query{TXN: RUBIS_SEARCHCAT, U1: 1, U2: 1}
	r, err = w.One(tx)
	if err != nil {
		t.Errorf("Search cat\n", err)
	}
	st := r.V.(*struct {
		items   []*Item
		maxbids []int32
		numbids []int32
	})
	if len(st.items) != 1 {
		for i := 0; i < len(st.items); i++ {
			fmt.Println(st.items[i])
		}
		t.Fatalf("Wrong length %v\n", st.items)
	}
	if st.numbids[0] != 1 {
		t.Errorf("Wrong numbids %v\n", st.numbids)
	}
	if st.maxbids[0] != 20 {
		t.Errorf("Wrong numbids %v\n", st.maxbids)
	}

	tx = Query{TXN: RUBIS_VIEWBIDHIST, U1: burrito}
	r, err = w.One(tx)
	if err != nil {
		t.Fatalf("View Bid Hist\n", err)
	}
	lst := r.V.(*struct {
		bids []Bid
		nns  []string
	})
	if len(lst.bids) != 1 || len(lst.nns) != 1 {
		t.Fatalf("Wrong length\n")
	}
	if lst.nns[0] != string(myname) {
		t.Errorf("Wrong nickname %v\n", lst.nns[0])
	}
	if lst.bids[0].Price != 20 {
		t.Errorf("Wrong price %v\n", lst.bids[0])
	}
}

func TestCandidates(t *testing.T) {
	h := make([]*OneStat, 0)
	sh := StatsHeap(h)
	c := Candidates{make(map[Key]*OneStat), &sh}
	k := ProductKey(1)
	br := &BRecord{}
	for i := 0; i < 10; i++ {
		c.Write(k, br)
	}
	c.Read(k, br)
	h2 := make([]*OneStat, 0)
	sh2 := StatsHeap(h2)
	c2 := Candidates{make(map[Key]*OneStat), &sh2}
	for i := 0; i < 9; i++ {
		c2.Write(k, br)
	}
	c.Merge(&c2)
}

func TestZipf(t *testing.T) {
	var n int64 = 1000
	x := NewZipf(n, .99)
	y := make([]int64, n)
	for i := 0; i < 1000000; i++ {
		z := x.NextSeeded()
		y[z-1]++
	}
	fmt.Println("\n.99:")
	var i int64
	for i = 0; i < n; i++ {
		if y[i] != 0 {
			fmt.Printf("%v ", y[i])
		}
	}
	fmt.Println("\n0:")
	x = NewZipf(n, 0)
	y = make([]int64, n)
	for i := 0; i < 1000000; i++ {
		z := x.NextSeeded()
		y[z-1]++
	}
	for i = 0; i < n; i++ {
		if y[i] != 0 {
			fmt.Printf("%v ", y[i])
		}
	}
}
