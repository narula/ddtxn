package ddtxn

import (
	"ddtxn/dlog"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestBasic(t *testing.T) {
	s := NewStore()
	c := NewCoordinator(1, s)
	w := c.Workers[0]
	s.LoadBuy([]Key{ProductKey(4), ProductKey(5)}, map[Key]Value{UserKey(1): "u1", UserKey(2): "u2", UserKey(3): "u3"})

	tx := Transaction{TXN: D_BUY, U: UserKey(1), A: int32(5), P: ProductKey(4), W: make(chan *Result), T: 0}
	w.Incoming <- tx
	r := <-tx.W

	// Fresh read test
	tx = Transaction{TXN: D_READ_FRESH, P: ProductKey(4), W: make(chan *Result), T: 0}
	w.Incoming <- tx
	r = <-tx.W
	if r.V.(int32) != 5 {
		t.Errorf("Wrong answer %v\n", r)
	}

	// Bidding
	tx = Transaction{TXN: D_BID, U: BidKey(5), P: MaxBidKey(5), W: make(chan *Result), A: 27, V: "bid on x"}
	w.Incoming <- tx
	r = <-tx.W

	tx = Transaction{TXN: D_READ_FRESH, P: MaxBidKey(5), W: make(chan *Result)}
	w.Incoming <- tx
	r = <-tx.W
	if r.V.(int32) != 27 {
		t.Errorf("Wrong answer %v\n", r)
	}
	tx = Transaction{TXN: D_BID, U: BidKey(5), P: MaxBidKey(5), W: make(chan *Result), A: 29, V: "bid on x"}
	w.Incoming <- tx
	r = <-tx.W
	tx = Transaction{TXN: D_READ_FRESH, P: MaxBidKey(5), W: make(chan *Result)}
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

func TestRubis(t *testing.T) {

	s := NewStore()
	c := NewCoordinator(1, s)
	w := c.Workers[0]

	myname := "johnny appleseed"
	tx := Transaction{TXN: RUBIS_REGISTER, S1: myname, U1: 1, W: make(chan *Result)}
	w.Incoming <- tx
	r := <-tx.W
	jaid := r.V.(uint64)

	tx = Transaction{TXN: RUBIS_NEWITEM, U1: jaid, S1: "burrito", S2: "slightly used burrito",
		U2: 1,   // initial price
		U3: 2,   // reserve price
		U4: 5,   // buy now price
		U5: 100, // duration
		U6: 10,  // quantity
		I:  42,  // enddate
		U7: 1,   // category
		W:  make(chan *Result)}
	w.Incoming <- tx
	r = <-tx.W
	burrito := uint64(r.T)

	tx = Transaction{TXN: RUBIS_BID, U1: jaid, U2: burrito, A: 20, W: make(chan *Result)}
	w.Incoming <- tx
	r = <-tx.W

	tx = Transaction{TXN: RUBIS_VIEW, U1: burrito, W: make(chan *Result)}
	w.Incoming <- tx
	r = <-tx.W
	a := r.V.(*struct {
		Item
		int32
		uint64
	})

	if a.int32 != 20 || a.uint64 != jaid {
		t.Errorf("Wrong answer %v\n", r)
	}

	tx = Transaction{TXN: RUBIS_BIDHIST, U1: burrito, W: make(chan *Result)}
	w.Incoming <- tx
	r = <-tx.W
	b := r.V.(*struct {
		bids []Bid
		nns  []string
	})
	if len(b.nns) != 1 || len(b.bids) != 1 {
		t.Errorf("Evil length %v %v\n", len(b.nns), len(b.bids))
	}
	if b.nns[0] != myname {
		t.Errorf("Wrong answer %v\n", r)
	}

	if b.bids[0].Bidder != jaid || b.bids[0].Price != 20 {
		t.Errorf("Evil bid: %v\n", b.bids[0])
	}

	tx = Transaction{TXN: RUBIS_VIEWUSER, U1: jaid, W: make(chan *Result)}
	w.Incoming <- tx
	r = <-tx.W
	if r.V.(*User).Nickname != myname {
		t.Errorf("Evil nickname: %v\n", r.V.(*User).Nickname)
	}

	tx = Transaction{TXN: RUBIS_BUYNOW, U1: jaid, U2: burrito, U3: 1, W: make(chan *Result)}
	w.Incoming <- tx
	r = <-tx.W
	if r.V.(uint64) != 1 {
		t.Errorf("Evil quantity: %v\n", r.V.(int))
	}
	fmt.Printf("purchased burrito!\n")

	tx = Transaction{TXN: RUBIS_PUTBID, U1: burrito, W: make(chan *Result)}
	w.Incoming <- tx
	r = <-tx.W
	d := r.V.(*struct {
		string
		int32
		int
	})
	if d.string != myname || d.int32 != 20 || d.int != 1 {
		t.Errorf("Evil bid info: %v\n", d)
	}

	tx = Transaction{TXN: RUBIS_PUTCOMMENT, U1: jaid, U2: burrito, W: make(chan *Result)}
	w.Incoming <- tx
	r = <-tx.W
	{
		d := r.V.(*struct {
			nick  string
			iname string
		})
		if d.nick != myname || d.iname != "burrito" {
			t.Errorf("Evil comment info: %v\n", d)
		}
	}

	tx = Transaction{TXN: RUBIS_COMMENT, U1: jaid, U2: 42, U3: burrito, S1: "gut",
		U4: 10, W: make(chan *Result)}
	w.Incoming <- tx
	r = <-tx.W

	tx = Transaction{TXN: RUBIS_SEARCHCAT, U1: 1, U2: 10, W: make(chan *Result)}
	w.Incoming <- tx
	r = <-tx.W
	{
		d := r.V.(*struct {
			items   []*Item
			maxbids []int32
			numbids []int32
		})
		if len(d.items) != 1 || d.items[0].Name != "burrito" || d.items[0].Categ != 1 ||
			d.maxbids[0] != 20 || d.numbids[0] != 1 {
			t.Errorf("Evil item info: %v\n", d)
		}
	}

	tx = Transaction{TXN: RUBIS_SEARCHREG, U1: 1, U2: 1, U3: 10, W: make(chan *Result)}
	w.Incoming <- tx
	r = <-tx.W
	{
		d := r.V.(*struct {
			items   []*Item
			maxbids []int32
			numbids []int32
		})
		if len(d.items) != 1 || d.items[0].Name != "burrito" || d.items[0].Categ != 1 ||
			d.maxbids[0] != 20 || d.numbids[0] != 1 {
			t.Errorf("Evil item info: %v\n", d)
		}
	}
}

func TestOCC(t *testing.T) {
	*SysType = OCC
	s := NewStore()
	c := NewCoordinator(1, s)
	w := c.Workers[0]
	s.LoadBuy([]Key{ProductKey(4), ProductKey(5)}, map[Key]Value{UserKey(1): "u1", UserKey(2): "u2", UserKey(3): "u3"})

	tx := Transaction{TXN: OCC_BUY, U: UserKey(1), A: int32(5), P: ProductKey(4), W: make(chan *Result), T: 0}
	w.Incoming <- tx
	r := <-tx.W

	// Fresh read test
	tx = Transaction{TXN: OCC_READ_BUY, P: ProductKey(4), U: UserKey(1), W: make(chan *Result), T: 0}
	w.Incoming <- tx
	r = <-tx.W
	if r.V.(int32) != 5 {
		t.Errorf("Wrong answer %v\n", r)
	}
	*SysType = DOPPEL
}

func TestLocking(t *testing.T) {
	*SysType = LOCKING
	s := NewStore()
	c := NewCoordinator(1, s)
	w := c.Workers[0]
	s.LoadBuy([]Key{ProductKey(4), ProductKey(5)}, map[Key]Value{UserKey(1): "u1", UserKey(2): "u2", UserKey(3): "u3"})

	tx := Transaction{TXN: LOCK_BUY, U: UserKey(1), A: int32(5), P: ProductKey(4), W: make(chan *Result), T: 0}
	w.Incoming <- tx
	r := <-tx.W

	// Fresh read test
	tx = Transaction{TXN: LOCK_READ, P: ProductKey(4), W: make(chan *Result), T: 0}
	w.Incoming <- tx
	r = <-tx.W
	if r.V.(int32) != 5 {
		t.Errorf("Wrong answer %v\n", r)
	}
	*SysType = DOPPEL
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
	ts.Add(Transaction{P: SKey("product")})
	if ts.t[0].P != SKey("product") {
		t.Errorf("Wrong value %v\n", ts.t)
	}
}

func TestBid(t *testing.T) {
	s := NewStore()
	c := NewCoordinator(1, s)
	w := c.Workers[0]
	txn := Transaction{TXN: RUBIS_BID, U1: 1, U2: 2, A: 30, W: make(chan *Result)}
	w.Incoming <- txn
	r := <-txn.W
	bid_key := MaxBidKey(uint64(2))
	txn = Transaction{TXN: D_READ_FRESH, P: bid_key, W: make(chan *Result)}
	w.Incoming <- txn
	r = <-txn.W
	if r.V.(int32) != 30 {
		t.Errorf("Wrong max bid\n")
	}
	bids := BidsPerItemKey(uint64(2))
	txn = Transaction{TXN: D_READ_FRESH, P: bids, W: make(chan *Result)}
	w.Incoming <- txn
	r = <-txn.W
	list := r.V.([]Entry)
	if list[0].order != 30 {
		t.Errorf("Wrong entry in list, %v\n", list)
	}
	for i := 0; i < 100; i++ {
		txn = Transaction{TXN: RUBIS_BID, U1: uint64(i), U2: 2, A: int32(i), W: make(chan *Result)}
		w.Incoming <- txn
		<-txn.W
	}
	txn = Transaction{TXN: D_READ_FRESH, P: bids, W: make(chan *Result)}
	w.Incoming <- txn
	r = <-txn.W
	list = r.V.([]Entry)
	if list[0].order != 99 || len(list) != DEFAULT_LIST_SIZE {
		t.Errorf("Wrong entry in list %v\n", list)
	}
}

func TestLoadRubis(t *testing.T) {
	s := NewStore()
	c := NewCoordinator(1, s)
	w := c.Workers[0]
	nusers := 10
	nitems := 20
	users, items := s.LoadRubis(c, nusers, nitems)
	if len(users) != nusers {
		t.Errorf("x")
	}
	if len(items) != nitems {
		t.Errorf("y")
	}

	for i := 0; i < 10; i++ {
		item := items[rand.Intn(nitems)]
		r := w.TxnViewItem(item, false)
		_ = r
		//fmt.Println(r.V)
	}
}

// A data structure to hold a key/value pair.
type Pair struct {
	Key   string
	Value float64
}

// A slice of Pairs that implements sort.Interface to sort by Value.
type PairList []Pair

func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value > p[j].Value }

// A function to turn a map into a PairList, then sort and return it.
func sortMapByValue(m map[string]float64) PairList {
	p := make(PairList, len(m))
	i := 0
	for k, v := range m {
		p[i] = Pair{k, v}
		i++
	}
	sort.Sort(p)
	return p
}

func TestPrintRubisActual(t *testing.T) {
	parseTransitions2("data/default_transitions_15.txt")
	dlog.Println(pages)
	dlog.Println("Bidding mix:")
	r := GetTxns(false)
	for i, r := range r {
		dlog.Println(i, r)
	}

	dlog.Println("Skewed mix:")
	r = GetTxns(true)
	for i, r := range r {
		dlog.Println(i, r)
	}
}

func TestRubiSomething(t *testing.T) {
	states := parseTransitions2("data/default_transitions_15.txt")
	tr := MakeT(states)
	times := make([]int, len(pages))
	n := 100000
	for i := 0; i < n; i++ {
		x := tr.next()
		times[x]++
	}

	sigh := make(map[string]float64)
	for i := 0; i < len(pages); i++ {
		sigh[pages[i]] = 100 * (float64(times[i]) / float64(n))
	}

	yy := sortMapByValue(sigh)
	for _, k := range yy {
		dlog.Println(k.Value, "\t", k.Key)
	}
}

func TestRT2(t *testing.T) {
	states := parseTransitions("data/default_transitions.txt")
	tr := MakeT(states)
	times := make([]int, len(pages))
	n := 100000
	for i := 0; i < n; i++ {
		x := tr.next()
		times[x]++
	}

	sigh := make(map[string]float64)
	for i := 0; i < len(pages); i++ {
		sigh[pages[i]] = 100 * (float64(times[i]) / float64(n))
	}

	yy := sortMapByValue(sigh)
	for _, k := range yy {
		dlog.Println(k.Value, "\t", k.Key)
	}
}

func TestCKey(t *testing.T) {
	k1 := CKey(1111, 'b')
	_ = k1
	//fmt.Printf("%v\n", k1)
}
