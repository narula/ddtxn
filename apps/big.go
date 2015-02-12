package apps

import (
	"ddtxn"
	"flag"
)

var incr = flag.Bool("incr", true, "Do incr or RW workload")

type Big struct {
	sp              uint32
	read_rate       int
	ni              uint64
	np              int
	ncontended_rate int
	nworkers        int
	ngo             int
}

func (b *Big) Init(ni, np, nw, rr, ngo int, ncrr float64) {
	b.ni = uint64(ni)
	b.np = np
	b.nworkers = nw
	b.ngo = ngo
	b.read_rate = rr
	b.ncontended_rate = int(ncrr * float64(rr))
}

func (b *Big) Populate(s *ddtxn.Store, ex *ddtxn.ETransaction) {
	for i := 0; i < int(b.ni); i++ {
		k := ddtxn.BidKey(uint64(i))
		s.CreateKey(k, int32(0), ddtxn.SUM)
	}
	for i := 0; i < b.np; i++ {
		k := ddtxn.ProductKey(i)
		s.CreateKey(k, int32(0), ddtxn.SUM)
	}
}

// Calls rand 4 times
func (b *Big) MakeOne(w int, local_seed *uint32, txn *ddtxn.Query) {
	rnd := uint64(ddtxn.RandN(local_seed, uint32(b.ni)))
	txn.U1 = rnd
	txn.U2 = (rnd * 2) % b.ni
	txn.U3 = (rnd * 3) % b.ni
	txn.U4 = (rnd * 4) % b.ni
	txn.U5 = (rnd * 5) % b.ni
	txn.U6 = (rnd * 6) % b.ni
	txn.U7 = (rnd) % uint64(b.np)
	if *incr {
		txn.TXN = ddtxn.BIG_INCR
	} else {
		txn.TXN = ddtxn.BIG_RW
	}
}

func (b *Big) Add(t ddtxn.Query) {
}

func (b *Big) Validate(s *ddtxn.Store, nitr int) bool {
	return true
}
