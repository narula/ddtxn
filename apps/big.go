package apps

import (
	"ddtxn"
	"ddtxn/stats"
	"flag"
	"fmt"
	"time"
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
	lhr             []*stats.LatencyHist
	lhw             []*stats.LatencyHist
}

func (b *Big) Init(ni, np, nw, rr, ngo int, ncrr float64) {
	b.ni = uint64(ni)
	b.np = np
	b.nworkers = nw
	b.ngo = ngo
	b.read_rate = rr
	b.ncontended_rate = int(ncrr * float64(rr))
	b.lhr = make([]*stats.LatencyHist, ngo)
	b.lhw = make([]*stats.LatencyHist, ngo)
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
	if *Latency {
		b.SetupLatency(100, 1000000, b.ngo)
	}
}

func (b *Big) SetupLatency(nincr int64, nbuckets int64, ngo int) {
	for i := 0; i < ngo; i++ {
		b.lhr[i] = stats.MakeLatencyHistogram(nincr, nbuckets)
		b.lhw[i] = stats.MakeLatencyHistogram(nincr, nbuckets)
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

func (b *Big) Time(t *ddtxn.Query, txn_end time.Duration, n int) {
	if t.TXN == ddtxn.D_READ_ONE {
		b.lhr[n].AddOne(txn_end.Nanoseconds())
	} else {
		b.lhw[n].AddOne(txn_end.Nanoseconds())
	}
}

func (b *Big) LatencyString() (string, string) {
	if !*Latency {
		return "", ""
	}
	for i := 1; i < b.ngo; i++ {
		b.lhr[0].Combine(b.lhr[i])
		b.lhw[0].Combine(b.lhw[i])
	}
	return fmt.Sprintf("Read 25: %v\nRead 50: %v\nRead 75: %v\nRead 99: %v\n", b.lhr[0].GetPercentile(25), b.lhr[0].GetPercentile(50), b.lhr[0].GetPercentile(75), b.lhr[0].GetPercentile(99)), fmt.Sprintf("Write 25: %v\nWrite 50: %v\nWrite 75: %v\nWrite 99: %v\n", b.lhw[0].GetPercentile(25), b.lhw[0].GetPercentile(50), b.lhw[0].GetPercentile(75), b.lhw[0].GetPercentile(99))
}
