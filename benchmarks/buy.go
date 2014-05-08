package main

import (
	"ddtxn"
	"ddtxn/prof"
	"ddtxn/stats"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var nprocs = flag.Int("nprocs", 2, "GOMAXPROCS default 2")
var nsec = flag.Int("nsec", 2, "Time to run in seconds")
var clientGoRoutines = flag.Int("ngo", 0, "Number of goroutines/workers generating client requests.")
var nworkers = flag.Int("nw", 0, "Number of workers")
var doValidate = flag.Bool("validate", false, "Validate")

var contention = flag.Int("contention", 1000, "Amount of contention, higher is more")
var nbidders = flag.Int("nb", 1000000, "Bidders in store, default is 1M")
var readrate = flag.Int("rr", 0, "Read rate %.  Rest are buys")
var notcontended_readrate = flag.Int("ncrr", 25, "Uncontended read rate %.  Default to half reads uncontended\n")

var latency = flag.Bool("latency", false, "Measure latency")

var nitr int64
var nreads int64
var nbuys int64
var naborts int64
var ncopies int64

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*nprocs)

	if *clientGoRoutines == 0 {
		*clientGoRoutines = *nprocs
	}
	if *nworkers == 0 {
		*nworkers = *nprocs
	}

	nproducts := *nbidders / *contention
	s := ddtxn.NewStore()
	portion_sz := *nbidders / *nworkers

	dd := make([]ddtxn.Key, nproducts)
	bidder_keys := make([]ddtxn.Key, *nbidders)

	for i := 0; i < nproducts; i++ {
		k := ddtxn.DistProductKey(i, nproducts)
		dd[i] = k
	}
	data := make(map[ddtxn.Key]ddtxn.Value)
	for i := 0; i < *nbidders; i++ {
		k := ddtxn.DistUserKey(i, *nbidders)
		data[k] = int32(0)
		bidder_keys[i] = k
	}
	s.LoadBuy(dd, data)
	coord := ddtxn.NewCoordinator(*nworkers, s)

	val := make([]int32, nproducts)

	p := prof.StartProfile()
	start := time.Now()
	var wg sync.WaitGroup

	lhr := make([]*stats.LatencyHist, *clientGoRoutines)
	lhw := make([]*stats.LatencyHist, *clientGoRoutines)

	var nincr int64 = 100
	var nbuckets int64 = 1000000

	for i := 0; i < *clientGoRoutines; i++ {
		if *latency {
			lhr[i] = stats.MakeLatencyHistogram(nincr, nbuckets)
			lhw[i] = stats.MakeLatencyHistogram(nincr, nbuckets)
		}
		wg.Add(1)
		go func(n int) {
			var local_seed uint32 = uint32(rand.Intn(1000000))
			done := time.NewTimer(time.Duration(*nsec) * time.Second).C
			wi := n % (*nworkers)
			w := coord.Workers[wi]
			for {
				select {
				case <-done:
					wg.Done()
					return
				default:
					var bidder int
					lb := int(ddtxn.RandN(&local_seed, uint32(portion_sz)))
					bidder = lb + wi*portion_sz
					amt := int32(ddtxn.RandN(&local_seed, 10))
					product := int(ddtxn.RandN(&local_seed, uint32(nproducts)))
					x := int(ddtxn.RandN(&local_seed, uint32(100)))

					var txn ddtxn.Transaction
					if x < *readrate {
						txn = ddtxn.Transaction{
							TXN: ddtxn.D_READ_FRESH,
							P:   dd[product],
						}
						if *latency {
							txn.W = make(chan *ddtxn.Result)
						}
						if x >= *notcontended_readrate {
							// Contended read; must execute in join phase
							switch *ddtxn.SysType {
							case ddtxn.OCC:
								txn.TXN = ddtxn.OCC_READ_FRESH
							case ddtxn.DOPPEL:
								txn.TXN = ddtxn.D_READ_FRESH
							case ddtxn.LOCKING:
								txn.TXN = ddtxn.LOCK_READ
							default:
								log.Fatalf("No such system %v\n", *ddtxn.SysType)
							}
						} else {
							// Uncontended read
							txn.P = bidder_keys[bidder]
							switch *ddtxn.SysType {
							case ddtxn.OCC:
								txn.TXN = ddtxn.OCC_READ_FRESH
							case ddtxn.DOPPEL:
								txn.TXN = ddtxn.D_READ_NP
							case ddtxn.LOCKING:
								txn.TXN = ddtxn.LOCK_READ
							default:
								log.Fatalf("No such system %v\n", *ddtxn.SysType)
							}
						}
					} else {
						txn = ddtxn.Transaction{
							TXN: ddtxn.D_BUY,
							U:   bidder_keys[bidder],
							A:   amt,
							P:   dd[product],
						}
						if *latency {
							txn.W = make(chan *ddtxn.Result)
						}
						switch *ddtxn.SysType {
						case ddtxn.OCC:
							txn.TXN = ddtxn.OCC_BUY
						case ddtxn.DOPPEL:
							txn.TXN = ddtxn.D_BUY
						case ddtxn.LOCKING:
							txn.TXN = ddtxn.LOCK_BUY
						default:
							log.Fatalf("No such system %v\n", *ddtxn.SysType)
						}
						if !*latency && *doValidate {
							atomic.AddInt32(&val[product], amt)
						}
					}
					if *latency {
						txn_start := time.Now()
						w.Incoming <- txn
						r := <-txn.W
						txn_end := time.Since(txn_start)
						if ddtxn.IsRead(txn.TXN) {
							lhr[n].AddOne(txn_end.Nanoseconds())
						} else {
							lhw[n].AddOne(txn_end.Nanoseconds())
						}
						if r.C == true && x >= *readrate && *doValidate {
							atomic.AddInt32(&val[product], amt)
						}

					} else {
						w.Incoming <- txn
					}
				}
			}
		}(i)
	}
	wg.Wait()
	coord.Finish()
	end := time.Since(start)
	p.Stop()
	for i := 0; i < *nworkers; i++ {
		nreads = nreads + coord.Workers[i].Nreads
		nbuys = nbuys + coord.Workers[i].Nbuys
		naborts = naborts + coord.Workers[i].Naborts
		ncopies = naborts + coord.Workers[i].Ncopy
	}
	nitr = nreads + nbuys
	if *doValidate {
		ddtxn.DistValidate(coord, s, *nbidders, nproducts, val, int(nitr))
	}

	fmt.Printf("sys: %v nworkers: %v, nbids: %v, nproducts: %v, contention: %v, done: %v, actual time: %v, nreads: %v, nbuys: %v, epoch changes: %v, total/sec %v, throughput ns/txn: %v, naborts %v, ncopies %v, nmoved %v.\n", *ddtxn.SysType, *nworkers, *nbidders, nproducts, *contention, nitr, end, nreads, nbuys, ddtxn.NextEpoch, float64(nitr)/end.Seconds(), end.Nanoseconds()/nitr, naborts, ncopies, ddtxn.Moved)
	//ddtxn.PrintLockCounts(s, *nbidders, nproducts, true)
	if *latency {
		for i := 1; i < *clientGoRoutines; i++ {
			lhr[0].Combine(lhr[i])
			lhw[0].Combine(lhw[i])
		}
		fmt.Printf("Read 25: %v, 50: %v, 75: %v, 99: %v\n", lhr[0].GetPercentile(25), lhr[0].GetPercentile(50), lhr[0].GetPercentile(75), lhr[0].GetPercentile(99))
		fmt.Printf("Write 25: %v, 50: %v, 75: %v, 99: %v\n", lhw[0].GetPercentile(25), lhw[0].GetPercentile(50), lhw[0].GetPercentile(75), lhw[0].GetPercentile(99))
	}
}
