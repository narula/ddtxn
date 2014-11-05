package main

import (
	"container/heap"
	"ddtxn"
	"ddtxn/dlog"
	"ddtxn/prof"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"
)

var nprocs = flag.Int("nprocs", 2, "GOMAXPROCS default 2")
var nsec = flag.Int("nsec", 2, "Time to run in seconds")
var clientGoRoutines = flag.Int("ngo", 0, "Number of goroutines/workers generating client requests.")
var nworkers = flag.Int("nw", 0, "Number of workers")
var nbidders = flag.Int("nb", 1000000, "Keys in store, default is 1M")
var prob = flag.Float64("contention", 100.0, "Probability contended key is in txn. -1 means zipfian distribution and we use ZipfDist")
var readrate = flag.Int("rr", 0, "Read rate %.  Rest are writes")
var dataFile = flag.String("out", "xdata.out", "Filename for output")
var atomicIncr = flag.Bool("atomic", false, "Workload of just atomic increments")
var notcontended_readrate = flag.Float64("ncrr", .8, "NOT USED")

var ZipfDist = flag.Float64("zipf", 1, "Zipfian distribution theta.  1 means only 1 hot key and we'll vary the percentage (single exp)")
var partition = flag.Bool("partition", false, "Whether or not to partition the non-contended keys amongst the cores")

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*nprocs)

	if *clientGoRoutines == 0 {
		*clientGoRoutines = *nprocs
	}
	if *nworkers == 0 {
		*nworkers = *nprocs
	}
	if *prob == -1 && *ZipfDist < 0 {
		log.Fatalf("Zipf distribution must be positive")
	}
	if *ZipfDist >= 0 && *prob > -1 {
		log.Fatalf("Set contention to -1 to use Zipf distribution of keys")
	}
	s := ddtxn.NewStore()
	for i := 0; i < *nbidders; i++ {
		k := ddtxn.ProductKey(i)
		s.CreateKey(k, int32(0), ddtxn.SUM)
	}
	dlog.Printf("Done with Populate")

	coord := ddtxn.NewCoordinator(*nworkers, s)

	if *ddtxn.CountKeys {
		for i := 0; i < *nworkers; i++ {
			w := coord.Workers[i]
			w.NKeyAccesses = make([]int64, *nbidders)
		}
	}

	dlog.Printf("Done initializing single\n")

	p := prof.StartProfile()
	start := time.Now()
	sp := uint32(*nbidders / *nworkers)
	var wg sync.WaitGroup
	pkey := int(sp - 1)
	dlog.Printf("Partition size: %v; Contended key %v\n", sp/2, pkey)
	gave_up := make([]int64, *clientGoRoutines)

	goZipf := make([]*ddtxn.Zipf, *clientGoRoutines)
	if *prob == -1 && *ZipfDist >= 0 {
		for i := 0; i < *clientGoRoutines; i++ {
			rnd := rand.New(rand.NewSource(int64(i * 12467)))
			goZipf[i] = ddtxn.NewZipf(rnd, *ZipfDist, 1, uint64(*nbidders)-1)
			if goZipf[i] == nil {
				panic("nil zipf")
			}
		}
	}

	for i := 0; i < *clientGoRoutines; i++ {
		wg.Add(1)
		go func(n int) {
			exp := ddtxn.MakeExp(50)
			retries := make(ddtxn.RetryHeap, 0)
			heap.Init(&retries)
			var local_seed uint32 = uint32(rand.Intn(10000000))
			wi := n % (*nworkers)
			w := coord.Workers[wi]
			top := (wi + 1) * int(sp)
			bottom := wi * int(sp)
			dlog.Printf("%v: Noncontended section: %v to %v\n", n, bottom, top)
			end_time := time.Now().Add(time.Duration(*nsec) * time.Second)
			for {
				tm := time.Now()
				if !end_time.After(tm) {
					break
				}
				var t ddtxn.Query
				if len(retries) > 0 && retries[0].TS.Before(tm) {
					t = heap.Pop(&retries).(ddtxn.Query)
				} else {
					x := float64(ddtxn.RandN(&local_seed, 100))
					if *prob == -1 {
						x := goZipf[n].Uint64()
						if x >= uint64(*nbidders) || x < 0 {
							log.Fatalf("x not in bounds: %v\n", x)
						}
						t.K1 = ddtxn.ProductKey(int(x))
					} else if x < *prob {
						// contended txn
						t.K1 = ddtxn.ProductKey(pkey)
					} else {
						// uncontended
						k := pkey
						for k == pkey {
							if *partition {
								rnd := ddtxn.RandN(&local_seed, sp-1)
								lb := int(rnd)
								k = lb + wi*int(sp) + 1
								if k < bottom || k >= top+1 {
									log.Fatalf("%v: outside my range %v [%v-%v]\n", n, k, bottom, top)
								}
							} else {
								k = int(ddtxn.RandN(&local_seed, uint32(*nbidders)))
							}
						}
						t.K1 = ddtxn.ProductKey(k)
					}
					t.TXN = ddtxn.D_INCR_ONE
					if *atomicIncr {
						t.TXN = ddtxn.D_ATOMIC_INCR_ONE
					}
					y := int(ddtxn.RandN(&local_seed, 100))
					if y < *readrate {
						t.TXN = ddtxn.D_READ_ONE
					}
				}
				committed := false
				_, err := w.One(t)
				if err == ddtxn.EABORT {
					committed = false
				} else {
					committed = true
				}
				t.I++
				if !committed {
					e := exp.Exp(t.I)
					if e <= 0 {
						e = 1
					}
					rnd := ddtxn.RandN(&local_seed, e)
					if rnd <= 0 {
						rnd = 1
					}
					t.TS = tm.Add(time.Duration(rnd) * time.Microsecond)
					if t.TS.Before(end_time) {
						heap.Push(&retries, t)
					} else {
						gave_up[n]++
					}
				}
			}
			w.Finished()
			wg.Done()
			if len(retries) > 0 {
				dlog.Printf("[%v] Length of retry queue on exit: %v\n", n, len(retries))
			}
			gave_up[n] = gave_up[n] + int64(len(retries))
		}(i)
	}
	wg.Wait()
	coord.Finish()
	end := time.Since(start)
	p.Stop()

	stats := make([]int64, ddtxn.LAST_STAT)
	nitr, nwait, _, _, _, _, _ := ddtxn.CollectCounts(coord, stats)

	for i := 1; i < *clientGoRoutines; i++ {
		gave_up[0] = gave_up[0] + gave_up[i]
	}

	// nitr + NABORTS + ENOKEY is how many requests were issued.  A
	// stashed transaction eventually executes and contributes to
	// nitr.
	out := fmt.Sprintf(" nworkers: %v, nwmoved: %v, nrmoved: %v, sys: %v, total/sec: %v, abortrate: %.2f, stashrate: %.2f, rr: %v, nkeys: %v, contention: %v, zipf: %v, done: %v, actual time: %v, nreads: %v, nincrs: %v, epoch changes: %v, throughput ns/txn: %v, naborts: %v, coord time: %v, coord stats time: %v, total worker time transitioning: %v, nstashed: %v, rlock: %v, wrratio: %v, nsamples: %v, getkeys: %v, ddwrites: %v, nolock: %v, failv: %v, nlocked: %v, stashdone: %v, nfast: %v, gaveup: %v, potential: %v ", *nworkers, ddtxn.WMoved, ddtxn.RMoved, *ddtxn.SysType, float64(nitr)/end.Seconds(), 100*float64(stats[ddtxn.NABORTS])/float64(nitr+stats[ddtxn.NABORTS]), 100*float64(stats[ddtxn.NSTASHED])/float64(nitr+stats[ddtxn.NABORTS]), *readrate, *nbidders, *prob, *ZipfDist, nitr, end, stats[ddtxn.D_READ_ONE], stats[ddtxn.D_INCR_ONE], ddtxn.NextEpoch, end.Nanoseconds()/nitr, stats[ddtxn.NABORTS], ddtxn.Time_in_IE, ddtxn.Time_in_IE1, nwait, stats[ddtxn.NSTASHED], *ddtxn.UseRLocks, *ddtxn.WRRatio, stats[ddtxn.NSAMPLES], stats[ddtxn.NGETKEYCALLS], stats[ddtxn.NDDWRITES], stats[ddtxn.NO_LOCK], stats[ddtxn.NFAIL_VERIFY], stats[ddtxn.NLOCKED], stats[ddtxn.NDIDSTASHED], ddtxn.Nfast, gave_up[0], coord.PotentialPhaseChanges)
	fmt.Printf(out)
	fmt.Printf("\n")

	f, err := os.OpenFile(*dataFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	ddtxn.PrintStats(out, stats, f, coord, s, *nbidders)
}
