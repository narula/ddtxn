package main

import (
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
var prob = flag.Float64("contention", 100.0, "Probability contended key is in txn")
var readrate = flag.Int("rr", 0, "Read rate %.  Rest are writes")
var dataFile = flag.String("out", "single-data.out", "Filename for output")
var latency = flag.Bool("latency", false, "dummy")
var Retry = flag.Bool("retry", false, "Whether to retry aborted transactions until they commit.  Changes the composition of reads/writes issued to the system (but maintains the read rate ratio specified for transactions *completed*. Default false.")
var AtomicIncr = flag.Bool("atomic", false, "Use atomic increment function instead (no aborts)")

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*nprocs)

	if *clientGoRoutines == 0 {
		*clientGoRoutines = *nprocs
	}
	if *nworkers == 0 {
		*nworkers = *nprocs
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
	for i := 0; i < *clientGoRoutines; i++ {
		wg.Add(1)
		go func(n int) {
			duration := time.Now().Add(time.Duration(*nsec) * time.Second)
			var local_seed uint32 = uint32(rand.Intn(10000000))
			wi := n % (*nworkers)
			w := coord.Workers[wi]
			top := (wi + 1) * int(sp)
			bottom := wi * int(sp)
			dlog.Printf("%v: Noncontended section: %v to %v\n", n, bottom, top)
			for duration.After(time.Now()) {
				var t ddtxn.Query
				x := float64(ddtxn.RandN(&local_seed, 100))
				if x < *prob {
					// contended txn
					t.K1 = ddtxn.ProductKey(pkey)
				} else {
					// uncontended
					rnd := ddtxn.RandN(&local_seed, sp/2)
					lb := int(rnd)
					k := lb + wi*int(sp) + 1
					if k < bottom || k >= top+1 {
						log.Fatalf("%v: outside my range %v [%v-%v]\n", n, k, bottom, top)
					}
					t.K1 = ddtxn.ProductKey(k)
				}
				if *AtomicIncr {
					t.TXN = ddtxn.D_ATOMIC_INCR_ONE
				} else {
					t.TXN = ddtxn.D_INCR_ONE
				}
				y := int(ddtxn.RandN(&local_seed, 100))
				if y < *readrate {
					t.TXN = ddtxn.D_READ_ONE
				}
				if *Retry {
					committed := false
					for !committed && duration.After(time.Now()) {
						_, err := w.One(t)
						if err == ddtxn.EABORT {
							if *AtomicIncr {
								log.Fatalf("Should not be able to abort here")
							}
							committed = false
						} else {
							committed = true
						}
					}
				} else {
					w.One(t)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	coord.Finish()
	end := time.Since(start)
	p.Stop()

	stats := make([]int64, ddtxn.LAST_STAT)
	nitr, nwait, _ := ddtxn.CollectCounts(coord, stats)

	// nitr + NABORTS + ENOKEY is how many requests were issued.  A
	// stashed transaction eventually executes and contributes to
	// nitr.
	out := fmt.Sprintf(" nworkers: %v, nwmoved: %v, nrmoved: %v, sys: %v, total/sec: %v, abortrate: %.2f, stashrate: %.2f, rr: %v, nkeys: %v, done: %v, actual time: %v, nreads: %v, nincrs: %v, epoch changes: %v, throughput ns/txn: %v, naborts: %v, coord time: %v, coord stats time: %v, total worker time transitioning: %v, nstashed: %v, rlock: %v, wrratio: %v, nsamples: %v, getkeys: %v, ddwrites: %v, nolock: %v, failv: %v, nlocked: %v ", *nworkers, ddtxn.WMoved, ddtxn.RMoved, *ddtxn.SysType, float64(nitr)/end.Seconds(), 100*float64(stats[ddtxn.NABORTS])/float64(nitr+stats[ddtxn.NABORTS]), 100*float64(stats[ddtxn.NSTASHED])/float64(nitr+stats[ddtxn.NABORTS]), *readrate, *nbidders, nitr, end, stats[ddtxn.D_READ_ONE], stats[ddtxn.D_INCR_ONE], ddtxn.NextEpoch, end.Nanoseconds()/nitr, stats[ddtxn.NABORTS], ddtxn.Time_in_IE, ddtxn.Time_in_IE1, nwait, stats[ddtxn.NSTASHED], *ddtxn.UseRLocks, *ddtxn.WRRatio, stats[ddtxn.NSAMPLES], stats[ddtxn.NGETKEYCALLS], stats[ddtxn.NDDWRITES], stats[ddtxn.NO_LOCK], stats[ddtxn.NFAIL_VERIFY], stats[ddtxn.NLOCKED])
	fmt.Printf(out)
	fmt.Printf("\n")

	f, err := os.OpenFile(*dataFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	ddtxn.PrintStats(out, stats, f, coord, s, *nbidders)
}
