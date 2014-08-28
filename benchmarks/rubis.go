package main

import (
	"container/heap"
	"ddtxn"
	"ddtxn/apps"
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
var doValidate = flag.Bool("validate", false, "Validate")

var contention = flag.Int("contention", 30, "Amount of contention, higher is more")
var nbidders = flag.Int("nb", 1000000, "Bidders in store, default is 1M")
var readrate = flag.Int("rr", 0, "Read rate %.  Rest are bids")
var notcontended_readrate = flag.Float64("ncrr", .8, "Uncontended read rate %.  Default to .8")
var dataFile = flag.String("out", "rubis-data.out", "Filename for output")
var atomicIncr = flag.Bool("atomic", false, "NOT USED")

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*nprocs)

	if *clientGoRoutines == 0 {
		*clientGoRoutines = *nprocs
	}
	if *nworkers == 0 {
		*nworkers = *nprocs
	}

	if *doValidate {
		if !*ddtxn.Allocate {
			log.Fatalf("Cannot correctly validate without waiting for results; add -allocate\n")
		}
	}

	nproducts := *nbidders / *contention
	s := ddtxn.NewStore()
	coord := ddtxn.NewCoordinator(*nworkers, s)

	if *ddtxn.CountKeys {
		for i := 0; i < *nworkers; i++ {
			w := coord.Workers[i]
			w.NKeyAccesses = make([]int64, *nbidders)
		}
	}

	rubis := &apps.Rubis{}
	rubis.Init(nproducts, *nbidders, *nworkers, *clientGoRoutines)
	rubis.Populate(s, coord.Workers[0].E)

	dlog.Printf("Done initializing rubis\n")

	p := prof.StartProfile()
	start := time.Now()
	gave_up := make([]int64, *clientGoRoutines)
	var wg sync.WaitGroup
	for i := 0; i < *clientGoRoutines; i++ {
		exp := ddtxn.MakeExp(30)
		wg.Add(1)
		go func(n int) {
			retries := make(ddtxn.RetryHeap, 0)
			heap.Init(&retries)
			end_time := time.Now().Add(time.Duration(*nsec) * time.Second)
			var local_seed uint32 = uint32(rand.Intn(1000000))
			wi := n % (*nworkers)
			w := coord.Workers[wi]
			for {
				tm := time.Now()
				if !end_time.After(tm) {
					break
				}
				var t ddtxn.Query
				if len(retries) > 0 && retries[0].TS.Before(tm) {
					t = heap.Pop(&retries).(ddtxn.Query)
				} else {
					rubis.MakeOne(w.ID, &local_seed, &t)
				}
				var txn_start time.Time
				if *apps.Latency || *doValidate {
					t.W = make(chan struct {
						R *ddtxn.Result
						E error
					})
					txn_start = time.Now()
				}
				committed := false
				_, err := w.One(t)
				if err == ddtxn.ESTASH {
					if *apps.Latency || *doValidate {
						x := <-t.W
						err = x.E
					}
					committed = true
				} else if err == ddtxn.EABORT {
					committed = false
				} else {
					committed = true
				}
				t.I++
				if !committed {
					t.TS = tm.Add(time.Duration(ddtxn.RandN(&local_seed, exp.Exp(t.I))) * time.Microsecond)
					if t.TS.Before(end_time) {
						heap.Push(&retries, t)
					} else {
						gave_up[n]++
					}
				}

				if committed && *apps.Latency {
					rubis.Time(&t, time.Since(txn_start), n)
				}
				if committed && *doValidate {
					rubis.Add(t)
				}
			}
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
	nitr, nwait, nwait2 := ddtxn.CollectCounts(coord, stats)
	_ = nwait2

	if *doValidate {
		rubis.Validate(s, int(nitr))
	}

	for i := 1; i < *clientGoRoutines; i++ {
		gave_up[0] = gave_up[0] + gave_up[i]
	}

	out := fmt.Sprintf("  nworkers: %v, nwmoved: %v, nrmoved: %v, sys: %v, total/sec: %v, abortrate: %.2f, stashrate: %.2f, nbidders: %v, nitems: %v, contention: %v, done: %v, actual time: %v, epoch changes: %v, throughput: ns/txn: %v, naborts: %v, coord time: %v, coord stats time: %v, total worker time transitioning: %v, nstashed: %v, rlock: %v, wrratio: %v, nsamples: %v, getkeys: %v, ddwrites: %v, nolock: %v, failv: %v, stashdone: %v, nfast: %v, gaveup: %v, potential: %v  ", *nworkers, ddtxn.WMoved, ddtxn.RMoved, *ddtxn.SysType, float64(nitr)/end.Seconds(), 100*float64(stats[ddtxn.NABORTS])/float64(nitr+stats[ddtxn.NABORTS]), 100*float64(stats[ddtxn.NSTASHED])/float64(nitr+stats[ddtxn.NABORTS]), *nbidders, nproducts, *contention, nitr, end, ddtxn.NextEpoch, end.Nanoseconds()/nitr, stats[ddtxn.NABORTS], ddtxn.Time_in_IE, ddtxn.Time_in_IE1, nwait, stats[ddtxn.NSTASHED], *ddtxn.UseRLocks, *ddtxn.WRRatio, stats[ddtxn.NSAMPLES], stats[ddtxn.NGETKEYCALLS], stats[ddtxn.NDDWRITES], stats[ddtxn.NO_LOCK], stats[ddtxn.NFAIL_VERIFY], stats[ddtxn.NDIDSTASHED], ddtxn.Nfast, gave_up[0], coord.PotentialPhaseChanges)
	fmt.Printf(out)
	fmt.Printf("\n")
	f, err := os.OpenFile(*dataFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	ddtxn.PrintStats(out, stats, f, coord, s, *nbidders)

	x, y := rubis.LatencyString()
	f.WriteString(x)
	f.WriteString(y)
	f.WriteString("\n")
}
