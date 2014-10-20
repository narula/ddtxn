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
	"strconv"
	"sync"
	"time"
)

var nprocs = flag.Int("nprocs", 2, "GOMAXPROCS default 2")
var nsec = flag.Int("nsec", 2, "Time to run in seconds")
var clientGoRoutines = flag.Int("ngo", 0, "Number of goroutines/workers generating client requests.")
var nworkers = flag.Int("nw", 0, "Number of workers")
var doValidate = flag.Bool("validate", false, "Validate")

var contention = flag.Int("contention", 3, "Amount of contention, higher is more")
var nbidders = flag.Int("nb", 1000000, "Bidders in store, default is 1M")
var readrate = flag.Int("rr", 0, "NOT USED")
var notcontended_readrate = flag.Float64("ncrr", 40.0, "BID RATE")
var dataFile = flag.String("out", "xdata.out", "Filename for output")
var atomicIncr = flag.Bool("atomic", false, "NOT USED")
var rounds = flag.Bool("rounds", true, "Preallocate keys in rounds instead of entirely in parallel")
var ZipfDist = flag.Float64("zipf", 1, "Zipfian distribution theta. -1 means use -contention instead")

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

	bidrate := *notcontended_readrate

	var nproducts int
	if *contention > 0 {
		nproducts = *nbidders / int(*contention)
	} else {
		nproducts = ddtxn.NUM_ITEMS
	}
	s := ddtxn.NewStore()
	coord := ddtxn.NewCoordinator(*nworkers, s)

	if *ddtxn.CountKeys {
		for i := 0; i < *nworkers; i++ {
			w := coord.Workers[i]
			w.NKeyAccesses = make([]int64, *nbidders)
		}
	}

	rubis := &apps.Rubis{}
	rubis.Init(nproducts, *nbidders, *nworkers, *clientGoRoutines, *ZipfDist, bidrate)
	rubis.Populate(s, coord)
	fmt.Printf("Done populating rubis\n")

	if !*ddtxn.Allocate {
		tmp := *ddtxn.UseRLocks
		*ddtxn.UseRLocks = true
		rubis.PreAllocate(coord, bidrate, *rounds)
		*ddtxn.UseRLocks = tmp
	}
	fmt.Printf("Done initializing rubis\n")

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
					if *ddtxn.Latency {
						t.S = time.Now()
					}
				}
				if *doValidate {
					t.W = make(chan struct {
						R *ddtxn.Result
						E error
					})
				}
				committed := false
				_, err := w.One(t)
				if err == ddtxn.ESTASH {
					if *doValidate {
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

	if !*ddtxn.Allocate {
		keys := []rune{'b', 'c', 'd', 'i', 'k', 'u'}
		for i := 0; i < *nworkers; i++ {
			dlog.Printf("w: %v ", i)
			for _, k := range keys {
				dlog.Printf("%v %v/%v \t", strconv.QuoteRuneToASCII(k), coord.Workers[i].CurrKey[k], coord.Workers[i].LastKey[k])
			}
			dlog.Printf("\n")
		}
	}

	out := fmt.Sprintf("  nworkers: %v, nwmoved: %v, nrmoved: %v, sys: %v, total/sec: %v, abortrate: %.2f, stashrate: %.2f, nbidders: %v, nitems: %v, contention: %v, done: %v, actual time: %v, throughput: ns/txn: %v, naborts: %v, coord stats time: %v, total worker time transitioning: %v, nstashed: %v, rlock: %v, wrratio: %v, nsamples: %v, getkeys: %v, ddwrites: %v, nolock: %v, failv: %v, stashdone: %v, nfast: %v, gaveup: %v,  epoch changes: %v, potential: %v, coordtotaltime %v, mergetime: %v, readtime: %v, gotime: %v  ", *nworkers, ddtxn.WMoved, ddtxn.RMoved, *ddtxn.SysType, float64(nitr)/end.Seconds(), 100*float64(stats[ddtxn.NABORTS])/float64(nitr+stats[ddtxn.NABORTS]), 100*float64(stats[ddtxn.NSTASHED])/float64(nitr+stats[ddtxn.NABORTS]), *nbidders, nproducts, *contention, nitr, end, end.Nanoseconds()/nitr, stats[ddtxn.NABORTS], ddtxn.Time_in_IE1, nwait, stats[ddtxn.NSTASHED], *ddtxn.UseRLocks, *ddtxn.WRRatio, stats[ddtxn.NSAMPLES], stats[ddtxn.NGETKEYCALLS], stats[ddtxn.NDDWRITES], stats[ddtxn.NO_LOCK], stats[ddtxn.NFAIL_VERIFY], stats[ddtxn.NDIDSTASHED], ddtxn.Nfast, gave_up[0], ddtxn.NextEpoch, coord.PotentialPhaseChanges, coord.TotalCoordTime, coord.MergeTime, coord.ReadTime, coord.GoTime)
	fmt.Printf(out)
	fmt.Printf("\n")
	f, err := os.OpenFile(*dataFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	fmt.Printf("DD: %v\n", coord.Workers[0].Store().DD())
	ddtxn.PrintStats(out, stats, f, coord, s, *nbidders)

	x, y := coord.Latency()
	f.WriteString(x)
	f.WriteString(y)
	f.WriteString("\n")
}
