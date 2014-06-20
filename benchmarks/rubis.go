package main

import (
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

var latency = flag.Bool("latency", false, "Measure latency")
var dataFile = flag.String("out", "rubis-data.out", "Filename for output")

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
	rubis.Init(s, nproducts, *nbidders, *nworkers, *readrate, *clientGoRoutines, *notcontended_readrate, coord.Workers[0].E)

	if *latency {
		rubis.SetupLatency(100, 1000000, *clientGoRoutines)
	}

	dlog.Printf("Done initializing rubis\n")

	p := prof.StartProfile()
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < *clientGoRoutines; i++ {
		wg.Add(1)
		go func(n int) {
			duration := time.Now().Add(time.Duration(*nsec) * time.Second)
			var local_seed uint32 = uint32(rand.Intn(1000000))
			wi := n % (*nworkers)
			w := coord.Workers[wi]
			// It's ok to reuse t because it gets copied in
			// w.One(), and if we're actually reading from t later
			// we pause and don't re-write it until it's done.
			var t ddtxn.Query
			for duration.After(time.Now()) {
				rubis.MakeOne(w.ID, &local_seed, &t)
				if *latency || *doValidate {
					t.W = make(chan struct {
						R *ddtxn.Result
						E error
					})
					txn_start := time.Now()
					_, err := w.One(t)
					if err == ddtxn.ESTASH {
						x := <-t.W
						err = x.E
					}
					txn_end := time.Since(txn_start)
					if *latency {
						rubis.Time(&t, txn_end, n)
					}
					if *doValidate {
						if err == nil {
							rubis.Add(t)
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
	nitr, nwait, nwait2 := ddtxn.CollectCounts(coord, stats)
	_ = nwait
	_ = nwait2

	if *doValidate {
		rubis.Validate(s, int(nitr))
	}

	out := fmt.Sprintf(" sys: %v, nworkers: %v, nbidders: %v, nitems: %v, contention: %v, done: %v, actual time: %v, epoch changes: %v, total/sec: %v, throughput: ns/txn: %v, naborts: %v, nwmoved: %v, nrmoved: %v", *ddtxn.SysType, *nworkers, *nbidders, nproducts, *contention, nitr, end, ddtxn.NextEpoch, float64(nitr)/end.Seconds(), end.Nanoseconds()/nitr, stats[ddtxn.NABORTS], ddtxn.WMoved, ddtxn.RMoved)
	fmt.Printf(out)
	fmt.Printf("\n")
	f, err := os.OpenFile(*dataFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	ddtxn.PrintStats(out, stats, f, coord, s, *nbidders)

	if *latency {
		x, y := rubis.LatencyString(*clientGoRoutines)
		f.WriteString(x)
		f.WriteString(y)
	}
	f.WriteString("\n")
}
