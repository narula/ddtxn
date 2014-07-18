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

var contention = flag.Int("contention", 1000, "Amount of contention, higher is more")
var nbidders = flag.Int("nb", 1000000, "Bidders in store, default is 1M")
var readrate = flag.Int("rr", 0, "Read rate %.  Rest are buys")
var notcontended_readrate = flag.Float64("ncrr", .8, "Uncontended read rate %.  Default to .8")

var dataFile = flag.String("out", "buy-data.out", "Filename for output")

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

	buy_app := &apps.Buy{}
	buy_app.Init(nproducts, *nbidders, *nworkers, *readrate, *clientGoRoutines, *notcontended_readrate)
	buy_app.Populate(s, coord.Workers[0].E)

	dlog.Printf("Done initializing buy\n")

	p := prof.StartProfile()
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < *clientGoRoutines; i++ {
		wg.Add(1)
		go func(n int) {
			duration := time.Now().Add(time.Duration(*nsec) * time.Second)
			var local_seed uint32 = uint32(rand.Intn(10000000))
			wi := n % (*nworkers)
			w := coord.Workers[wi]
			// It's ok to reuse t because it gets copied in
			// w.One(), and if we're actually reading from t later
			// we pause and don't re-write it until it's done.
			var t ddtxn.Query
			for duration.After(time.Now()) {
				buy_app.MakeOne(w.ID, &local_seed, &t)
				if *apps.Latency || *doValidate {
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
					if *apps.Latency {
						buy_app.Time(&t, txn_end, n)
					}
					if *doValidate {
						if err == nil {
							buy_app.Add(t)
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

	if *doValidate {
		buy_app.Validate(s, int(nitr))
	}

	// nitr + NABORTS + ENOKEY is how many requests were issued.  A
	// stashed transaction eventually executes and contributes to
	// nitr.
	out := fmt.Sprintf(" sys: %v, nworkers: %v, rr: %v, ncrr: %v, nbids: %v, nproducts: %v, contention: %v, done: %v, actual time: %v, nreads: %v, nbuys: %v, epoch changes: %v, total/sec: %v, throughput ns/txn: %v, naborts: %v, nwmoved: %v, nrmoved: %v, ietime: %v, ietime1: %v, etime: %v, etime2: %v, nstashed: %v, rlock: %v, wrratio: %v, nsamples: %v, abortrate: %v, stashrate: %v ", *ddtxn.SysType, *nworkers, *readrate, *notcontended_readrate*float64(*readrate), *nbidders, nproducts, *contention, nitr, end, stats[ddtxn.D_READ_ONE], stats[ddtxn.D_BUY], ddtxn.NextEpoch, float64(nitr)/end.Seconds(), end.Nanoseconds()/nitr, stats[ddtxn.NABORTS], ddtxn.WMoved, ddtxn.RMoved, ddtxn.Time_in_IE.Seconds(), ddtxn.Time_in_IE1.Seconds(), nwait.Seconds()/float64(*nworkers), nwait2.Seconds()/float64(*nworkers), stats[ddtxn.NSTASHED], *ddtxn.UseRLocks, *ddtxn.WRRatio, stats[ddtxn.NSAMPLES], stats[ddtxn.NABORTS]/(nitr+stats[ddtxn.NABORTS]), stats[ddtxn.NSTASHED]/(nitr+stats[ddtxn.NABORTS]))
	fmt.Printf(out)
	fmt.Printf("\n")
	f, err := os.OpenFile(*dataFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	ddtxn.PrintStats(out, stats, f, coord, s, *nbidders)

	x, y := buy_app.LatencyString()
	f.WriteString(x)
	f.WriteString(y)
	f.WriteString("\n")
}
