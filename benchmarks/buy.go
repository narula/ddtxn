package main

import (
	"ddtxn"
	"ddtxn/apps"
	"ddtxn/dlog"
	"ddtxn/prof"
	"ddtxn/stats"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"strings"
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
var notcontended_readrate = flag.Int("ncrr", 25, "Uncontended read rate %.  Default to half reads uncontended\n")

var latency = flag.Bool("latency", false, "Measure latency")
var dataFile = flag.String("out", "buy-data.out", "Filename for output")

var nitr int64
var nreads int64
var nbuys int64
var naborts int64
var ncopies int64
var nwait time.Duration
var nwait2 time.Duration

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
	portion_sz := *nbidders / *nworkers
	coord := ddtxn.NewCoordinator(*nworkers, s)

	var wg sync.WaitGroup

	lhr := make([]*stats.LatencyHist, *clientGoRoutines)
	lhw := make([]*stats.LatencyHist, *clientGoRoutines)

	var nincr int64 = 100
	var nbuckets int64 = 1000000

	buy_app := apps.InitBuy(s, nproducts, *nbidders, portion_sz, *nworkers, *readrate, *notcontended_readrate)
	dlog.Printf("Done initializing buy\n")

	p := prof.StartProfile()
	start := time.Now()

	for i := 0; i < *clientGoRoutines; i++ {
		if *latency {
			lhr[i] = stats.MakeLatencyHistogram(nincr, nbuckets)
			lhw[i] = stats.MakeLatencyHistogram(nincr, nbuckets)
		}
		wg.Add(1)
		go func(n int) {
			done := time.NewTimer(time.Duration(*nsec) * time.Second).C
			var local_seed uint32 = uint32(rand.Intn(10000000))
			wi := n % (*nworkers)
			w := coord.Workers[wi]
			// It's ok to reuse t because it gets copied in
			// w.One(), and if we're actually reading from t later
			// we pause and don't re-write it until it's done.
			var t ddtxn.Query
			for {
				select {
				case <-done:
					wg.Done()
					return
				default:
					buy_app.MakeOne(w.ID, &local_seed, &t)
					if *latency || *doValidate {
						t.W = make(chan *ddtxn.Result)
						txn_start := time.Now()
						_, err := w.One(t)
						if err == ddtxn.ESTASH {
							<-t.W
						}
						txn_end := time.Since(txn_start)
						if *latency {
							if t.TXN == ddtxn.D_READ_BUY {
								lhr[n].AddOne(txn_end.Nanoseconds())
							} else {
								lhw[n].AddOne(txn_end.Nanoseconds())
							}
						}
						if *doValidate {
							if err != ddtxn.EABORT {
								buy_app.Add(t)
							}
						}
					} else {
						w.One(t)
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
		nreads = nreads + coord.Workers[i].Nstats[ddtxn.D_READ_BUY]
		nbuys = nbuys + coord.Workers[i].Nstats[ddtxn.D_BUY]
		naborts = naborts + coord.Workers[i].Naborts
		nwait = nwait + coord.Workers[i].Nwait
		nwait2 = nwait2 + coord.Workers[i].Nwait2
	}
	nitr = nreads + nbuys
	if *doValidate {
		buy_app.Validate(s, int(nitr))
	}

	out := fmt.Sprintf(" sys: %v, nworkers: %v, nbids: %v, nproducts: %v, contention: %v, done: %v, actual time: %v, nreads: %v, nbuys: %v, epoch changes: %v, total/sec %v, throughput ns/txn: %v, naborts: %v, ncopies: %v, nwmoved: %v, nrmoved: %v, ietime: %v, etime: %v, etime2: %v", *ddtxn.SysType, *nworkers, *nbidders, nproducts, *contention, nitr, end, nreads, nbuys, ddtxn.NextEpoch, float64(nitr)/end.Seconds(), end.Nanoseconds()/nitr, naborts, ncopies, ddtxn.WMoved, ddtxn.RMoved, ddtxn.Time_in_IE.Seconds(), nwait.Seconds()/float64(*nworkers), nwait2.Seconds()/float64(*nworkers))
	fmt.Printf(out)
	fmt.Printf("\n")

	st := strings.Split(out, ",")
	f, err := os.OpenFile(*dataFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	f.WriteString("# ")
	o2, err := exec.Command("git", "describe", "--always", "HEAD").Output()
	f.WriteString(string(o2))
	f.WriteString("# ")
	f.WriteString(strings.Join(os.Args, " "))
	f.WriteString("\n")
	for i := range st {
		if _, err = f.WriteString(st[i]); err != nil {
			panic(err)
		}
		f.WriteString("\n")
	}

	f.WriteString(fmt.Sprintf("txn%v: %v\n", ddtxn.D_BUY, nbuys))
	f.WriteString(fmt.Sprintf("txn%v: %v\n", ddtxn.D_READ_BUY, nreads))

	if *latency {
		for i := 1; i < *clientGoRoutines; i++ {
			lhr[0].Combine(lhr[i])
			lhw[0].Combine(lhw[i])
		}
		f.WriteString(fmt.Sprint("Read 25: %v\nRead 50: %v\nRead 75: %v\nRead 99: %v\n", lhr[0].GetPercentile(25), lhr[0].GetPercentile(50), lhr[0].GetPercentile(75), lhr[0].GetPercentile(99)))
		f.WriteString(fmt.Sprint("Write 25: %v\nWrite 50: %v\nWrite 75: %v\nWrite 99: %v\n", lhw[0].GetPercentile(25), lhw[0].GetPercentile(50), lhw[0].GetPercentile(75), lhw[0].GetPercentile(99)))
	}

	mean, stddev := ddtxn.StddevChunks(s.NChunksAccessed)
	f.WriteString(fmt.Sprintf("mean: %v\nstddev: %v\n", mean, stddev))
	for i := 0; i < len(s.NChunksAccessed); i++ {
		x := float64(mean) - float64(s.NChunksAccessed[i])
		if x < 0 {
			x = x * -1
		}
		if x > stddev {
			f.WriteString(fmt.Sprintf("Chunk %v: %v\n", i, s.NChunksAccessed[i]))
		}
	}
	f.WriteString("\n")
	//ddtxn.PrintLockCounts(s, *nbidders, nproducts, true)
}
