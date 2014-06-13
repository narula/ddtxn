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
var nwait time.Duration
var nwait2 time.Duration
var nstashed int64

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

	buy_app := apps.InitBuy(s, nproducts, *nbidders, *nworkers, *readrate, *notcontended_readrate, *clientGoRoutines)
	if *latency {
		buy_app.SetupLatency(100, 1000000, *clientGoRoutines)
	}

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
				if *latency || *doValidate {
					t.W = make(chan *ddtxn.Result)
					txn_start := time.Now()
					_, err := w.One(t)
					if err == ddtxn.ESTASH {
						<-t.W
					}
					txn_end := time.Since(txn_start)
					if *latency {
						buy_app.Time(&t, txn_end, n)
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
			wg.Done()
		}(i)
	}
	wg.Wait()
	coord.Finish()
	end := time.Since(start)
	p.Stop()

	for i := 0; i < *nworkers; i++ {
		nreads = nreads + coord.Workers[i].Nstats[ddtxn.D_READ_ONE]
		nbuys = nbuys + coord.Workers[i].Nstats[ddtxn.D_BUY]
		naborts = naborts + coord.Workers[i].Naborts
		nwait = nwait + coord.Workers[i].Nwait
		nwait2 = nwait2 + coord.Workers[i].Nwait2
		nstashed = nstashed + coord.Workers[i].Nstats[ddtxn.LAST_TXN]
	}
	nitr = nreads + nbuys
	if *doValidate {
		buy_app.Validate(s, int(nitr))
	}

	out := fmt.Sprintf(" sys: %v, nworkers: %v, nbids: %v, nproducts: %v, contention: %v, done: %v, actual time: %v, nreads: %v, nbuys: %v, epoch changes: %v, total/sec: %v, throughput ns/txn: %v, naborts: %v, nwmoved: %v, nrmoved: %v, ietime: %v, ietime1: %v, etime: %v, etime2: %v, nstashed: %v, rlock: %v, wrratio: %v", *ddtxn.SysType, *nworkers, *nbidders, nproducts, *contention, nitr, end, nreads, nbuys, ddtxn.NextEpoch, float64(nitr)/end.Seconds(), end.Nanoseconds()/nitr, naborts, ddtxn.WMoved, ddtxn.RMoved, ddtxn.Time_in_IE.Seconds(), ddtxn.Time_in_IE1.Seconds(), nwait.Seconds()/float64(*nworkers), nwait2.Seconds()/float64(*nworkers), nstashed, *ddtxn.UseRLocks, *ddtxn.WRRatio)
	fmt.Printf(out)

	if *ddtxn.Conflicts {
		ddtxn.PrintLockCounts(s)
	}
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
	f.WriteString(fmt.Sprintf("txn%v: %v\n", ddtxn.D_READ_ONE, nreads))

	if *latency {
		x, y := buy_app.LatencyString(*clientGoRoutines)
		f.WriteString(x)
		f.WriteString(y)
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
}
