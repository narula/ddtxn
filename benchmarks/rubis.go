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

var contention = flag.Int("contention", 30, "Amount of contention, higher is more")
var nbidders = flag.Int("nb", 1000000, "Bidders in store, default is 1M")

var latency = flag.Bool("latency", false, "Measure latency")
var readrate = flag.Int("rr", 0, "Read rate %.  Rest are bids")
var skewed = flag.Bool("skewed", false, "Skewed workload")
var dataFile = flag.String("out", "rubis-data.out", "Filename for output")

var nitr int64
var nviews int64
var nbids int64
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

	if *doValidate {
		if !*ddtxn.Allocate {
			log.Fatalf("Cannot correctly validate without waiting for results; add -allocate\n")
		}
	}

	nproducts := *nbidders / *contention
	s := ddtxn.NewStore()
	coord := ddtxn.NewCoordinator(*nworkers, s)
	rubis := apps.InitRubis(s, nproducts, *nbidders, *nworkers, *readrate, *clientGoRoutines, coord.Workers[0].E)

	if *latency {
		rubis.SetupLatency(100, 1000000, *clientGoRoutines)
	}

	dlog.Printf("Done initializing buy\n")

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
			var t ddtxn.Query
			for duration.After(time.Now()) {
				rubis.MakeOne(w.ID, &local_seed, &t)
				if *latency || *doValidate {
					t.W = make(chan *ddtxn.Result)
					txn_start := time.Now()
					_, err := w.One(t)
					if err == ddtxn.ESTASH {
						<-t.W
					}
					txn_end := time.Since(txn_start)
					if *latency {
						rubis.Time(&t, txn_end, n)
					}
					if *doValidate {
						if err != ddtxn.EABORT {
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
	for i := 0; i < *nworkers; i++ {
		for j := 0; j < ddtxn.LAST_STAT; j++ {
			stats[j] = stats[j] + coord.Workers[i].Nstats[j]
		}
	}
	for i := ddtxn.RUBIS_REGISTER; i < ddtxn.LAST_TXN; i++ {
		nitr = nitr + stats[i]
	}
	if *doValidate {
		rubis.Validate(s, int(nitr))
	}

	out := fmt.Sprintf(" sys: %v, nworkers: %v, nbidders: %v, nitems: %v, contention: %v, done: %v, actual time: %v, nviews: %v, nbids: %v, epoch changes: %v, total/sec: %v, throughput: ns/txn: %v, naborts: %v, nwmoved: %v, nrmoved: %v", *ddtxn.SysType, *nworkers, *nbidders, nproducts, *contention, nitr, end, "xx", stats[ddtxn.RUBIS_BID], ddtxn.NextEpoch, float64(nitr)/end.Seconds(), end.Nanoseconds()/nitr, stats[ddtxn.NABORTS], ddtxn.WMoved, ddtxn.RMoved)
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

	for i := 0; i < len(stats); i++ {
		if stats[i] != 0 {
			f.WriteString(fmt.Sprintf("txn%v: %v\n", i, stats[i]))
		}
	}
	if *latency {
		x, y := rubis.LatencyString(*clientGoRoutines)
		f.WriteString(x)
		f.WriteString(y)
	}
}
