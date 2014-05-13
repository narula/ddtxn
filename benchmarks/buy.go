package main

import (
	"ddtxn"
	"ddtxn/prof"
	"ddtxn/stats"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"strings"
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
var dataFile = flag.String("out", "buy-data.out", "Filename for output")

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
		k := ddtxn.ProductKey(i)
		dd[i] = k
	}
	data := make(map[ddtxn.Key]ddtxn.Value)
	for i := 0; i < *nbidders; i++ {
		k := ddtxn.UserKey(i)
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

					var txn ddtxn.Query
					if x < *readrate {
						txn = ddtxn.Query{
							TXN: ddtxn.D_READ_BUY,
							K1:  dd[product],
						}
						if *latency {
							txn.W = make(chan *ddtxn.Result)
						}
						if x >= *notcontended_readrate {
							// Contended read; already set K1
						} else {
							// Uncontended read
							txn.K1 = bidder_keys[bidder]
						}
					} else {
						txn = ddtxn.Query{
							TXN: ddtxn.D_BUY,
							K1:  bidder_keys[bidder],
							A:   amt,
							K2:  dd[product],
						}
						if *latency {
							txn.W = make(chan *ddtxn.Result)
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
		nreads = nreads + coord.Workers[i].Nstats[ddtxn.D_READ_BUY]
		nbuys = nbuys + coord.Workers[i].Nstats[ddtxn.D_BUY]
		naborts = naborts + coord.Workers[i].Naborts
	}
	nitr = nreads + nbuys
	if *doValidate {
		ddtxn.Validate(coord, s, *nbidders, nproducts, val, int(nitr))
	}

	out := fmt.Sprintf(" sys: %v, nworkers: %v, nbids: %v, nproducts: %v, contention: %v, done: %v, actual time: %v, nreads: %v, nbuys: %v, epoch changes: %v, total/sec %v, throughput ns/txn: %v, naborts %v, ncopies %v, nmoved %v", *ddtxn.SysType, *nworkers, *nbidders, nproducts, *contention, nitr, end, nreads, nbuys, ddtxn.NextEpoch, float64(nitr)/end.Seconds(), end.Nanoseconds()/nitr, naborts, ncopies, ddtxn.Moved)
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
