package main

import (
	"ddtxn"
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

var contention = flag.Int("contention", 30, "Amount of contention, higher is more")
var nbidders = flag.Int("nb", 1000000, "Bidders in store, default is 1M")

var latency = flag.Bool("latency", false, "Measure latency")

var skewed = flag.Bool("skewed", false, "Skewed workload")
var dataFile = flag.String("out", "data.out", "Filename for output")

var nitr int64
var nviews int64
var nbids int64
var naborts int64
var ncopies int64
var nfound int64
var nentered int64

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*nprocs)

	if *clientGoRoutines == 0 {
		*clientGoRoutines = *nprocs
	}
	if *nworkers == 0 {
		*nworkers = *nprocs
	}

	nitems := *nbidders / *contention
	s := ddtxn.NewStore()
	portion2 := *nbidders / *nworkers

	coord := ddtxn.NewCoordinator(*nworkers, s)
	users, items := s.LoadRubis(coord, *nbidders, nitems)

	p := prof.StartProfile()
	start := time.Now()
	var wg sync.WaitGroup

	lh := make([]*stats.LatencyHist, *clientGoRoutines)

	var nincr int64 = 100
	var nbuckets int64 = 1000000
	rates := ddtxn.GetTxns(*skewed)

	for i := 0; i < *clientGoRoutines; i++ {
		if *latency {
			lh[i] = stats.MakeLatencyHistogram(nincr, nbuckets)
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
					var product int = int(ddtxn.RandN(&local_seed, uint32(nitems)))
					if *skewed {
						product = 5
					}

					var bidder int
					lb2 := int(ddtxn.RandN(&local_seed, uint32(portion2)))
					bidder = lb2 + wi*portion2
					amt := int32(ddtxn.RandN(&local_seed, 10))

					x := float64(ddtxn.RandN(&local_seed, uint32(100)))
					var tx ddtxn.Transaction

					if x < rates[0] {
						tx = ddtxn.Transaction{
							TXN: ddtxn.RUBIS_BID,
							U1:  users[bidder],
							A:   amt,
							U2:  items[product],
						}
						switch *ddtxn.SysType {
						case ddtxn.OCC:
							tx.TXN = ddtxn.RUBIS_BID_OCC
						}
					} else if x < rates[1] {
						tx = ddtxn.Transaction{
							TXN: ddtxn.RUBIS_BIDHIST,
							U1:  items[product],
						}
					} else if x < rates[2] {
						tx = ddtxn.Transaction{
							TXN: ddtxn.RUBIS_BUYNOW,
							U1:  users[bidder],
							U2:  items[product],
							U3:  uint64(amt),
						}
					} else if x < rates[3] {
						tu := int(ddtxn.RandN(&local_seed, uint32(*nbidders)))
						fu := int(ddtxn.RandN(&local_seed, uint32(*nbidders)))
						com := "xxxxxxx" //ddtxn.Randstr(10)
						var rating uint64 = 1
						tx = ddtxn.Transaction{
							TXN: ddtxn.RUBIS_COMMENT,
							U1:  users[tu],
							U2:  users[fu],
							U3:  items[product],
							S1:  com,
							U4:  rating,
						}
						switch *ddtxn.SysType {
						case ddtxn.OCC:
							tx.TXN = ddtxn.RUBIS_COMMENT_OCC
						}
					} else if x < rates[4] {
						tx = ddtxn.Transaction{
							TXN: ddtxn.RUBIS_NEWITEM,
							U1:  users[bidder],
							S1:  "yyyy", //ddtxn.Randstr(7),
							S2:  "zzzz", //ddtxn.Randstr(15),
							U2:  uint64(amt),
							U3:  uint64(amt),
							U4:  uint64(amt),
							U5:  1,
							U6:  1,
							I:   1,
							U7:  uint64(ddtxn.RandN(&local_seed, uint32(ddtxn.NUM_CATEGORIES))),
						}
						switch *ddtxn.SysType {
						case ddtxn.OCC:
							tx.TXN = ddtxn.RUBIS_NEWITEM_OCC
						}
					} else if x < rates[5] {
						tx = ddtxn.Transaction{
							TXN: ddtxn.RUBIS_PUTBID,
							U1:  items[product],
						}
						switch *ddtxn.SysType {
						case ddtxn.OCC:
							tx.TXN = ddtxn.RUBIS_PUTBID_OCC
						}
					} else if x < rates[6] {
						tu := int(ddtxn.RandN(&local_seed, uint32(*nbidders)))
						tx = ddtxn.Transaction{
							TXN: ddtxn.RUBIS_PUTCOMMENT,
							U1:  users[tu],
							U2:  items[product],
						}
					} else if x < rates[7] {
						tx = ddtxn.Transaction{
							TXN: ddtxn.RUBIS_REGISTER,
							S1:  "aaaaaaa", //ddtxn.Randstr(7),
							U1:  uint64(ddtxn.RandN(&local_seed, uint32(ddtxn.NUM_REGIONS))),
						}
					} else if x < rates[8] {
						tx = ddtxn.Transaction{
							TXN: ddtxn.RUBIS_SEARCHCAT,
							U1:  uint64(ddtxn.RandN(&local_seed, uint32(ddtxn.NUM_CATEGORIES))),
							U2:  5,
						}
					} else if x < rates[9] {
						tx = ddtxn.Transaction{
							TXN: ddtxn.RUBIS_SEARCHREG,
							U1:  uint64(ddtxn.RandN(&local_seed, uint32(ddtxn.NUM_REGIONS))),
							U2:  uint64(ddtxn.RandN(&local_seed, uint32(ddtxn.NUM_CATEGORIES))),
							U3:  5,
						}
					} else if x < rates[10] {
						tx = ddtxn.Transaction{
							TXN: ddtxn.RUBIS_VIEW,
							U1:  items[product],
						}
					} else if x < rates[11] {
						tx = ddtxn.Transaction{
							TXN: ddtxn.RUBIS_VIEWUSER,
							U1:  users[bidder],
						}
					} else {
						log.Fatalf("No such transaction %v\n", x)
					}
					if *latency {
						tx.W = make(chan *ddtxn.Result)
						txn_start := time.Now()
						w.Incoming <- tx
						<-tx.W
						txn_end := time.Since(txn_start)
						lh[n].AddOne(txn_end.Nanoseconds())
					} else {
						w.Incoming <- tx
					}
				}
			}
		}(i)
	}
	wg.Wait()
	coord.Finish()
	end := time.Since(start)
	p.Stop()
	stats := make([]int64, ddtxn.LAST_TXN)
	for i := 0; i < *nworkers; i++ {
		for j := ddtxn.RUBIS_BID; j < ddtxn.LAST_TXN; j++ {
			stats[j] = stats[j] + coord.Workers[i].Nstats[j]
		}
		nbids = nbids + coord.Workers[i].Nstats[ddtxn.RUBIS_BID]
		naborts = naborts + coord.Workers[i].Naborts
		ncopies = ncopies + coord.Workers[i].Ncopy
	}
	for i := 0; i < len(stats); i++ {
		nitr = nitr + stats[i]
	}
	if *doValidate {
		ddtxn.ValidateRubis(s, users, items)
	}

	out := fmt.Sprintf(" sys: %v, nworkers: %v, nbidders: %v, nitems: %v, contention: %v, done: %v, actual time: %v, nviews: %v, nbids: %v, epoch changes: %v, total/sec: %v, throughput ns/txn: %v, naborts: %v, ncopies: %v, nmoved: %v, nfound: %v, nentered: %v", *ddtxn.SysType, *nworkers, *nbidders, nitems, *contention, nitr, end, "xx", nbids, ddtxn.NextEpoch, float64(nitr)/end.Seconds(), end.Nanoseconds()/nitr, naborts, ncopies, ddtxn.Moved, nfound, nentered)
	fmt.Printf(out)

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
		for i := 1; i < *clientGoRoutines; i++ {
			lh[0].Combine(lh[i])
		}
		f.WriteString(fmt.Sprint("25: %v\n50: %v\n75: %v\n99: %v\n", lh[0].GetPercentile(25), lh[0].GetPercentile(50), lh[0].GetPercentile(75), lh[0].GetPercentile(99)))
	}
	f.WriteString("\n")
}
