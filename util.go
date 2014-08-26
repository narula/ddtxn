package ddtxn

import (
	crand "crypto/rand"
	"ddtxn/dlog"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"strings"
	"time"
)

type Exp2 struct {
	values []uint32
}

func MakeExp(n int) *Exp2 {
	e := &Exp2{
		values: make([]uint32, n),
	}
	for i := 0; i < n; i++ {
		e.values[i] = uint32(math.Exp2(float64(i)))
	}
	return e
}

func (e *Exp2) Exp(n int) uint32 {
	if n >= len(e.values) {
		log.Fatalf("Too big")
	}
	return e.values[n]
}

func RandN(seed *uint32, n uint32) uint32 {
	*seed = *seed*1103515245 + 12345
	return ((*seed & 0x7fffffff) % (n * 8) / 8)
}

func Randstr(sz int) string {
	alphanum := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, sz)
	crand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bytes)
}

func Validate(c *Coordinator, s *Store, nkeys int, nproducts int, val []int32, n int) bool {
	good := true
	dlog.Printf("Validate start, store at %x\n", c.GetEpoch())
	zero_cnt := 0
	for j := 0; j < nproducts; j++ {
		var x int32
		k := ProductKey(j)
		v, err := s.getKey(k)
		if err != nil {
			if val[j] != 0 {
				fmt.Printf("Validating key %v failed; store: none should have: %v\n", k, val[j])
				dlog.Printf("Validating key %v failed; store: none should have: %v\n", k, val[j])
				good = false
			}
			continue
		}
		x = v.Value().(int32)
		if x != val[j] {
			dlog.Printf("Validating key %v failed; store: %v should have: %v\n", k, x, val[j])
			good = false
		}
		if x == 0 {
			//dlog.Printf("Saying x is zero %v %v\n", x, zero_cnt)
			zero_cnt++
		}
	}
	if zero_cnt == nproducts && n > 10 {
		fmt.Printf("Bad: all zeroes!\n")
		dlog.Printf("Bad: all zeroes!\n")
		good = false
	}
	dlog.Printf("Done validating\n")
	if !good {
		fmt.Printf("Validating failed\n")
	}
	return good
}

func PrintLockCounts(s *Store) {
	if *Conflicts == false {
		fmt.Println("Didn't measure conflicts!")
	}
	for i, chunk := range s.store {
		for k, v := range chunk.rows {
			if v.conflict > 0 {
				x, y := UndoCKey(k)
				fmt.Printf("CONFLICT! chunk[%v] %v %v\t:%v\n", i, x, y, v.conflict)
			}
		}
	}
}

func StddevChunks(nc []int64) (int64, float64) {
	var total int64
	var n int64
	var i int64
	n = int64(len(nc))
	for i = 0; i < n; i++ {
		total += nc[i]
	}
	mean := total / n
	variances := make([]int64, n)

	for i = 0; i < n; i++ {
		x := nc[i] - mean
		if x < 0 {
			x = x * -1
		}
		x = x * x
		variances[i] = x
	}

	var stddev int64
	for i = 0; i < n; i++ {
		stddev += variances[i]
	}
	return mean, math.Sqrt(float64(stddev / n))
}

func StddevKeys(nc []int64) (int64, float64) {
	var total int64
	var n int64
	var i int64
	n = int64(len(nc))
	var cnt int64 = 1
	for i = 0; i < n; i++ {
		if nc[i] != 0 {
			total += nc[i]
			cnt++
		}
	}
	mean := total / cnt
	variances := make([]int64, cnt)

	cnt = 1
	for i = 0; i < n; i++ {
		if nc[i] != 0 {
			x := nc[i] - mean
			if x < 0 {
				x = x * -1
			}
			x = x * x
			variances[cnt] = x
			cnt++
		}
	}

	var stddev int64
	for i = 0; i < cnt; i++ {
		stddev += variances[i]
	}
	return mean, math.Sqrt(float64(stddev / cnt))
}

func WriteChunkStats(s *Store, f *os.File) {
	mean, stddev := StddevChunks(s.NChunksAccessed)
	f.WriteString(fmt.Sprintf("chunk-mean: %v\nchunk-stddev: %v\n", mean, stddev))
	for i := 0; i < len(s.NChunksAccessed); i++ {
		x := float64(mean) - float64(s.NChunksAccessed[i])
		if x < 0 {
			x = x * -1
		}
		if x > stddev {
			f.WriteString(fmt.Sprintf("Chunk %v: %v\n", i, s.NChunksAccessed[i]))
		}
	}
}

func PrintStats(out string, stats []int64, f *os.File, coord *Coordinator, s *Store, nb int) {
	st := strings.Split(out, ",")
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
	for i := 0; i < LAST_TXN; i++ {
		if stats[i] != 0 {
			f.WriteString(fmt.Sprintf("txn%v: %v\n", i, stats[i]))
		}
	}
	WriteChunkStats(s, f)
	if *CountKeys {
		WriteCountKeyStats(coord, nb, f)
	}
	if *Conflicts {
		PrintLockCounts(s)
	}
}

func WriteCountKeyStats(coord *Coordinator, nb int, f *os.File) {
	bk := make([]int64, nb)
	ok := make([]int64, 4)
	for j := 0; j < len(coord.Workers); j++ {
		w := coord.Workers[j]
		for i := 0; i < nb; i++ {
			bk[i] = bk[i] + w.NKeyAccesses[i]
		}
	}
	mean, stddev := StddevKeys(bk)
	f.WriteString(fmt.Sprintf("b-kmean: %v\nb-kstddev: %v\n", mean, stddev))
	for i := 0; i < nb; i++ {
		x := float64(mean) - float64(bk[i])
		if x < 0 {
			x = x * -1
		}
		if x < stddev {
			ok[0]++
		} else if x < 2*stddev {
			ok[1]++
		} else if x < 3*stddev {
			ok[2]++
		} else {
			ok[3]++
		}
		if x > 2*stddev && bk[i] != 0 {
			f.WriteString(fmt.Sprintf("BKey %v: %v\n", i, bk[i]))
		}
		if x > stddev && bk[i] != 0 && x < 2*stddev {
			f.WriteString(fmt.Sprintf("BKey %v: %v\n", i, bk[i]))
		}
	}
	f.WriteString(fmt.Sprintf("b-stddev counts: %v\n", ok))
}

func CollectCounts(coord *Coordinator, stats []int64) (int64, time.Duration, time.Duration) {
	var nitr int64
	var nwait time.Duration
	var nwait2 time.Duration
	for i := 0; i < coord.n; i++ {
		for j := 0; j < LAST_STAT; j++ {
			stats[j] = stats[j] + coord.Workers[i].Nstats[j]
			if j < LAST_TXN {
				nitr = nitr + coord.Workers[i].Nstats[j]
			}
		}
		nwait = nwait + coord.Workers[i].Nwait
		nwait2 = nwait2 + coord.Workers[i].Nwait2
	}
	return nitr, nwait, nwait2
}
