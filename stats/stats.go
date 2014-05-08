package stats

import (
	"log"
	"sync/atomic"
	"time"
)

func Timer(variable *Counter) (*Counter, time.Time) {
	return variable, time.Now()
}

func Do(variable *Counter, start time.Time) {
	endTime := time.Since(start)
	atomic.AddInt64(&variable.time, endTime.Nanoseconds())
	atomic.AddInt64(&variable.n, 1)
}

type LatencyHist struct {
	bins  []int
	incr  int64
	nb    int64
	total int64
}

func MakeLatencyHistogram(incr int64, buckets int64) *LatencyHist {
	l := &LatencyHist{
		bins: make([]int, buckets),
		incr: incr,
		nb:   buckets,
	}
	return l
}

func (lh *LatencyHist) Combine(lh2 *LatencyHist) {
	if lh.incr != lh2.incr || lh.nb != lh2.nb {
		log.Fatalf("incombinable\n")
	}
	var i int64
	for i = 0; i < lh.nb; i++ {
		lh.bins[i] += lh2.bins[i]
	}
	lh.total += lh2.total
}

func (lh *LatencyHist) AddOne(tm int64) {
	var b int64
	var i int64
	if tm > lh.incr*lh.nb {
		b = lh.nb - 1
	} else if tm == 0 {
		b = 0
	} else {
		for i = 0; i < lh.nb; i++ {
			if i*lh.incr > tm {
				b = i - 1
				break
			}
		}
	}
	lh.bins[int(b)]++
	lh.total++
}

func (lh *LatencyHist) GetPercentile(percent float64) int64 {
	reach := (percent / 100) * float64(lh.total)
	var total float64
	var i int64
	for i = 0; i < lh.nb; i++ {
		total += float64(lh.bins[i])
		if total >= reach {
			return i * lh.incr
		}
	}
	return 0
}

type Counter struct {
	time int64
	n    int64
}

func (c *Counter) One() int64 {
	if c.n == 0 {
		return 0
	}
	return c.time / c.n
}

func (c *Counter) QPS() int64 {
	return (int64(time.Second/time.Nanosecond) * c.n) / c.time
}
