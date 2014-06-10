package ddtxn

import (
	"container/heap"
	"ddtxn/dlog"
)

type TStore struct {
	t []Query
}

func TSInit(n int) *TStore {
	ts := &TStore{t: make([]Query, 0, n)}
	return ts
}

func (ts *TStore) Add(t Query) {
	ts.t = append(ts.t, t)
}

func (ts *TStore) clear() {
	ts.t = ts.t[:0]
}

const (
	WRRATIO = 5
)

type OneStat struct {
	k      Key
	reads  float64
	writes float64
	index  int
}

func (o *OneStat) ratio() float64 {
	return float64(o.writes) / float64(o.reads)
}

type Candidates struct {
	m map[Key]*OneStat
	h *StatsHeap
}

func (c *Candidates) Merge(c2 *Candidates) {
	for i := 0; i < len(*c2.h); i++ {
		o2 := heap.Pop(c2.h).(*OneStat)
		o, ok := c.m[o2.k]
		if !ok {
			c.m[o2.k] = &OneStat{k: o2.k, reads: 0, writes: 0, index: -1}
			o = c.m[o2.k]
		}
		o.reads += o2.reads
		o.writes += o2.writes
		dlog.Printf("Added %v reads and %v writes to %v\n", o2.reads, o2.writes, o2.k)
		if o.ratio() > WRRATIO {
			c.h.update(o)
		}
	}
}

func (c *Candidates) Read(k Key) {
	o, ok := c.m[k]
	if !ok {
		c.m[k] = &OneStat{k: k, reads: 2, writes: 1, index: -1}
		o = c.m[k]
	} else {
		o.reads++
	}
	if o.ratio() > WRRATIO {
		c.h.update(o)
	}
}

func (c *Candidates) Write(k Key) {
	o, ok := c.m[k]
	if !ok {
		c.m[k] = &OneStat{k: k, reads: 1, writes: 2, index: -1}
		o = c.m[k]
	} else {
		o.writes++
	}
	if o.ratio() > WRRATIO {
		c.h.update(o)
	}
}

type StatsHeap []*OneStat

func (h StatsHeap) Len() int           { return len(h) }
func (h StatsHeap) Less(i, j int) bool { return h[i].ratio() > h[j].ratio() }
func (h StatsHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *StatsHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	n := len(*h)
	*h = append(*h, x.(*OneStat))
	(*h)[n].index = n
}

func (h *StatsHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	x.index = -1
	*h = old[0 : n-1]
	return x
}

// after updating reads and writes in item, re-set
func (h *StatsHeap) update(o *OneStat) {
	if o.index != -1 {
		heap.Remove(h, o.index)
	}
	heap.Push(h, o)
}
