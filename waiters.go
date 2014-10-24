package ddtxn

import "flag"

var TriggerCount = flag.Int("trigger", 100000, "How long the queue can get before triggering a phase change\n")

type TStore struct {
	t []Query
	n int
}

func TSInit(n int) *TStore {
	ts := &TStore{t: make([]Query, 0, n)}
	return ts
}

func (ts *TStore) Add(t Query) bool {
	ts.t = append(ts.t, t)
	ts.n += 1
	if ts.n == *TriggerCount {
		return true
	}
	return false
}

func (ts *TStore) clear() {
	ts.t = ts.t[:0]
	ts.n = 0
}

type RetryHeap []Query

func (h RetryHeap) Len() int           { return len(h) }
func (h RetryHeap) Less(i, j int) bool { return h[j].TS.After(h[i].TS) }
func (h RetryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *RetryHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(Query))
}

func (h *RetryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
