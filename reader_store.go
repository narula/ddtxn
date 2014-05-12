package ddtxn

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
