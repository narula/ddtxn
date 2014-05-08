package ddtxn

type TStore struct {
	t []Transaction
}

func TSInit(n int) *TStore {
	ts := &TStore{t: make([]Transaction, 0, n)}
	return ts
}

func (ts *TStore) Add(t Transaction) {
	ts.t = append(ts.t, t)
}

func (ts *TStore) clear() {
	ts.t = ts.t[:0]
}
