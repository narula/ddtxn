package apps

import "ddtxn"

type Buy struct {
	nproducts      int
	nbidders       int
	portion_sz     int
	nworkers       int
	read_rate      int
	contended_rate int
	bidder_keys    []ddtxn.Key
	product_keys   []ddtxn.Key
}

func InitBuy(s *ddtxn.Store, np, nb, portion_sz, nw, rr, crr int) *Buy {
	b := &Buy{
		nproducts:      np,
		nbidders:       nb,
		portion_sz:     portion_sz,
		nworkers:       nw,
		read_rate:      rr,
		contended_rate: crr,
		bidder_keys:    make([]ddtxn.Key, nb),
		product_keys:   make([]ddtxn.Key, np),
	}

	for i := 0; i < np; i++ {
		k := ddtxn.ProductKey(i)
		b.product_keys[i] = k
		s.CreateKey(k, int32(0), ddtxn.SUM)
	}
	// Uncontended keys
	for i := np; i < nb/10; i++ {
		k := ddtxn.ProductKey(i)
		s.CreateKey(k, int32(0), ddtxn.SUM)
	}
	for i := 0; i < nb; i++ {
		k := ddtxn.UserKey(i)
		b.bidder_keys[i] = k
		s.CreateKey(k, "x", ddtxn.WRITE)
	}
	return b
}

// Actually execute transaction? Or return a txn object which worker
// then executes?  buy.go main() should call worker.Go?  Seems like
// the only reasonable thing.  Or could call Buy.Go() and that just
// feeds transactions to a worker without a loop?

// Worker.go:  Do(txn Query), runs that query
//
// buy.go: DoOne(w *Worker, params..), runs that query
//
// buy.go: MakeOne(params...), returns a Query object to pass to Worker
func (b *Buy) DoOne(w *ddtxn.Worker, local_seed *uint32) *ddtxn.Result {
	var txn ddtxn.Query
	lb := int(ddtxn.RandN(local_seed, uint32(b.portion_sz)))
	bidder := lb + w.ID*b.portion_sz
	amt := int32(ddtxn.RandN(local_seed, 10))
	product := int(ddtxn.RandN(local_seed, uint32(b.nproducts)))
	x := int(ddtxn.RandN(local_seed, uint32(100)))
	if x < b.read_rate {
		if x >= b.contended_rate {
			// Contended read
			txn.K1 = b.product_keys[product]
		} else {
			// Uncontended read
			txn.K1 = b.bidder_keys[bidder]
		}
		r, err := ddtxn.ReadBuyTxn(txn, w)
		_ = err
		return r
	} else {
		txn.K1 = b.bidder_keys[bidder]
		txn.K2 = b.product_keys[product]
		txn.A = amt
		r, err := ddtxn.BuyTxn(txn, w)
		_ = err
		return r
	}
	return nil
}

func (b *Buy) MakeOne(w *ddtxn.Worker, local_seed *uint32) *ddtxn.Query {
	var txn ddtxn.Query
	lb := int(ddtxn.RandN(local_seed, uint32(b.portion_sz)))
	bidder := lb + w.ID*b.portion_sz
	amt := int32(ddtxn.RandN(local_seed, 10))
	product := int(ddtxn.RandN(local_seed, uint32(b.nproducts)))
	x := int(ddtxn.RandN(local_seed, uint32(100)))
	if x < b.read_rate {
		if x >= b.contended_rate {
			// Contended read
			txn.K1 = b.product_keys[product]
		} else {
			// Uncontended read
			txn.K1 = b.bidder_keys[bidder]
		}
	} else {
		txn.K1 = b.bidder_keys[bidder]
		txn.K2 = b.product_keys[product]
		txn.A = amt
	}
	return &txn
}
