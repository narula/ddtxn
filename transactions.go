package ddtxn

import (
	"ddtxn/dlog"
	"flag"
)

// I tried keeping a slice of interfaces; the reflection was costly.
// Hard code in random parameter types to re-use for now.
// TODO: clean this up.
type Query struct {
	TXN int
	W   chan *Result
	T   TID
	K1  Key
	K2  Key
	A   int32
	U1  uint64
	U2  uint64
	U3  uint64
	U4  uint64
	U5  uint64
	U6  uint64
	U7  uint64
	S1  string
	S2  string
	I   int
}

type Result struct {
	T TID
	V Value
	C bool // committed?
}

var Allocate = flag.Bool("allocate", true, "Allocate results")

func IsRead(t int) bool {
	if t == D_READ_BUY {
		return true
	}
	return false
}

func BuyTxn(t Query, w *Worker) (*Result, error) {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	tx := w.ctxn
	tx.WriteInt32(t.K1, 1, SUM)
	tx.WriteInt32(t.K2, t.A, SUM)
	if tx.Commit() == 0 {
		w.Naborts++
		return r, EABORT
	}
	w.Nstats[D_BUY]++
	if *Allocate {
		r.C = true
	}
	return r, nil
}

// Verison of BUY that puts total in read set
func BuyNCTxn(t Query, w *Worker) (*Result, error) {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	tx := w.ctxn
	tx.Write(t.K1, "x", WRITE)
	br, err := tx.Read(t.K2)
	if err == ESTASH {
		return nil, ESTASH
	}
	if err == EABORT {
		dlog.Println("Abort", br, err, t.K2)
		return r, EABORT
	}
	var sum int32
	if err == nil {
		sum = br.value.(int32)
	}
	tx.Write(t.K2, t.A+sum, WRITE)
	if tx.Commit() == 0 {
		w.Naborts++
		return r, EABORT
	}
	w.Nstats[D_BUY_NC]++
	if *Allocate {
		r.C = true
	}
	return r, nil
}

// Commutative BID
func BidTxn(t Query, w *Worker) (*Result, error) {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	tx := w.ctxn
	tx.Write(t.K1, t.S1, WRITE)
	tx.Write(t.K2, t.A, MAX)
	if tx.Commit() == 0 {
		return r, EABORT
	}
	w.Nstats[D_BID]++
	if *Allocate {
		r.C = true
	}
	return r, nil
}

// Version of Bid that puts bid in read set
func BidNCTxn(t Query, w *Worker) (*Result, error) {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}
	tx := w.ctxn
	tx.Write(t.K1, t.S1, WRITE)
	high_bid, err := tx.Read(t.K2)
	if err == ESTASH {
		return nil, ESTASH
	}
	if err == EABORT {
		dlog.Println("Abort", high_bid, err, t.K2)
		return r, EABORT
	}
	if err == ENOKEY || t.A > high_bid.value.(int32) {
		tx.Write(t.K2, t.A, WRITE)
	}
	if tx.Commit() == 0 {
		return r, EABORT
	}
	w.Nstats[D_BID_NC]++
	if *Allocate {
		r.C = true
	}
	return r, nil
}

func ReadBuyTxn(t Query, w *Worker) (*Result, error) {
	var r *Result = nil
	if *Allocate {
		r = &Result{C: false}
	}

	tx := w.ctxn
	v1, err := tx.Read(t.K1)
	if err != nil {
		return r, err
	}
	var txid TID
	if txid = tx.Commit(); txid == 0 {
		return r, EABORT
	}
	if *Allocate {
		r = &Result{txid, v1.Value(), true}
	}
	w.Nstats[D_READ_BUY]++
	return r, nil
}
