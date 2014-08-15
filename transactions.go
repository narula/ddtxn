package ddtxn

import (
	"ddtxn/dlog"
	"flag"
	"log"
	"sync/atomic"
)

// I tried keeping a slice of interfaces; the reflection was costly.
// Hard code in random parameter types to re-use for now.
// TODO: clean this up.
type Query struct {
	TXN int
	W   chan struct {
		R *Result
		E error
	}
	T  TID
	K1 Key
	K2 Key
	A  int32
	U1 uint64
	U2 uint64
	U3 uint64
	U4 uint64
	U5 uint64
	U6 uint64
	U7 uint64
	S1 string
	S2 string
	I  int
}

type Result struct {
	V Value
}

var Allocate = flag.Bool("allocate", true, "Allocate results")

func IsRead(t int) bool {
	if t == D_READ_ONE {
		return true
	}
	return false
}

func BuyTxn(t Query, tx ETransaction) (*Result, error) {
	var r *Result = nil
	err := tx.WriteInt32(t.K1, 1, SUM)
	if err != nil {
		return nil, err
	}
	err = tx.WriteInt32(t.K2, t.A, SUM)
	if err != nil {
		return nil, err
	}
	if tx.Commit() == 0 {
		return r, EABORT
	}
	return r, nil
}

// Verison of BUY that always puts keys in the read set
func BuyNCTxn(t Query, tx ETransaction) (*Result, error) {
	var r *Result = nil
	var err error
	err = tx.WriteInt32(t.K1, 1, SUM)
	if err != nil {
		return nil, err
	}
	br, err := tx.Read(t.K2)
	if err == ESTASH {
		return nil, ESTASH
	}
	if err == EABORT {
		dlog.Println("Abort", err, t.K2)
		return r, EABORT
	}
	if err != nil {
		dlog.Fatalf("???")
	}
	var sum int32 = br.value.(int32)
	tx.Write(t.K2, t.A+sum, WRITE)
	if tx.Commit() == 0 {
		return r, EABORT
	}
	return r, nil
}

func ReadTxn(t Query, tx ETransaction) (*Result, error) {
	var r *Result = nil
	v1, err := tx.Read(t.K1)
	if err != nil {
		return r, err
	}
	x := v1.Value()
	_ = x
	var txid TID
	if txid = tx.Commit(); txid == 0 {
		return r, EABORT
	}
	if *Allocate {
		r = &Result{x}
	}
	return r, nil
}

// This is special. It does not use the Commit() protocol, instead it
// just performs atomic increments on keys.  It is impossible to
// abort, and no stats are kept to indicate this key should be in
// split phase or not.  This shouldn't be run in a mix with any other
// transaction types.
func AtomicIncr(t Query, tx ETransaction) (*Result, error) {
	br, err := tx.Store().getKey(t.K1)
	if err != nil || br == nil {
		log.Fatalf("Why no key?")
	}
	atomic.AddInt32(&br.int_value, 1)
	return nil, nil
}

func IncrTxn(t Query, tx ETransaction) (*Result, error) {
	err := tx.WriteInt32(t.K1, 1, SUM)
	if err != nil {
		return nil, err
	}
	if tx.Commit() == 0 {
		return nil, EABORT
	}
	return nil, nil
}

func BigIncrTxn(t Query, tx ETransaction) (*Result, error) {
	var r *Result = nil
	key := [6]Key{}
	key[0] = BidKey(t.U1)
	key[1] = BidKey(t.U2)
	key[2] = BidKey(t.U3)
	key[3] = BidKey(t.U4)
	key[4] = BidKey(t.U5)
	key[5] = BidKey(t.U6)

	for z := 0; z < 10; z++ {
		for i := 0; i < 6; i++ {
			k, err := tx.Read(key[i])
			if err == ESTASH {
				return nil, ESTASH
			}
			if err == ENOKEY {
				if err := tx.WriteInt32(key[i], int32(0), SUM); err != nil {
					return nil, err
				}
			} else if err != nil {
				return r, EABORT
			} else {
				_ = k
			}
		}
	}

	if err := tx.WriteInt32(ProductKey(int(t.U7)), 1, SUM); err != nil {
		return nil, err
	}
	if tx.Commit() == 0 {
		return r, EABORT
	}
	return r, nil
}

// Version of Big that puts keys in read set (doesn't rely on
// commutativity)
func BigRWTxn(t Query, tx ETransaction) (*Result, error) {
	var r *Result = nil

	key := [7]Key{}
	key[0] = BidKey(t.U1)
	key[1] = BidKey(t.U2)
	key[2] = BidKey(t.U3)
	key[3] = BidKey(t.U4)
	key[4] = BidKey(t.U5)
	key[5] = BidKey(t.U6)
	key[6] = ProductKey(int(t.U7))

	for z := 0; z < 10; z++ {
		for i := 0; i < 6; i++ {
			k, err := tx.Read(key[i])
			if err == ESTASH {
				return nil, ESTASH
			}
			if err == ENOKEY {
				if err := tx.WriteInt32(key[i], int32(0), SUM); err != nil {
					return nil, err
				}
			} else if err != nil {
				return r, EABORT
			} else {
				_ = k
			}
		}
	}

	k, err := tx.Read(key[6])
	if err == ESTASH {
		return nil, ESTASH
	}
	if err == ENOKEY {
		if err := tx.WriteInt32(key[6], int32(0), SUM); err != nil {
			return nil, err
		}
	} else if err != nil {
		return r, EABORT
	} else {
		_ = k
	}
	if err := tx.WriteInt32(key[6], 1, SUM); err != nil {
		return nil, err
	}

	if tx.Commit() == 0 {
		return r, EABORT
	}
	return r, nil
}
