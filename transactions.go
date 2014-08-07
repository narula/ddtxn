package ddtxn

import (
	"ddtxn/dlog"
	"flag"
)

var AtomicIncr = flag.Bool("atomic", true, "Use atomic increment version of Buy (LIKE) transaction")

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
	if *AtomicIncr == false {
		return BuyNCTxn(t, tx)
	}
	var r *Result = nil
	tx.WriteInt32(t.K1, 1, SUM)
	tx.WriteInt32(t.K2, t.A, SUM)
	if tx.Commit() == 0 {
		return r, EABORT
	}
	return r, nil
}

// Verison of BUY that puts total in read set (doesn't rely on
// commutatitivity)
func BuyNCTxn(t Query, tx ETransaction) (*Result, error) {
	var r *Result = nil
	tx.WriteInt32(t.K1, 1, SUM)
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

func IncrTxn(t Query, tx ETransaction) (*Result, error) {
	if *SysType == DOPPEL {
		tx.WriteInt32(t.K1, 1, SUM)
	} else {
		br, err := tx.Read(t.K1)
		if err == EABORT {
			dlog.Println(err)
			return nil, EABORT
		}
		sum := br.int_value
		_ = sum
		tx.WriteInt32(t.K1, 1, SUM)
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
				tx.WriteInt32(key[i], int32(0), SUM)
			} else if err != nil {
				return r, EABORT
			} else {
				_ = k
			}
		}
	}

	tx.WriteInt32(ProductKey(int(t.U7)), 1, SUM)
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
				tx.WriteInt32(key[i], int32(0), SUM)
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
		tx.WriteInt32(key[6], int32(0), SUM)
	} else if err != nil {
		return r, EABORT
	} else {
		_ = k
	}
	tx.WriteInt32(key[6], 1, SUM)

	if tx.Commit() == 0 {
		return r, EABORT
	}
	return r, nil
}
