package ddtxn

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestGenZipf(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	z := NewZipf(r, 1.000001, 1, uint64(100))
	n := 1000
	x := make([]uint64, n)
	for i := 0; i < n; i++ {
		x[i] = z.Uint64()
	}
	first := 0
	second := 0
	third := 0
	for i := 0; i < len(x); i++ {
		if x[i] == uint64(0) {
			first++
		} else if x[i] == uint64(1) {
			second++
		} else if x[i] == 2 {
			third++
		}
	}
	fmt.Printf("mine 1.000001,: first: %v, second: %v, third: %v\n", first, second, third)

	z = NewZipf(r, .9999, 1, uint64(100))
	n = 1000
	x = make([]uint64, n)
	for i := 0; i < n; i++ {
		x[i] = z.Uint64()
	}
	first = 0
	second = 0
	third = 0
	for i := 0; i < len(x); i++ {
		if x[i] == uint64(0) {
			first++
		} else if x[i] == uint64(1) {
			second++
		} else if x[i] == 2 {
			third++
		}
	}
	fmt.Printf("mine .9999: first: %v, second: %v, third: %v\n", first, second, third)
}

func TestGenZipfGo(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	z := rand.NewZipf(r, 1.000001, 1, uint64(100))
	n := 1000
	x := make([]uint64, n)
	for i := 0; i < n; i++ {
		x[i] = z.Uint64()
	}
	first := 0
	second := 0
	third := 0
	for i := 0; i < len(x); i++ {
		if x[i] == uint64(0) {
			first++
		} else if x[i] == uint64(1) {
			second++
		} else if x[i] == 2 {
			third++
		}
	}
	fmt.Printf("go 1.000001: first: %v, second: %v, third: %v\n", first, second, third)
}
