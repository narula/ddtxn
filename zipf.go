package ddtxn

import (
	"fmt"
	"math"
	"math/rand"
)

type Zipf struct {
	n          int64
	theta      float64
	zetan      float64
	alpha      float64
	pow_half   float64
	eta        float64
	local_seed uint32
}

func czeta(theta float64, n int64) float64 {
	var sum float64
	var i int64
	for i = 1; i < n+1; i++ {
		sum += math.Pow(1/float64(i), theta)
	}
	return sum
}

func NewZipf(n int64, theta float64) *Zipf {
	x := czeta(theta, n)
	z := &Zipf{
		n:          n,
		theta:      theta,
		zetan:      x,
		alpha:      1 / (1 - theta),
		eta:        (1 - math.Pow(float64(2/n), 1-theta)) / (1 - czeta(theta, 2)/x),
		pow_half:   math.Pow(0.5, theta),
		local_seed: uint32(rand.Intn(1000000)),
	}
	fmt.Printf("n: %v, theta: %v, zetan: %v, alpha: %v, eta: %v, pow_half: %v, local_seed: %v\n", z.n, z.theta, z.zetan, z.alpha, z.eta, z.pow_half, z.local_seed)
	return z
}

func (z *Zipf) NextSeeded() int64 {
	return z.Next(&z.local_seed)
}

func (z *Zipf) Next(local_seed *uint32) int64 {
	u := float64(RandN(local_seed, 1000000)) / 1000000
	uz := u * z.zetan
	if uz < 1 {
		return 1
	}
	if uz < 1+z.pow_half {
		return 2
	}
	x := math.Pow((z.eta*u)-z.eta+1, z.alpha)
	return 1 + int64(float64(z.n)*x)
}
