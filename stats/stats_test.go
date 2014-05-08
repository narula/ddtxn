package stats

import (
	"fmt"
	"testing"
)

func TestHistogram(t *testing.T) {
	lh := MakeLatencyHistogram(1, 100)
	lh.AddOne(67)
	lh.AddOne(42)
	if lh.GetPercentile(50) != 42 {
		t.Errorf("bad")
	}
	if lh.GetPercentile(1) != 42 {
		t.Errorf("bad")
	}
	if lh.GetPercentile(100) != 67 {
		t.Errorf("bad")
	}
	if lh.GetPercentile(82) != 67 {
		t.Errorf("bad")
	}

	lh = MakeLatencyHistogram(100, 100000)
	for i := 0; i < 20000; i++ {
		lh.AddOne(int64(i))
	}
	fmt.Println(lh.GetPercentile(25))
}
