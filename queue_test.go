package ddtxn

import "testing"

func TestQueue(t *testing.T) {
	q := NewQueue(100)
	for i := 0; i < 100; i++ {
		q.Add(Query{TXN: i})
	}
}
