package ddtxn

import "errors"

type Queue struct {
	t    []Query
	n    int
	head int
	tail int
}

var (
	EEMPTY_QUEUE = errors.New("Queue is empty")
)

func NewQueue(n int) *Queue {
	q := &Queue{
		t:    make([]Query, n, n),
		n:    n,
		head: -1,
		tail: 0,
	}
	return q
}

func (q *Queue) Add(t Query) bool {
	if q.tail == q.head {
		// If the tail catches up to the head, we're full
		return false
	}
	if q.head == -1 {
		q.head = 0
	}
	q.t[q.tail] = t
	q.tail = q.tail + 1
	if q.tail >= q.n {
		q.tail = 0
	}
	return true
}

func (q *Queue) Remove() (*Query, error) {
	if q.head == -1 {
		return nil, EEMPTY_QUEUE
	}
	old := q.head
	empty := q.head == q.tail
	q.head = q.head + 1
	if q.head == q.n {
		q.head = 0
	}
	if empty {
		q.head = -1
	}
	return &q.t[old], nil
}
