package ddtxn

import (
	"log"
	"sync"
	"testing"
)

func TestConcurrentGotomic(t *testing.T) {
	s := NewStore()
	sp := 5
	for i := 0; i < 10; i++ {
		k := ProductKey(i)
		s.CreateKey(k, int32(0), SUM)
		_, err := s.Get(k)
		if err != nil {
			log.Fatalf("Could not get key I just put %v\n", k)
		}
	}
	pkey := ProductKey(int(sp - 1))
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(n int) {
			for j := 0; j < 10000; j++ {
				_, err := s.Get(pkey)
				if err != nil {
					t.Errorf("%v Could not get key\n", n)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}
