package nfs4server

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestExportsAtomicPointerRace(t *testing.T) {
	s := &Server{}
	s.exports.Store(emptySnap)

	var stop atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				snap := s.loadExports()
				_ = snap.byBucket
				_ = snap.sortedNames
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for gen := uint64(0); gen < 1000; gen++ {
			s.exports.Store(buildSnap(map[string]exportConfig{
				"a": {generation: gen},
				"b": {generation: gen, readOnly: gen%2 == 0},
			}))
		}
		stop.Store(true)
	}()

	wg.Wait()
}
