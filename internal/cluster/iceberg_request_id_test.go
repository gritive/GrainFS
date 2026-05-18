package cluster

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetaCatalogRequestIDUnique(t *testing.T) {
	t.Run("two instances no collision on first call", func(t *testing.T) {
		a := &MetaCatalog{idPrefix: newIcebergRequestIDPrefix()}
		b := &MetaCatalog{idPrefix: newIcebergRequestIDPrefix()}
		idA := a.requestID("create-table")
		idB := b.requestID("create-table")
		assert.NotEqual(t, idA, idB, "two MetaCatalog instances must not produce the same requestID")
	})

	t.Run("single instance 1000 sequential calls all unique", func(t *testing.T) {
		c := &MetaCatalog{idPrefix: newIcebergRequestIDPrefix()}
		seen := make(map[string]struct{}, 1000)
		for i := 0; i < 1000; i++ {
			id := c.requestID("create-table")
			_, dup := seen[id]
			assert.False(t, dup, "duplicate requestID detected: %s", id)
			seen[id] = struct{}{}
		}
		assert.Len(t, seen, 1000)
	})

	t.Run("1000 concurrent calls from 8 goroutines all unique", func(t *testing.T) {
		const goroutines = 8
		const callsEach = 1000
		c := &MetaCatalog{idPrefix: newIcebergRequestIDPrefix()}

		var wg sync.WaitGroup
		var sm sync.Map
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < callsEach; i++ {
					id := c.requestID(fmt.Sprintf("create-table-%d", i))
					sm.Store(id, struct{}{})
				}
			}()
		}
		wg.Wait()

		count := 0
		sm.Range(func(_, _ any) bool {
			count++
			return true
		})
		assert.Equal(t, goroutines*callsEach, count, "all requestIDs across goroutines must be unique")
	})
}
