package cluster

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouter_RouteKey_DefaultGroup(t *testing.T) {
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup("group-0", []string{"node-0"}))
	r := NewRouter(mgr)
	r.SetDefault("group-0")

	g, err := r.RouteKey("my-bucket", "any/key/path")
	require.NoError(t, err)
	assert.Equal(t, "group-0", g.ID())
}

func TestRouter_RouteKey_ExplicitBucket(t *testing.T) {
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup("group-0", []string{"node-0"}))
	mgr.Add(NewDataGroup("group-1", []string{"node-1"}))
	r := NewRouter(mgr)
	r.SetDefault("group-0")
	r.AssignBucket("special-bucket", "group-1")

	g, err := r.RouteKey("special-bucket", "key")
	require.NoError(t, err)
	assert.Equal(t, "group-1", g.ID())
}

func TestRouter_RouteKey_NoDefault_ReturnsError(t *testing.T) {
	mgr := NewDataGroupManager()
	r := NewRouter(mgr)
	_, err := r.RouteKey("unknown-bucket", "key")
	require.ErrorIs(t, err, ErrNoGroup)
}

func TestRouter_RouteKey_KeyIgnored(t *testing.T) {
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup("group-0", []string{"node-0"}))
	r := NewRouter(mgr)
	r.SetDefault("group-0")

	g1, err := r.RouteKey("bucket", "key-aaaa")
	require.NoError(t, err)
	g2, err := r.RouteKey("bucket", "key-zzzz-completely-different")
	require.NoError(t, err)
	assert.Equal(t, g1.ID(), g2.ID(), "same bucket → same group, key is ignored")
}

func TestRouter_ConcurrentReadWrite_Race(t *testing.T) {
	const nGroups = 10
	mgr := NewDataGroupManager()
	for i := range nGroups {
		mgr.Add(NewDataGroup(fmt.Sprintf("group-%d", i), []string{fmt.Sprintf("node-%d", i)}))
	}
	r := NewRouter(mgr)
	r.SetDefault("group-0")

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				r.SetDefault(fmt.Sprintf("group-%d", i%nGroups))
				r.AssignBucket("bucket-a", fmt.Sprintf("group-%d", (i+1)%nGroups))
			}
		}
	}()

	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = r.RouteKey("bucket-a", "some-key")
					_, _ = r.RouteKey("unknown-bucket", "some-key")
				}
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}
