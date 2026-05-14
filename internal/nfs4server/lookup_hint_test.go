package nfs4server

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHinter_RatelimitsByClientAndBucket(t *testing.T) {
	h := newUnknownExportHinter(5 * time.Minute)
	defer h.Close()

	require.True(t, h.shouldLog("10.0.0.5", "foo"))
	require.False(t, h.shouldLog("10.0.0.5", "foo"))
	require.True(t, h.shouldLog("10.0.0.5", "bar"))
	require.True(t, h.shouldLog("10.0.0.7", "foo"))
}

func TestHinter_WindowExpiry(t *testing.T) {
	h := newUnknownExportHinter(20 * time.Millisecond)
	defer h.Close()

	require.True(t, h.shouldLog("10.0.0.5", "foo"))
	time.Sleep(40 * time.Millisecond)
	require.True(t, h.shouldLog("10.0.0.5", "foo"))
}

func TestHinter_LRUEviction(t *testing.T) {
	h := newUnknownExportHinter(5 * time.Minute)
	defer h.Close()

	for i := 0; i < hinterMaxEntries+100; i++ {
		h.shouldLog("c", fmt.Sprintf("b-%d", i))
	}

	h.mu.Lock()
	require.LessOrEqual(t, h.order.Len(), hinterMaxEntries)
	require.LessOrEqual(t, len(h.index), hinterMaxEntries)
	h.mu.Unlock()
}

func TestHinter_ConcurrentSafe(t *testing.T) {
	h := newUnknownExportHinter(5 * time.Minute)
	defer h.Close()

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				h.shouldLog(fmt.Sprintf("c-%d", g), fmt.Sprintf("b-%d", i%50))
			}
		}(g)
	}
	wg.Wait()
}

func TestOpLookup_UnknownRootExportCallsHinter(t *testing.T) {
	h := newUnknownExportHinter(time.Minute)
	defer h.Close()

	srv := NewServer(nil)
	defer srv.Close()
	d := &Dispatcher{
		state:       NewStateManager(),
		server:      srv,
		currentPath: "/",
		clientAddr:  "10.0.0.5:2049",
		hinter:      h,
	}

	res := d.opLookup([]byte("missing"))
	require.Equal(t, NFS4ERR_NOENT, res.Status)
	require.False(t, h.shouldLog("10.0.0.5:2049", "missing"))
}
