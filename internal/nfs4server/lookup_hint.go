package nfs4server

import (
	"container/list"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/metrics"
)

const (
	hinterMaxEntries = 4096
	hinterTTL        = 5 * time.Minute
	hinterSweepEvery = time.Minute
)

type hintKey struct {
	client string
	bucket string
}

type hintEntry struct {
	key hintKey
	at  time.Time
}

type unknownExportHinter struct {
	window time.Duration

	mu    sync.Mutex
	order *list.List
	index map[hintKey]*list.Element

	stopSweep chan struct{}
	closeOnce sync.Once
}

func newUnknownExportHinter(window time.Duration) *unknownExportHinter {
	h := &unknownExportHinter{
		window:    window,
		order:     list.New(),
		index:     make(map[hintKey]*list.Element),
		stopSweep: make(chan struct{}),
	}
	go h.sweepLoop()
	return h
}

func (h *unknownExportHinter) Close() {
	h.closeOnce.Do(func() {
		close(h.stopSweep)
	})
}

func (h *unknownExportHinter) shouldLog(client, bucket string) bool {
	key := hintKey{client: client, bucket: bucket}
	now := time.Now()

	h.mu.Lock()
	defer h.mu.Unlock()

	if el, ok := h.index[key]; ok {
		prev := el.Value.(*hintEntry)
		if now.Sub(prev.at) < h.window {
			return false
		}
		prev.at = now
		h.order.MoveToFront(el)
		return true
	}

	if h.order.Len() >= hinterMaxEntries {
		oldest := h.order.Back()
		if oldest != nil {
			delete(h.index, oldest.Value.(*hintEntry).key)
			h.order.Remove(oldest)
		}
	}

	el := h.order.PushFront(&hintEntry{key: key, at: now})
	h.index[key] = el
	return true
}

func (h *unknownExportHinter) emit(client, bucket string) {
	metrics.NFSLookupUnknownExportTotal.Inc()
	if !h.shouldLog(client, bucket) {
		return
	}
	log.Warn().
		Str("bucket", bucket).
		Str("client", client).
		Msgf("nfs4: lookup unknown export bucket=%q from client=%s; register with: grainfs nfs export add %s",
			bucket, client, bucket)
}

func (h *unknownExportHinter) sweepLoop() {
	t := time.NewTicker(hinterSweepEvery)
	defer t.Stop()
	for {
		select {
		case <-h.stopSweep:
			return
		case now := <-t.C:
			h.sweepExpired(now)
		}
	}
}

func (h *unknownExportHinter) sweepExpired(now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for {
		oldest := h.order.Back()
		if oldest == nil {
			return
		}
		e := oldest.Value.(*hintEntry)
		if now.Sub(e.at) < h.window {
			return
		}
		delete(h.index, e.key)
		h.order.Remove(oldest)
	}
}
