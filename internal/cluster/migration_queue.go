package cluster

import (
	"container/heap"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// migrationItem is a candidate source node in the priority queue.
type migrationItem struct {
	srcNode     string
	diskUsedPct float64
	queuedAt    time.Time
	index       int // maintained by migrationHeap.Swap
}

// effectivePriority = diskUsedPct × (1 + ageMinutes/10).
// Aging boosts older items so starvation can't happen: an item queued 10 min ago
// at 50% competes equally with a freshly-queued item at 100%.
func (m *migrationItem) effectivePriority() float64 {
	ageMin := time.Since(m.queuedAt).Minutes()
	return m.diskUsedPct * (1.0 + ageMin/10.0)
}

// migrationHeap implements heap.Interface as a max-heap (highest effective priority first).
type migrationHeap []*migrationItem

func (h migrationHeap) Len() int { return len(h) }
func (h migrationHeap) Less(i, j int) bool {
	return h[i].effectivePriority() > h[j].effectivePriority()
}
func (h migrationHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *migrationHeap) Push(x any) {
	item := x.(*migrationItem)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *migrationHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
	return item
}

// MigrationPriorityQueue orders migration source nodes by effective priority and
// enforces a token-bucket rate limit on dequeues. Thread-safe.
type MigrationPriorityQueue struct {
	mu      sync.Mutex
	h       migrationHeap
	seen    map[string]*migrationItem // srcNode → item pointer
	limiter *rate.Limiter
}

// NewMigrationPriorityQueue creates a queue that allows ratePerSec dequeues per second.
// burst equals ratePerSec (rounded up to at least 1).
func NewMigrationPriorityQueue(ratePerSec float64) *MigrationPriorityQueue {
	burst := int(ratePerSec)
	if burst < 1 {
		burst = 1
	}
	q := &MigrationPriorityQueue{
		seen:    make(map[string]*migrationItem),
		limiter: rate.NewLimiter(rate.Limit(ratePerSec), burst),
	}
	heap.Init(&q.h)
	return q
}

// Upsert adds srcNode to the queue or updates its diskUsedPct if already present.
// queuedAt is set to now for new items; preserved for existing items.
func (q *MigrationPriorityQueue) Upsert(srcNode string, diskUsedPct float64) {
	q.upsertAt(srcNode, diskUsedPct, time.Now())
}

// upsertAt is like Upsert but accepts an explicit queuedAt for testing.
func (q *MigrationPriorityQueue) upsertAt(srcNode string, diskUsedPct float64, at time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if item, ok := q.seen[srcNode]; ok {
		item.diskUsedPct = diskUsedPct
		heap.Fix(&q.h, item.index)
		return
	}
	item := &migrationItem{
		srcNode:     srcNode,
		diskUsedPct: diskUsedPct,
		queuedAt:    at,
	}
	heap.Push(&q.h, item)
	q.seen[srcNode] = item
}

// TryDequeue returns the highest-priority source node if the token bucket allows.
// Returns ("", false) when the queue is empty or the rate limit is exhausted.
func (q *MigrationPriorityQueue) TryDequeue() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.h.Len() == 0 {
		return "", false
	}
	if !q.limiter.Allow() {
		return "", false
	}
	// Re-heapify to account for aging before popping.
	heap.Init(&q.h)
	item := heap.Pop(&q.h).(*migrationItem)
	delete(q.seen, item.srcNode)
	return item.srcNode, true
}

// Len returns the number of items in the queue.
func (q *MigrationPriorityQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.h.Len()
}
