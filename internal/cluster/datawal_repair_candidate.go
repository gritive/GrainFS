package cluster

import (
	"strconv"
	"sync"
)

// DataWALRepairReason classifies why a shard was nominated for repair.
type DataWALRepairReason string

const (
	DataWALRepairMissing      DataWALRepairReason = "missing"
	DataWALRepairSizeMismatch DataWALRepairReason = "size_mismatch"
)

// DataWALRepairCandidate describes one shard that needs repair after a
// metadata-only WAL record is discovered during startup.
type DataWALRepairCandidate struct {
	Bucket       string
	ShardKey     string
	ShardIdx     int
	ExpectedSize int64
	Reason       DataWALRepairReason
}

// DataWALRepairSink receives repair candidates discovered during data WAL
// startup scanning. Implementations must be safe for concurrent use.
type DataWALRepairSink interface {
	AddDataWALRepairCandidate(DataWALRepairCandidate)
}

// DataWALRepairCollector accumulates repair candidates, coalescing duplicate
// (bucket, shardKey, shardIdx) entries so the last write wins. It is safe
// for concurrent use.
type DataWALRepairCollector struct {
	mu      sync.Mutex
	byKey   map[string]DataWALRepairCandidate
	ordered []string
}

// NewDataWALRepairCollector returns a ready-to-use collector.
func NewDataWALRepairCollector() *DataWALRepairCollector {
	return &DataWALRepairCollector{byKey: make(map[string]DataWALRepairCandidate)}
}

// AddDataWALRepairCandidate records a candidate. Duplicate keys are
// overwritten so the last add wins (e.g. size_mismatch supersedes missing).
func (c *DataWALRepairCollector) AddDataWALRepairCandidate(candidate DataWALRepairCandidate) {
	if c == nil {
		return
	}
	key := candidate.Bucket + "\x00" + candidate.ShardKey + "\x00" + strconv.Itoa(candidate.ShardIdx)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.byKey[key]; !ok {
		c.ordered = append(c.ordered, key)
	}
	c.byKey[key] = candidate
}

// Candidates returns all collected candidates in insertion order.
func (c *DataWALRepairCollector) Candidates() []DataWALRepairCandidate {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]DataWALRepairCandidate, 0, len(c.ordered))
	for _, key := range c.ordered {
		out = append(out, c.byKey[key])
	}
	return out
}
