package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// AppendSizeCapRejectedTotal counts AppendObject requests rejected by the
// FSM-side authoritative size cap check.
var AppendSizeCapRejectedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "grainfs_append_size_cap_rejected_total",
	Help: "Number of AppendObject requests rejected because the object would exceed SizeCapBytes.",
})

// AppendCoalescedDepth is a histogram of coalesced[] entry counts per
// successful AppendObject (sampled after Raft apply).
var AppendCoalescedDepth = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "grainfs_append_coalesced_depth",
	Help:    "Distribution of coalesced[] entry counts per AppendObject success.",
	Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024},
})

// AppendCoalescedTotalBytes is a histogram of total object size (obj.Size)
// per successful AppendObject (sampled after Raft apply).
var AppendCoalescedTotalBytes = promauto.NewHistogram(prometheus.HistogramOpts{
	Name: "grainfs_append_coalesced_total_bytes",
	Help: "Distribution of total object size per AppendObject success.",
	Buckets: []float64{
		1 << 20,   // 1 MiB
		16 << 20,  // 16 MiB
		256 << 20, // 256 MiB
		4 << 30,   // 4 GiB
		64 << 30,  // 64 GiB
		1 << 40,   // 1 TiB
	},
})

// AppendCoalescedEntriesAtCap counts the times MaxCoalescedEntries soft limit
// was reached, triggering a recoalesce-of-coalesced path.
var AppendCoalescedEntriesAtCap = promauto.NewCounter(prometheus.CounterOpts{
	Name: "grainfs_append_coalesced_entries_at_cap_total",
	Help: "Number of times MaxCoalescedEntries soft limit was reached (recoalesce-of-coalesced trigger).",
})
