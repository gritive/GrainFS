package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// SnapshotLegacyPlaintextReadsTotal counts object-metadata snapshot reads that
// hit a pre-Phase-D-snap plaintext file (no GSNE envelope). It must stay flat at
// zero across a full snapshot cycle before D-cut removes the read-compat shim.
var SnapshotLegacyPlaintextReadsTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "grainfs_snapshot_legacy_plaintext_reads_total",
	Help: "Object-metadata snapshot reads served from a legacy plaintext (un-enveloped) file during the Phase D-snap migration window.",
})

// SnapshotOpenErrorsTotal counts object-metadata snapshot files that List() could
// not open (envelope decrypt / auth / KEK-version failure, or corruption) and
// therefore skipped. A non-zero value means a snapshot is silently absent from
// listings, which a restore from that snapshot set would silently miss.
var SnapshotOpenErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "grainfs_snapshot_open_errors_total",
	Help: "Object-metadata snapshot files skipped by List() because they could not be opened (decrypt/auth/KEK-version failure or corruption).",
})
