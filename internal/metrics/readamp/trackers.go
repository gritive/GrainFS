package readamp

// Pre-built trackers for the read paths we want to characterize, sized
// so each row in the resulting hit-rate table corresponds to a buffer
// budget an operator could plausibly approve. Capacity is in entries;
// each entry is one block (volume) or one shard (EC).
//
// Volume blocks are 4 KB. EC shards are typically 1 MB (object_size /
// k=4). Memory cost per resident entry is ~50-100 bytes of LRU
// bookkeeping, NOT the block payload (we are simulating, not caching).
//
// Three sizes per path so the resulting curve shows where the working
// set actually sits. If hit rate is the same at 16 MB and 256 MB, the
// workload has no temporal locality at any reachable budget.
var (
	// Volume(4 KB blocks):
	//   16 MB = 4096 entries — pocket-sized, fits inside the L3 of most
	//                          servers. Almost free if it works.
	//   64 MB = 16384 entries — comparable to BadgerDB's default block
	//                          cache. Realistic for a busy node.
	//   256 MB = 65536 entries — generous; if even this misses, the
	//                          working set is too big and a UBC won't
	//                          fix it.
	VolumeBlock16MB  = New("volume_block_16mb", 4096)
	VolumeBlock64MB  = New("volume_block_64mb", 16384)
	VolumeBlock256MB = New("volume_block_256mb", 65536)

	// EC shard (1 MB shards):
	//   16 MB = 16 entries — only the very last few objects.
	//   64 MB = 64 entries — a small hot set.
	//   256 MB = 256 entries — broad enough to exercise re-reads of
	//                          recently-fetched objects.
	ECShard16MB  = New("ec_shard_16mb", 16)
	ECShard64MB  = New("ec_shard_64mb", 64)
	ECShard256MB = New("ec_shard_256mb", 256)

	// Storage backend (disk-touching reads). Sees every read that
	// CachedBackend missed — the "post-object-cache, pre-disk" boundary.
	// Tells us what a unified buffer cache below CachedBackend would
	// have caught, so the curve directly compares "object cache only"
	// (today) vs "object cache + UBC". Sized in entries; key
	// granularity is whatever the caller passes (typically bucket+key
	// for the object backend, so each entry = one object miss).
	//
	// Sizes pick the same memory budgets as the volume trackers; what
	// differs is what fits — bigger objects mean fewer entries per MB.
	// We keep the same N (4096/16384/65536) so dashboards line up
	// across paths even if cache budgets are interpreted differently.
	BackendObject16MB  = New("backend_object_16mb", 4096)
	BackendObject64MB  = New("backend_object_64mb", 16384)
	BackendObject256MB = New("backend_object_256mb", 65536)
)

// RecordVolumeBlock fans one access out to all three volume-block
// trackers. Callers pass the same key they would use to identify the
// block on disk (typically the dedup-resolved physical key).
func RecordVolumeBlock(key string) {
	VolumeBlock16MB.Record(key)
	VolumeBlock64MB.Record(key)
	VolumeBlock256MB.Record(key)
}

// RecordECShard fans one access out to all three EC-shard trackers.
// Caller is the shard read path on the EC reconstruction goroutine.
func RecordECShard(key string) {
	ECShard16MB.Record(key)
	ECShard64MB.Record(key)
	ECShard256MB.Record(key)
}

// RecordBackendObject fans one access out to all three backend-object
// trackers. Caller is the lowest-level disk-touching GetObject — the
// boundary CachedBackend has already passed before the data reaches
// the actual storage backend. Hits here are what a unified buffer
// cache below CachedBackend would have absorbed.
func RecordBackendObject(key string) {
	BackendObject16MB.Record(key)
	BackendObject64MB.Record(key)
	BackendObject256MB.Record(key)
}
