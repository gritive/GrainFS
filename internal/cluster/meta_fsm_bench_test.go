package cluster

import (
	"fmt"
	"testing"
)

func BenchmarkMetaFSMSnapshotObjectIndex4096(b *testing.B) {
	f := NewMetaFSM()
	const objects = 4096

	f.mu.Lock()
	for i := 0; i < objects; i++ {
		entry := ObjectIndexEntry{
			Bucket:           "bench",
			Key:              fmt.Sprintf("obj-%08d", i),
			VersionID:        fmt.Sprintf("v-%08d", i),
			PlacementGroupID: "group-0",
			Size:             64 << 10,
			ContentType:      "application/octet-stream",
			ETag:             fmt.Sprintf("%032x", i),
			ModTime:          int64(i),
		}
		f.objectIndex[objectIndexVersionKey(entry.Bucket, entry.Key, entry.VersionID)] = entry
		f.objectLatest[objectIndexLatestKey(entry.Bucket, entry.Key)] = entry.VersionID
	}
	f.mu.Unlock()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := f.Snapshot(); err != nil {
			b.Fatal(err)
		}
	}
}
