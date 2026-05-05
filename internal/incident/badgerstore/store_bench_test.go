package badgerstore

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/incident"
)

const (
	benchVolumeBucket = "__grainfs_volumes"
	benchTargetVolume = "target-vol"
)

// seedBenchStore populates the store with `total` incidents:
//   - matchN incidents under {Bucket: __grainfs_volumes, Key: __vol/target-vol/blk_<i>}
//   - ~33% of the rest under same bucket but different volume names (close-miss distractors)
//   - remainder in other buckets entirely
//
// Timestamps are uniform across [now-7d, now], so the newest 500 contain
// approximately matchN/total × 500 matching incidents — a realistic worst
// case for StatVolume's filter (most scanned items don't match).
func seedBenchStore(b *testing.B, total, matchN int) *Store {
	b.Helper()
	db, err := badger.Open(badger.DefaultOptions(b.TempDir()).WithLogger(nil))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	s := New(db)
	ctx := context.Background()

	base := time.Now().Add(-7 * 24 * time.Hour)
	step := 7 * 24 * time.Hour / time.Duration(total)

	matchSpacing := total / matchN
	matchedSoFar := 0
	for i := 0; i < total; i++ {
		ts := base.Add(time.Duration(i) * step)
		var scope incident.Scope
		switch {
		case i%matchSpacing == 0 && matchedSoFar < matchN:
			scope = incident.Scope{
				Kind:   incident.ScopeObject,
				Bucket: benchVolumeBucket,
				Key:    fmt.Sprintf("__vol/%s/blk_%05d", benchTargetVolume, i),
			}
			matchedSoFar++
		case i%3 == 0:
			scope = incident.Scope{
				Kind:   incident.ScopeObject,
				Bucket: benchVolumeBucket,
				Key:    fmt.Sprintf("__vol/other-%d/blk_%05d", i%5, i),
			}
		default:
			scope = incident.Scope{
				Kind:   incident.ScopeObject,
				Bucket: fmt.Sprintf("bench-other-%d", i%10),
				Key:    fmt.Sprintf("k/%05d", i),
			}
		}

		st := incident.IncidentState{
			ID:        fmt.Sprintf("cid-%06d", i),
			State:     incident.StateObserved,
			Scope:     scope,
			UpdatedAt: ts,
		}
		if err := s.Put(ctx, st); err != nil {
			b.Fatal(err)
		}
	}
	return s
}

// BenchmarkStoreList_500 measures Store.List(500) latency at N total
// incidents. The result should plateau once N >= 500 because List caps
// the iterator; what scales is the per-item secondary getStateTxn cost.
func BenchmarkStoreList_500(b *testing.B) {
	for _, total := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("total=%d", total), func(b *testing.B) {
			s := seedBenchStore(b, total, 50)
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := s.List(ctx, 500)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkStatVolumeFilter simulates StatVolume's incident-lookup step:
// List(500) + scope filter (bucket + key prefix) + cap at 50. This is the
// realistic per-request cost the TODO's "100ms" threshold targets.
func BenchmarkStatVolumeFilter(b *testing.B) {
	for _, total := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("total=%d", total), func(b *testing.B) {
			s := seedBenchStore(b, total, 50)
			ctx := context.Background()
			blockPrefix := "__vol/" + benchTargetVolume + "/blk_"
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				all, err := s.List(ctx, 500)
				if err != nil {
					b.Fatal(err)
				}
				var matches []incident.IncidentState
				for _, st := range all {
					if st.Scope.Bucket != benchVolumeBucket {
						continue
					}
					if st.Scope.Key == benchTargetVolume || strings.HasPrefix(st.Scope.Key, blockPrefix) {
						matches = append(matches, st)
						if len(matches) >= 50 {
							break
						}
					}
				}
				_ = matches
			}
		})
	}
}
