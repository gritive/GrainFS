package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gritive/GrainFS/internal/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// recordingMetaBucketStore is a MetaBucketStore that delegates Create to a
// callback and applies other mutations directly to a directFSMMetaBucketStore.
// Used by tests that need to observe the groupID passed to CreateBucket without
// a real MetaBucketStore.
type recordingMetaBucketStore struct {
	onCreate func(bucket, groupID string, bypass bool)
}

func (r *recordingMetaBucketStore) CreateBucket(_ context.Context, bucket, groupID string, bypassReserved bool) error {
	if r.onCreate != nil {
		r.onCreate(bucket, groupID, bypassReserved)
	}
	return nil
}
func (r *recordingMetaBucketStore) DeleteBucket(_ context.Context, bucket string) error { return nil }
func (r *recordingMetaBucketStore) SetVersioning(_ context.Context, bucket, state string) error {
	return nil
}
func (r *recordingMetaBucketStore) SetPolicy(_ context.Context, bucket string, policy []byte) error {
	return nil
}
func (r *recordingMetaBucketStore) DeletePolicy(_ context.Context, bucket string) error { return nil }
func (r *recordingMetaBucketStore) Record(bucket string) (BucketRecord, bool) {
	return BucketRecord{}, false
}
func (r *recordingMetaBucketStore) RecordLinearized(_ context.Context, bucket string) (BucketRecord, bool, error) {
	return BucketRecord{}, false, nil
}
func (r *recordingMetaBucketStore) AllRecords() map[string]BucketRecord {
	return nil
}

var _ = Describe("Backend bucket management integration", func() {
	var (
		b   *DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
	})

	It("accepts a nil bucket assigner", func() {
		b.SetBucketAssigner(nil)
		Expect(b.CreateBucket(ctx, "photos")).To(Succeed())
	})

	// Task 8 cutover: CreateBucket now routes through MetaBucketStore, not the
	// legacy assigner. The following tests verify groupID selection via
	// resolveCreateGroupID, which is passed to MetaBucketStore.CreateBucket.

	It("passes groupID from router default to MetaBucketStore", func() {
		mgr := NewDataGroupManager()
		mgr.Add(NewDataGroup("group-0", []string{"node-0"}))
		r := NewRouter(mgr)
		r.SetDefault("group-0")
		b.SetRouter(r)

		var capturedGroup string
		b.SetMetaBucketStore(&recordingMetaBucketStore{
			onCreate: func(bucket, groupID string, bypass bool) {
				capturedGroup = groupID
			},
		})

		Expect(b.CreateBucket(ctx, "photos")).To(Succeed())
		Expect(capturedGroup).To(Equal("group-0"))
	})

	It("selects groupID via shardGroup for bucket placement", func() {
		mgr := NewDataGroupManager()
		mgr.Add(NewDataGroup("group-0", []string{"node-0"}))
		r := NewRouter(mgr)
		r.SetDefault("group-0")
		r.SetRequireExplicitAssignments(true)
		b.SetRouter(r)
		b.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
			"group-0": {ID: "group-0", PeerIDs: []string{"node-0"}},
		}})

		var capturedGroup string
		b.SetMetaBucketStore(&recordingMetaBucketStore{
			onCreate: func(bucket, groupID string, bypass bool) {
				capturedGroup = groupID
			},
		})

		Expect(b.CreateBucket(ctx, "photos")).To(Succeed())
		Expect(capturedGroup).To(Equal("group-0"))
	})

	It("assigns internal buckets to the widest EC group", func() {
		mgr := NewDataGroupManager()
		mgr.Add(NewDataGroup("group-1", []string{"node-0"}))
		mgr.Add(NewDataGroup("group-8", []string{"node-0", "node-1", "node-2"}))
		r := NewRouter(mgr)
		r.SetDefault("group-1")
		r.SetRequireExplicitAssignments(true)
		b.SetRouter(r)
		b.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
			"group-1": {ID: "group-1", PeerIDs: []string{"node-0"}},
			"group-8": {ID: "group-8", PeerIDs: []string{"node-0", "node-1", "node-2"}},
		}})

		var capturedGroup string
		b.SetMetaBucketStore(&recordingMetaBucketStore{
			onCreate: func(bucket, groupID string, bypass bool) {
				capturedGroup = groupID
			},
		})

		Expect(b.CreateBucket(ctx, "__grainfs_volumes")).To(Succeed())
		Expect(capturedGroup).To(Equal("group-8"))
	})

	It("force deletes objects and the bucket", func() {
		Expect(b.CreateBucket(ctx, "todelete")).To(Succeed())
		_, err := b.PutObject(ctx, "todelete", "a.txt", strings.NewReader("aaa"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		_, err = b.PutObject(ctx, "todelete", "b.txt", strings.NewReader("bbb"), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		Expect(b.ForceDeleteBucket(ctx, "todelete")).To(Succeed())
		err = b.HeadBucket(ctx, "todelete")
		Expect(errors.Is(err, storage.ErrBucketNotFound)).To(BeTrue())
	})

	It("returns bucket-not-found for missing force-delete buckets", func() {
		err := b.ForceDeleteBucket(ctx, "nope")
		Expect(errors.Is(err, storage.ErrBucketNotFound)).To(BeTrue())
	})

	It("propagates canceled contexts during force-delete", func() {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()
		err := b.ForceDeleteBucket(cancelCtx, "any")
		Expect(err).To(HaveOccurred())
	})

	It("force deletes buckets with multiple object versions", func() {
		Expect(b.CreateBucket(ctx, "mv-bucket")).To(Succeed())
		for i := range 3 {
			_, err := b.PutObject(ctx, "mv-bucket", "doc.txt", strings.NewReader(fmt.Sprintf("v%d", i)), "text/plain")
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(b.ForceDeleteBucket(ctx, "mv-bucket")).To(Succeed())
		err := b.HeadBucket(ctx, "mv-bucket")
		Expect(errors.Is(err, storage.ErrBucketNotFound)).To(BeTrue())
	})

	It("force deletes slash keys with versioned prefix collisions", func() {
		Skip("Phase 3: ForceDeleteBucket/HeadBucket not fully adapted to quorum meta store")

		Expect(b.CreateBucket(ctx, "slash-bucket")).To(Succeed())
		_, err := b.PutObject(ctx, "slash-bucket", "dir", strings.NewReader("versioned"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		_, err = b.PutObject(ctx, "slash-bucket", "dir/file", strings.NewReader("nested"), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		Expect(b.ForceDeleteBucket(ctx, "slash-bucket")).To(Succeed())
		err = b.HeadBucket(ctx, "slash-bucket")
		Expect(errors.Is(err, storage.ErrBucketNotFound)).To(BeTrue())
	})
})
