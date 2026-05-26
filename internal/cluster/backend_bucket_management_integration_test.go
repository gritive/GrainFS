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

type mockBucketAssigner struct {
	fn func(ctx context.Context, bucket, groupID string) error
}

func (m *mockBucketAssigner) ProposeBucketAssignment(ctx context.Context, bucket, groupID string) error {
	return m.fn(ctx, bucket, groupID)
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

	It("returns an error when the assigner has no router", func() {
		b.SetBucketAssigner(&mockBucketAssigner{fn: func(ctx context.Context, bucket, groupID string) error {
			return nil
		}})
		err := b.CreateBucket(ctx, "photos")
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(ContainSubstring("router not configured")))
	})

	It("calls the bucket assigner", func() {
		mgr := NewDataGroupManager()
		mgr.Add(NewDataGroup("group-0", []string{"node-0"}))
		r := NewRouter(mgr)
		r.SetDefault("group-0")
		b.SetRouter(r)

		var calledBucket, calledGroup string
		b.SetBucketAssigner(&mockBucketAssigner{fn: func(ctx context.Context, bucket, groupID string) error {
			calledBucket = bucket
			calledGroup = groupID
			return nil
		}})

		Expect(b.CreateBucket(ctx, "photos")).To(Succeed())
		Expect(calledBucket).To(Equal("photos"))
		Expect(calledGroup).To(Equal("group-0"))
	})

	It("assigns buckets before strict routing", func() {
		mgr := NewDataGroupManager()
		mgr.Add(NewDataGroup("group-0", []string{"node-0"}))
		r := NewRouter(mgr)
		r.SetDefault("group-0")
		r.SetRequireExplicitAssignments(true)

		b.SetRouter(r)
		b.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
			"group-0": {ID: "group-0", PeerIDs: []string{"node-0"}},
		}})
		var assignedBucket, assignedGroup string
		b.SetBucketAssigner(&mockBucketAssigner{fn: func(ctx context.Context, bucket, groupID string) error {
			assignedBucket = bucket
			assignedGroup = groupID
			return nil
		}})

		Expect(b.CreateBucket(ctx, "photos")).To(Succeed())
		Expect(assignedBucket).To(Equal("photos"))
		Expect(assignedGroup).To(Equal("group-0"))
		r.AssignBucket(assignedBucket, assignedGroup)

		g, err := r.RouteKey("photos", "image.jpg")
		Expect(err).NotTo(HaveOccurred())
		Expect(g.ID()).To(Equal("group-0"))
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
		var assignedGroup string
		b.SetBucketAssigner(&mockBucketAssigner{fn: func(ctx context.Context, bucket, groupID string) error {
			assignedGroup = groupID
			return nil
		}})

		Expect(b.CreateBucket(ctx, "__grainfs_volumes")).To(Succeed())
		Expect(assignedGroup).To(Equal("group-8"))
	})

	It("propagates router errors during bucket creation", func() {
		mgr := NewDataGroupManager()
		r := NewRouter(mgr)
		b.SetRouter(r)
		b.SetBucketAssigner(&mockBucketAssigner{fn: func(ctx context.Context, bucket, groupID string) error {
			return nil
		}})

		err := b.CreateBucket(ctx, "photos")
		Expect(err).To(HaveOccurred())
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
