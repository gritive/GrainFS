package cluster

import (
	"bytes"
	"context"
	"time"

	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Object quarantine integration", func() {
	var (
		b   *DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
		Expect(b.CreateBucket(ctx, "b")).To(Succeed())
	})

	It("blocks reads for only the affected object", func() {
		_, err := b.PutObject(ctx, "b", "bad", bytes.NewReader([]byte("bad")), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		_, err = b.PutObject(ctx, "b", "good", bytes.NewReader([]byte("good")), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())

		Expect(b.QuarantineObject(ctx, "b", "bad", "", "corrupt_blob", "CRC mismatch")).To(Succeed())

		_, _, err = b.GetObject(ctx, "b", "bad")
		Expect(err).To(MatchError(ErrObjectQuarantined))

		rc, _, err := b.GetObject(ctx, "b", "good")
		Expect(err).NotTo(HaveOccurred())
		Expect(rc.Close()).To(Succeed())
	})

	It("blocks writes to the affected object", func() {
		Expect(b.QuarantineObject(ctx, "b", "bad", "", "corrupt_blob", "CRC mismatch")).To(Succeed())

		_, err := b.PutObject(ctx, "b", "bad", bytes.NewReader([]byte("new")), "application/octet-stream")
		Expect(err).To(MatchError(ErrObjectQuarantined))

		_, err = b.PutObject(ctx, "b", "good", bytes.NewReader([]byte("good")), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
	})

	It("returns not found for an unknown object", func() {
		_, _, err := b.GetObject(ctx, "b", "missing")
		Expect(err).To(MatchError(storage.ErrObjectNotFound))
	})

	It("isolates quarantine to a specific object version", func() {
		oldObj, err := b.PutObject(ctx, "b", "bad", bytes.NewReader([]byte("old")), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		newObj, err := b.PutObject(ctx, "b", "bad", bytes.NewReader([]byte("new")), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		Expect(oldObj.VersionID).NotTo(Equal(newObj.VersionID))

		Expect(b.QuarantineObject(ctx, "b", "bad", oldObj.VersionID, "corrupt_shard", "CRC mismatch")).To(Succeed())

		_, _, err = b.GetObjectVersion("b", "bad", oldObj.VersionID)
		Expect(err).To(MatchError(ErrObjectQuarantined))

		rc, got, err := b.GetObject(ctx, "b", "bad")
		Expect(err).NotTo(HaveOccurred())
		Expect(rc.Close()).To(Succeed())
		Expect(got.VersionID).To(Equal(newObj.VersionID))

		_, err = b.PutObject(ctx, "b", "bad", bytes.NewReader([]byte("newer")), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
	})

	It("records an isolated incident for a local corrupt shard", func() {
		rec := &recordingIncidentRecorder{}
		b.SetIncidentRecorder(rec)

		Expect(b.QuarantineCorruptShardLocal("b", "bad", "v1", 0, "CRC mismatch")).To(Succeed())

		Expect(rec.facts).NotTo(BeEmpty())
		Expect(rec.facts[0].Type).To(Equal(incident.FactObserved))
		Expect(rec.facts[0].Cause).To(Equal(incident.CauseCorruptShard))
		Expect(rec.facts[0].Scope.Kind).To(Equal(incident.ScopeObject))
		Expect(rec.facts[0].Scope.Bucket).To(Equal("b"))
		Expect(rec.facts[0].Scope.Key).To(Equal("bad"))
		Expect(rec.facts[0].Scope.VersionID).To(Equal("v1"))
		Expect(rec.facts[0].Scope.ShardID).To(Equal(0))
		Expect(rec.facts[len(rec.facts)-1].Type).To(Equal(incident.FactIsolated))

		state, err := incident.NewReducer().Reduce(append([]incident.Fact(nil), rec.facts...))
		Expect(err).NotTo(HaveOccurred())
		Expect(state.State).To(Equal(incident.StateIsolated))
		Expect(state.Action).To(Equal(incident.ActionIsolateObject))
		Expect(state.CompletedAt).To(BeTemporally("~", time.Now().UTC(), time.Second))
	})
})
