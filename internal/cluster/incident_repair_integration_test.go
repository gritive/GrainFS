package cluster

import (
	"context"
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/incident"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type recordingIncidentRecorder struct{ facts []incident.Fact }

func (r *recordingIncidentRecorder) Record(_ context.Context, facts []incident.Fact) error {
	r.facts = append(r.facts, facts...)
	return nil
}

var _ = Describe("Incident repair integration", func() {
	var (
		b  *DistributedBackend
		db *badger.DB
	)

	BeforeEach(func() {
		b, db = newTestDistributedBackendWithDB(GinkgoT())
	})

	It("records failure when the shard service is missing", func() {
		writePlacement(GinkgoT(), b, db, "b", "k/v1", []string{"test-node", "other-a"})
		rec := &recordingIncidentRecorder{}

		err := b.RepairShardLocalWithIncident(context.Background(), IncidentRepairRequest{
			Bucket: "b", Key: "k", VersionID: "v1", ShardIdx: 0, Recorder: rec, Now: time.Unix(100, 0).UTC(),
		})
		Expect(err).To(HaveOccurred())
		Expect(rec.facts).NotTo(BeEmpty())
		Expect(rec.facts[0].Type).To(Equal(incident.FactObserved))
		Expect(rec.facts[len(rec.facts)-1].Type).To(Equal(incident.FactActionFailed))
	})

	It("records a blocked fact when the context is canceled", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		rec := &recordingIncidentRecorder{}

		err := b.RepairShardLocalWithIncident(ctx, IncidentRepairRequest{
			Bucket: "b", Key: "k", VersionID: "v1", ShardIdx: 0, Recorder: rec, Now: time.Unix(100, 0).UTC(),
		})
		Expect(errors.Is(err, context.Canceled)).To(BeTrue())
		Expect(rec.facts).NotTo(BeEmpty())
		Expect(rec.facts[len(rec.facts)-1].ErrorCode).To(Equal("context_canceled"))
	})

	It("records verified when a concurrent repair already restored the shard", func() {
		dir := GinkgoT().TempDir()
		keeper, clusterID := testDEKKeeper(GinkgoT())
		svc := NewShardService(dir, nil, WithShardDEKKeeper(keeper, clusterID), withTestWALDEK(GinkgoT(), keeper, clusterID))
		nodes := []string{"test-node", "other-a", "other-b", "other-c", "other-d", "other-e"}
		b.SetShardService(svc, nodes)
		// Placement (NodeIDs + VersionID) lives on the latest-only quorum-meta blob —
		// the authority readPlacementMeta resolves for the repair verify. VersionID
		// "v1" makes the resolved shard key "k/v1", matching the on-disk shard below.
		blob, err := encodeQuorumMetaBlob(PutObjectMetaCmd{
			Bucket: "b", Key: "k", VersionID: "v1", Size: 1, ContentType: "application/octet-stream",
			ETag: "etag", ModTime: 1, ECData: 4, ECParity: 2, NodeIDs: nodes,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(svc.writeQuorumMetaLocal("b", "k", blob)).To(Succeed())
		Expect(svc.WriteLocalShard("b", "k/v1", 0, []byte("already-repaired"))).To(Succeed())

		rec := &recordingIncidentRecorder{}
		err = b.RepairShardLocalWithIncident(context.Background(), IncidentRepairRequest{
			Bucket: "b", Key: "k", VersionID: "v1", ShardIdx: 0, Recorder: rec, Now: time.Unix(100, 0).UTC(),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(rec.facts).NotTo(BeEmpty())
		Expect(rec.facts[len(rec.facts)-1].Type).To(Equal(incident.FactVerified))
		Expect(rec.facts[len(rec.facts)-1].Type).NotTo(Equal(incident.FactActionFailed))
	})

	It("records receipt signed only after the persist callback", func() {
		rec := &recordingIncidentRecorder{}
		req := IncidentRepairRequest{
			Bucket: "b", Key: "k", VersionID: "v1", ShardIdx: 0, Recorder: rec, CorrelationID: "cid-repair", Now: time.Unix(100, 0).UTC(),
		}

		Expect(b.RecordRepairReceiptSigned(context.Background(), req, "rcpt-cid-repair")).To(Succeed())
		Expect(rec.facts).NotTo(BeEmpty())
		Expect(rec.facts[0].Type).To(Equal(incident.FactObserved))
		Expect(rec.facts[len(rec.facts)-1].Type).To(Equal(incident.FactReceiptSigned))
		Expect(rec.facts[len(rec.facts)-1].ReceiptID).To(Equal("rcpt-cid-repair"))
	})
})
