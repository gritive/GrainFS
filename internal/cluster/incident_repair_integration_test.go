package cluster

import (
	"context"
	"errors"
	"time"

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
	var b *DistributedBackend

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
	})

	It("records failure when the shard service is missing", func() {
		writePlacement(GinkgoT(), b, "b", "k/v1", []string{"test-node", "other-a"})
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
		enc := testEncryptor(GinkgoT())
		svc := NewShardService(dir, nil, WithEncryptor(enc), withTestWALEnc(GinkgoT(), enc))
		nodes := []string{"test-node", "other-a", "other-b", "other-c", "other-d", "other-e"}
		b.SetShardService(svc, nodes)
		seedPlacementMeta(GinkgoT(), b, "b", "k", "v1", nodes, 4, 2)
		Expect(svc.WriteLocalShard("b", "k/v1", 0, []byte("already-repaired"))).To(Succeed())

		rec := &recordingIncidentRecorder{}
		err := b.RepairShardLocalWithIncident(context.Background(), IncidentRepairRequest{
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
