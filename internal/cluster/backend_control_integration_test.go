package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Backend control integration", func() {
	var (
		b   *DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
	})

	It("reports unavailable topology write targets", func() {
		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())
		b.SetECConfig(ECConfig{DataShards: 2, ParityShards: 1})
		enc := testEncryptor(GinkgoT())
		b.SetShardService(NewShardService(GinkgoT().TempDir(), nil, WithEncryptor(enc), withTestWALEnc(GinkgoT(), enc)), []string{"n1", "n2", "n3"})

		group := ShardGroupEntry{ID: "group-1", PeerIDs: []string{"n1", "n2", "n3"}}
		baseCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer cancel()
		writeCtx := ContextWithPlacementGroupEntry(baseCtx, group)

		_, err := b.PutObject(writeCtx, "bucket", "key.txt", strings.NewReader("hello"), "text/plain")
		Expect(errors.Is(err, ErrPlacementTargetsUnavailable)).To(BeTrue())
	})

	It("rejects unhealthy topology write targets before shard writes", func() {
		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())
		b.SetECConfig(ECConfig{DataShards: 2, ParityShards: 1})
		enc := testEncryptor(GinkgoT())
		b.SetShardService(NewShardService(GinkgoT().TempDir(), nil, WithEncryptor(enc), withTestWALEnc(GinkgoT(), enc)), []string{"n1", "n2", "n3"})
		b.peerHealth.MarkUnhealthy("n2")

		group := ShardGroupEntry{ID: "group-1", PeerIDs: []string{"n1", "n2", "n3"}}
		writeCtx := ContextWithPlacementGroupEntry(ctx, group)

		_, err := b.PutObject(writeCtx, "bucket", "key.txt", strings.NewReader("hello"), "text/plain")
		Expect(errors.Is(err, ErrPlacementTargetsUnavailable)).To(BeTrue())
		Expect(err).To(MatchError(ContainSubstring("known unhealthy placement target")))
	})

	It("allows cluster node updates while readers inspect derived state", func() {
		b.SetECConfig(ECConfig{DataShards: 3, ParityShards: 2})
		enc := testEncryptor(GinkgoT())
		b.SetShardService(NewShardService(GinkgoT().TempDir(), nil, WithEncryptor(enc), withTestWALEnc(GinkgoT(), enc)), []string{"n1", "n2", "n3"})

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				b.SetClusterNodes([]string{"n1", "n2", "n3", "n4", fmt.Sprintf("n%d", i+5)})
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = b.LiveNodes()
				_ = b.ECActive()
				_ = b.EffectiveECConfig()
				_ = b.NodeID()
				if ph := b.PeerHealth(); ph != nil {
					_ = ph.Snapshot()
				}
			}
		}()
		wg.Wait()
	})

	It("waits for backend apply progress", func() {
		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())
		applied := b.lastApplied.Load()
		Expect(applied).NotTo(BeZero())
		b.lastApplied.Store(0)

		waitCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()
		err := b.WaitApplied(waitCtx, applied)
		Expect(errors.Is(err, context.DeadlineExceeded)).To(BeTrue())
	})

	Describe("EC object reader bounded-load injection", func() {
		var fakeBL *BoundedLoads

		BeforeEach(func() {
			store := NewNodeStatsStore(time.Minute)
			params := BoundedLoadsParams{C: 1.25, CLow: 1.0}
			fakeBL = NewBoundedLoads(store, params)
		})

		It("injects bounded loads by default", func() {
			b.bl = fakeBL
			r := b.newECObjectReader()
			Expect(r.bl).NotTo(BeNil())
		})

		It("skips bounded loads when the cluster config disables it", func() {
			b.bl = fakeBL
			disabled := false
			b.clusterCfg.applyPatch(ClusterConfigPatch{BoundedLoadsEnabled: &disabled}, time.Now())
			r := b.newECObjectReader()
			Expect(r.bl).To(BeNil())
		})
	})
})
