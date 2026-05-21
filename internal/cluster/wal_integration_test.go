package cluster

import (
	"context"
	"io"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/storage/wal"
)

var _ = Describe("WAL distributed backend integration", func() {
	var (
		ctx     context.Context
		dist    *DistributedBackend
		walDir  string
		w       *wal.WAL
		backend *wal.Backend
	)

	BeforeEach(func() {
		ctx = context.Background()
		dist = newTestDistributedBackend(GinkgoT())
		walDir = GinkgoT().TempDir()

		var err error
		w, err = wal.Open(walDir)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			if w != nil {
				_ = w.Close()
			}
		})

		backend = wal.NewBackend(dist, w)
	})

	closeWAL := func() {
		Expect(w.Flush()).To(Succeed())
		Expect(w.Close()).To(Succeed())
		w = nil
	}

	It("records Put entries with FSM-assigned version IDs", func() {
		Expect(dist.CreateBucket(ctx, "vbucket")).To(Succeed())

		obj1, err := backend.PutObject(ctx, "vbucket", "k", strings.NewReader("v1"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj1.VersionID).NotTo(BeEmpty(), "DistributedBackend must assign a VersionID")

		obj2, err := backend.PutObject(ctx, "vbucket", "k", strings.NewReader("v2-longer"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj2.VersionID).NotTo(BeEmpty())
		Expect(obj2.VersionID).NotTo(Equal(obj1.VersionID))

		closeWAL()

		var entries []wal.Entry
		n, err := wal.Replay(walDir, 0, time.Now().Add(time.Second), func(e wal.Entry) {
			entries = append(entries, e)
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(2), "both PUTs must produce WAL entries")
		Expect(entries[0].Op).To(Equal(wal.OpPut))
		Expect(entries[0].VersionID).To(Equal(obj1.VersionID), "WAL entry 1 carries FSM-assigned VersionID")
		Expect(entries[1].Op).To(Equal(wal.OpPut))
		Expect(entries[1].VersionID).To(Equal(obj2.VersionID), "WAL entry 2 carries FSM-assigned VersionID")
	})

	It("records hard-version deletes as OpDeleteVersion", func() {
		Expect(dist.CreateBucket(ctx, "vbucket")).To(Succeed())

		obj, err := backend.PutObject(ctx, "vbucket", "k", strings.NewReader("v1"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		Expect(backend.DeleteObjectVersion("vbucket", "k", obj.VersionID)).To(Succeed())

		closeWAL()

		var entries []wal.Entry
		_, err = wal.Replay(walDir, 0, time.Now().Add(time.Second), func(e wal.Entry) {
			entries = append(entries, e)
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(entries).To(HaveLen(2), "Put + DeleteVersion")
		Expect(entries[0].Op).To(Equal(wal.OpPut))
		Expect(entries[1].Op).To(Equal(wal.OpDeleteVersion))
		Expect(entries[1].VersionID).To(Equal(obj.VersionID))
	})

	It("records delete markers with marker version IDs", func() {
		Expect(dist.CreateBucket(ctx, "vbucket")).To(Succeed())

		_, err := backend.PutObject(ctx, "vbucket", "k", strings.NewReader("v1"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		markerID, err := backend.DeleteObjectReturningMarker("vbucket", "k")
		Expect(err).NotTo(HaveOccurred())
		Expect(markerID).NotTo(BeEmpty())

		closeWAL()

		var entries []wal.Entry
		_, err = wal.Replay(walDir, 0, time.Now().Add(time.Second), func(e wal.Entry) {
			entries = append(entries, e)
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(entries).To(HaveLen(2), "Put + delete marker")
		Expect(entries[1].Op).To(Equal(wal.OpDelete))
		Expect(entries[1].VersionID).To(Equal(markerID))
	})

	It("replays to the same latest logical object state", func() {
		Expect(dist.CreateBucket(ctx, "b")).To(Succeed())

		_, err := backend.PutObject(ctx, "b", "a", strings.NewReader("A"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		_, err = backend.PutObject(ctx, "b", "b", strings.NewReader("BB"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		_, err = backend.PutObject(ctx, "b", "a", strings.NewReader("A-v2"), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		closeWAL()

		latest := map[string]wal.Entry{}
		_, err = wal.Replay(walDir, 0, time.Now().Add(time.Second), func(e wal.Entry) {
			switch e.Op {
			case wal.OpPut:
				latest[e.Bucket+"/"+e.Key] = e
			case wal.OpDelete, wal.OpDeleteVersion:
				delete(latest, e.Bucket+"/"+e.Key)
			}
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(latest).To(HaveLen(2))

		rcA, objA, err := dist.GetObject(ctx, "b", "a")
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(rcA.Close)
		_, _ = io.ReadAll(rcA)
		replayed := latest["b/a"]
		Expect(replayed.VersionID).To(Equal(objA.VersionID))
		Expect(replayed.Size).To(Equal(objA.Size))
	})
})
