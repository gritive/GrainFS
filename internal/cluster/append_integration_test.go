package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Append object integration", func() {
	var (
		b   *DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
		Expect(b.CreateBucket(ctx, "test")).To(Succeed())
	})

	It("applies duplicate append commands idempotently", func() {
		seg := storage.SegmentRef{
			BlobID:   "blob-1",
			Size:     4,
			Checksum: bytes.Repeat([]byte{0xde}, storage.ChecksumLen),
		}
		cmd := AppendObjectCmd{
			Bucket:         "test",
			Key:            "k",
			ExpectedOffset: 0,
			BlobID:         seg.BlobID,
			SegmentSize:    seg.Size,
			SegmentETag:    "deadbeefcafebabedeadbeefcafebabe",
		}
		data, err := encodeAppendObjectCmd(cmd)
		Expect(err).NotTo(HaveOccurred())

		Expect(b.fsm.db.Update(func(txn *badger.Txn) error {
			return b.fsm.applyAppendObjectFromCmd(txn, data)
		})).To(Succeed())

		obj1, err := b.HeadObject(ctx, "test", "k")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj1.Segments).To(HaveLen(1))
		Expect(obj1.Segments[0].BlobID).To(Equal("blob-1"))
		Expect(obj1.Size).To(Equal(seg.Size))
		Expect(obj1.IsAppendable).To(BeTrue())

		Expect(b.fsm.db.Update(func(txn *badger.Txn) error {
			return b.fsm.applyAppendObjectFromCmd(txn, data)
		})).To(Succeed())

		obj2, err := b.HeadObject(ctx, "test", "k")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj2.Segments).To(HaveLen(1))
		Expect(obj2.Size).To(Equal(seg.Size))
	})

	It("serializes concurrent appends through Raft apply", func() {
		_, err := b.AppendObject(ctx, "test", "k", 0, bytes.NewReader([]byte("aaaa")))
		Expect(err).NotTo(HaveOccurred())

		type result struct{ err error }
		ch := make(chan result, 2)
		for i := 0; i < 2; i++ {
			go func() {
				_, err := b.AppendObject(ctx, "test", "k", 4, bytes.NewReader([]byte("bbbb")))
				ch <- result{err}
			}()
		}
		r1, r2 := <-ch, <-ch

		successes, mismatches := countAppendResults(r1.err, r2.err)
		Expect(successes).To(Equal(1))
		Expect(mismatches).To(Equal(1))

		final, err := b.HeadObject(ctx, "test", "k")
		Expect(err).NotTo(HaveOccurred())
		Expect(final.Size).To(Equal(int64(8)))
		Expect(final.Segments).To(HaveLen(2))
	})

	It("serializes same-object appends before segment write", func() {
		_, err := b.AppendObject(ctx, "test", "k", 0, bytes.NewReader([]byte("aaaa")))
		Expect(err).NotTo(HaveOccurred())

		firstEntered := make(chan struct{})
		secondEntered := make(chan struct{})
		releaseFirst := make(chan struct{})
		var entered atomic.Int32
		b.testBeforeAppendSegmentWrite = func() {
			switch entered.Add(1) {
			case 1:
				close(firstEntered)
				<-releaseFirst
			case 2:
				close(secondEntered)
			}
		}

		errCh := make(chan error, 2)
		go func() {
			_, err := b.AppendObject(ctx, "test", "k", 4, bytes.NewReader([]byte("bbbb")))
			errCh <- err
		}()
		<-firstEntered

		go func() {
			_, err := b.AppendObject(ctx, "test", "k", 4, bytes.NewReader([]byte("cccc")))
			errCh <- err
		}()

		select {
		case <-secondEntered:
			close(releaseFirst)
			<-errCh
			<-errCh
			Fail("second same-object append reached segment write before the first append committed")
		case <-time.After(100 * time.Millisecond):
		}

		close(releaseFirst)

		errs := []error{<-errCh, <-errCh}
		successes, mismatches := countAppendResults(errs...)
		Expect(successes).To(Equal(1))
		Expect(mismatches).To(Equal(1))
		Expect(entered.Load()).To(Equal(int32(1)))
	})

	It("reuses version IDs across segments", func() {
		first, err := b.AppendObject(ctx, "test", "k", 0, bytes.NewReader([]byte("aaaa")))
		Expect(err).NotTo(HaveOccurred())
		second, err := b.AppendObject(ctx, "test", "k", 4, bytes.NewReader([]byte("bbbb")))
		Expect(err).NotTo(HaveOccurred())

		Expect(first.VersionID).NotTo(BeEmpty())
		Expect(second.VersionID).To(Equal(first.VersionID))
	})

	It("uses command modified time when applying append commands", func() {
		cmd := AppendObjectCmd{
			Bucket:          "test",
			Key:             "k",
			ExpectedOffset:  0,
			BlobID:          "blob-1",
			SegmentSize:     4,
			SegmentETag:     "deadbeefcafebabedeadbeefcafebabe",
			ModifiedUnixSec: 1234,
		}
		data, err := encodeAppendObjectCmd(cmd)
		Expect(err).NotTo(HaveOccurred())

		Expect(b.fsm.db.Update(func(txn *badger.Txn) error {
			return b.fsm.applyAppendObjectFromCmd(txn, data)
		})).To(Succeed())

		obj, err := b.HeadObject(ctx, "test", "k")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.LastModified).To(Equal(int64(1234)))
	})

	It("converts plain puts at current offset", func() {
		_, err := b.PutObject(ctx, "test", "k", bytes.NewReader([]byte("hello")), "text/plain")
		Expect(err).NotTo(HaveOccurred())

		obj, err := b.AppendObject(ctx, "test", "k", 5, bytes.NewReader([]byte("world")))
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.IsAppendable).To(BeTrue())
		Expect(obj.Size).To(Equal(int64(10)))

		rc, _, err := b.GetObject(ctx, "test", "k")
		Expect(err).NotTo(HaveOccurred())
		got, err := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(err).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(string(got)).To(Equal("helloworld"))
	})

	It("defers follower head consistency to the multi-node harness", func() {
		Skip("Red 14: requires multi-node cluster harness - deferred to Phase 6 e2e (Task 25)")
	})

	It("defers non-owner forwarding to the multi-node harness", func() {
		Skip("Red 15: requires multi-node cluster harness - deferred to Phase 6 e2e (Task 25)")
	})

	It("defers object index sync to the multi-node harness", func() {
		Skip("Red 16: ObjectIndex commit verified via Phase 6 cluster e2e (Task 25)")
	})

	It("defers stale placement retry to placement-rebalance fault injection", func() {
		Skip("Red 17: requires placement-rebalance fault injection - deferred to integration suite")
	})
})

func countAppendResults(errs ...error) (successes, mismatches int) {
	GinkgoHelper()
	for _, err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, storage.ErrAppendOffsetMismatch):
			mismatches++
		default:
			Fail("unexpected append error: " + err.Error())
		}
	}
	return successes, mismatches
}
