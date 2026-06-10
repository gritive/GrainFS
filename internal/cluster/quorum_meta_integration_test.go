package cluster

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/dgraph-io/badger/v4"
)

var _ = Describe("Quorum meta — Phase 3 primary path", func() {
	var (
		b   *DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())
	})

	It("PUT writes metadata to quorum store", func() {
		payload := bytes.Repeat([]byte("x"), 1<<10)
		put, err := b.PutObject(ctx, "bucket", "obj.bin",
			bytes.NewReader(payload), "application/octet-stream")
		Expect(err).To(Succeed())
		Expect(put.ETag).NotTo(BeEmpty())

		qmetaPath := filepath.Join(b.root, "shards", quorumMetaSubDir, "bucket", "obj.bin")
		_, statErr := os.Stat(qmetaPath)
		Expect(statErr).To(Succeed(), "quorum meta file must exist: %s", qmetaPath)
	})

	It("PUT does NOT write object meta to BadgerDB (data_raft bypassed)", func() {
		payload := bytes.Repeat([]byte("y"), 512)
		_, err := b.PutObject(ctx, "bucket", "bypassed.bin",
			bytes.NewReader(payload), "application/octet-stream")
		Expect(err).To(Succeed())

		// Object meta key must NOT exist in BadgerDB.
		var found bool
		_ = b.db.View(func(txn *badger.Txn) error {
			k := b.ks().ObjectMetaKey("bucket", "bypassed.bin")
			_, dbErr := txn.Get(k)
			found = dbErr == nil
			return nil
		})
		Expect(found).To(BeFalse(), "data_raft (BadgerDB) must not receive object meta in Phase 3")
	})

	It("GET reads metadata from quorum store", func() {
		payload := bytes.Repeat([]byte("z"), 2<<10)
		put, err := b.PutObject(ctx, "bucket", "read.bin",
			bytes.NewReader(payload), "text/plain")
		Expect(err).To(Succeed())

		obj, err := b.HeadObject(ctx, "bucket", "read.bin")
		Expect(err).To(Succeed())
		Expect(obj.ETag).To(Equal(put.ETag))
		Expect(obj.Size).To(Equal(int64(len(payload))))
		Expect(obj.ContentType).To(Equal("text/plain"))
	})

	It("GET body is readable after quorum-meta PUT", func() {
		payload := bytes.Repeat([]byte("body"), 256)
		_, err := b.PutObject(ctx, "bucket", "getbody.bin",
			bytes.NewReader(payload), "application/octet-stream")
		Expect(err).To(Succeed())

		obj, err := b.HeadObject(ctx, "bucket", "getbody.bin")
		Expect(err).To(Succeed())

		buf := make([]byte, obj.Size)
		n, rerr := b.ReadAt(ctx, "bucket", "getbody.bin", 0, buf)
		Expect(rerr).To(Or(Succeed(), MatchError(io.EOF)))
		Expect(buf[:n]).To(Equal(payload))
	})
})
