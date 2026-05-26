package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type recordingMultipartRaftNode struct {
	RaftNode
	mu    sync.Mutex
	types []CommandType
}

func (n *recordingMultipartRaftNode) ProposeWait(ctx context.Context, command []byte) (uint64, error) {
	if cmd, err := DecodeCommand(command); err == nil {
		n.mu.Lock()
		n.types = append(n.types, cmd.Type)
		n.mu.Unlock()
	}
	return n.RaftNode.ProposeWait(ctx, command)
}

func (n *recordingMultipartRaftNode) commandTypes() []CommandType {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]CommandType, len(n.types))
	copy(out, n.types)
	return out
}

var _ = Describe("Backend multipart integration", func() {
	var (
		b   *DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())
	})

	It("completes multipart uploads without proposing a separate abort", func() {
		up, err := b.CreateMultipartUpload(ctx, "bucket", "mp.bin", "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		part, err := b.UploadPart(ctx, "bucket", "mp.bin", up.UploadID, 1, bytes.NewReader([]byte("small-final-part")))
		Expect(err).NotTo(HaveOccurred())

		rec := &recordingMultipartRaftNode{RaftNode: b.node}
		b.node = rec

		obj, err := b.CompleteMultipartUpload(ctx, "bucket", "mp.bin", up.UploadID, []storage.Part{*part})
		Expect(err).NotTo(HaveOccurred())
		Expect(obj).NotTo(BeNil())
		Expect(rec.commandTypes()).To(Equal([]CommandType{CmdCompleteMultipart}))
	})

	It("bypasses complete spooling for single-part multipart uploads", func() {
		up, err := b.CreateMultipartUpload(ctx, "bucket", "mp.bin", "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		payload := []byte("small-final-part")
		part, err := b.UploadPart(ctx, "bucket", "mp.bin", up.UploadID, 1, bytes.NewReader(payload))
		Expect(err).NotTo(HaveOccurred())

		Expect(os.MkdirAll(filepath.Dir(b.spoolDir()), 0o700)).To(Succeed())
		Expect(os.Mkdir(b.spoolDir(), 0o500)).To(Succeed())
		Expect(os.Chmod(b.spoolDir(), 0o500)).To(Succeed())
		DeferCleanup(func() { _ = os.Chmod(b.spoolDir(), 0o700) })

		obj, err := b.CompleteMultipartUpload(ctx, "bucket", "mp.bin", up.UploadID, []storage.Part{*part})
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Size).To(Equal(int64(len(payload))))

		rc, _, err := b.GetObject(ctx, "bucket", "mp.bin")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(payload))
	})

	It("completes multipart uploads", func() {
		part1 := bytes.Repeat([]byte("A"), 5<<20)
		part2 := bytes.Repeat([]byte("B"), 512)

		upload, err := b.CreateMultipartUpload(ctx, "bucket", "mp.bin", "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		Expect(upload.UploadID).NotTo(BeEmpty())

		p1, err := b.UploadPart(ctx, "bucket", "mp.bin", upload.UploadID, 1, bytes.NewReader(part1))
		Expect(err).NotTo(HaveOccurred())
		Expect(p1.PartNumber).To(Equal(1))
		Expect(p1.Size).To(Equal(int64(5 << 20)))

		p2, err := b.UploadPart(ctx, "bucket", "mp.bin", upload.UploadID, 2, bytes.NewReader(part2))
		Expect(err).NotTo(HaveOccurred())

		obj, err := b.CompleteMultipartUpload(ctx, "bucket", "mp.bin", upload.UploadID, []storage.Part{
			{PartNumber: p1.PartNumber, ETag: p1.ETag, Size: p1.Size},
			{PartNumber: p2.PartNumber, ETag: p2.ETag, Size: p2.Size},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Size).To(Equal(int64(len(part1) + len(part2))))

		rc, _, err := b.GetObject(ctx, "bucket", "mp.bin")
		Expect(err).NotTo(HaveOccurred())
		data, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(data).To(HaveLen(len(part1) + len(part2)))
		Expect(data[:len(part1)]).To(Equal(part1))
		Expect(data[len(part1):]).To(Equal(part2))
	})

	It("stores segments and parts for chunked multipart completes", func() {
		configureChunkedMultipartTestBackend(b)

		part1 := bytes.Repeat([]byte("A"), testChunkedMultipartChunkSize)
		part2 := bytes.Repeat([]byte("B"), testChunkedMultipartChunkSize)
		part3 := []byte("C")
		up, err := b.CreateMultipartUpload(ctx, "bucket", "large-mp.bin", "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		p1, err := b.UploadPart(ctx, "bucket", "large-mp.bin", up.UploadID, 1, bytes.NewReader(part1))
		Expect(err).NotTo(HaveOccurred())
		p2, err := b.UploadPart(ctx, "bucket", "large-mp.bin", up.UploadID, 2, bytes.NewReader(part2))
		Expect(err).NotTo(HaveOccurred())
		p3, err := b.UploadPart(ctx, "bucket", "large-mp.bin", up.UploadID, 3, bytes.NewReader(part3))
		Expect(err).NotTo(HaveOccurred())

		obj, err := b.CompleteMultipartUpload(ctx, "bucket", "large-mp.bin", up.UploadID, []storage.Part{*p1, *p2, *p3})
		Expect(err).NotTo(HaveOccurred())
		Expect(len(obj.Segments)).To(BeNumerically(">=", 2))
		Expect(obj.Parts).To(HaveLen(3))

		head, err := b.HeadObject(ctx, "bucket", "large-mp.bin")
		Expect(err).NotTo(HaveOccurred())
		Expect(len(head.Segments)).To(BeNumerically(">=", 2))
		Expect(head.Parts).To(HaveLen(3))
		Expect(head.Size).To(Equal(int64(len(part1) + len(part2) + len(part3))))

		rc, _, err := b.GetObject(ctx, "bucket", "large-mp.bin")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		want := append(append(append([]byte{}, part1...), part2...), part3...)
		Expect(got).To(Equal(want))
	})

	It("preserves chunked multipart uploads when complete fails", func() {
		configureChunkedMultipartTestBackend(b)

		up, err := b.CreateMultipartUpload(ctx, "bucket", "large-mp.bin", "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		p1, err := b.UploadPart(ctx, "bucket", "large-mp.bin", up.UploadID, 1, bytes.NewReader(bytes.Repeat([]byte("A"), testChunkedMultipartChunkSize)))
		Expect(err).NotTo(HaveOccurred())
		p2, err := b.UploadPart(ctx, "bucket", "large-mp.bin", up.UploadID, 2, bytes.NewReader([]byte("B")))
		Expect(err).NotTo(HaveOccurred())

		b.testBeforeChunkedMultipartCommit = func() error { return errors.New("injected commit preflight failure") }
		_, err = b.CompleteMultipartUpload(ctx, "bucket", "large-mp.bin", up.UploadID, []storage.Part{*p1, *p2})
		Expect(err).To(MatchError(ContainSubstring("injected commit preflight failure")))

		listed, err := b.ListParts(ctx, "bucket", "large-mp.bin", up.UploadID, 100)
		Expect(err).NotTo(HaveOccurred())
		Expect(listed).To(HaveLen(2))
		_, err = b.HeadObject(ctx, "bucket", "large-mp.bin")
		Expect(err).To(MatchError(storage.ErrObjectNotFound))
	})

	It("encrypts multipart part storage", func() {
		enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x45}, 32))
		Expect(err).NotTo(HaveOccurred())
		b.SetShardService(NewShardService(b.root, nil, WithEncryptor(enc), withTestWALEnc(GinkgoT(), enc)), []string{b.selfAddr})

		partBytes := []byte("cluster multipart sensitive payload")
		upload, err := b.CreateMultipartUpload(ctx, "bucket", "mp.bin", "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())

		part, err := b.UploadPart(ctx, "bucket", "mp.bin", upload.UploadID, 1, bytes.NewReader(partBytes))
		Expect(err).NotTo(HaveOccurred())
		Expect(part.Size).To(Equal(int64(len(partBytes))))

		rawPart, err := os.ReadFile(b.partPath(upload.UploadID, 1))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(rawPart)).NotTo(ContainSubstring(string(partBytes)))

		listed, err := b.ListParts(ctx, "bucket", "mp.bin", upload.UploadID, 100)
		Expect(err).NotTo(HaveOccurred())
		Expect(listed).To(HaveLen(1))
		Expect(listed[0].ETag).To(Equal(part.ETag))

		obj, err := b.CompleteMultipartUpload(ctx, "bucket", "mp.bin", upload.UploadID, []storage.Part{*part})
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Size).To(Equal(int64(len(partBytes))))

		rc, _, err := b.GetObject(ctx, "bucket", "mp.bin")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(partBytes))
	})

	It("aborts multipart uploads", func() {
		upload, err := b.CreateMultipartUpload(ctx, "bucket", "abort.bin", "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())

		_, err = b.UploadPart(ctx, "bucket", "abort.bin", upload.UploadID, 1, strings.NewReader("data"))
		Expect(err).NotTo(HaveOccurred())

		Expect(b.AbortMultipartUpload(ctx, "bucket", "abort.bin", upload.UploadID)).To(Succeed())

		_, err = b.HeadObject(ctx, "bucket", "abort.bin")
		Expect(err).To(MatchError(storage.ErrObjectNotFound))
	})

	It("recreates missing active upload part directories", func() {
		upload, err := b.CreateMultipartUpload(ctx, "bucket", "mp.bin", "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		Expect(os.RemoveAll(b.partDir(upload.UploadID))).To(Succeed())

		part, err := b.UploadPart(ctx, "bucket", "mp.bin", upload.UploadID, 1, strings.NewReader("data"))
		Expect(err).NotTo(HaveOccurred())
		Expect(part.PartNumber).To(Equal(1))
	})

	It("returns upload-not-found for bad multipart upload IDs", func() {
		_, err := b.UploadPart(ctx, "bucket", "file.bin", "bad-id", 1, strings.NewReader("data"))
		Expect(err).To(MatchError(storage.ErrUploadNotFound))

		err = b.AbortMultipartUpload(ctx, "bucket", "file.bin", "bad-id")
		Expect(err).To(MatchError(storage.ErrUploadNotFound))

		_, err = b.CompleteMultipartUpload(ctx, "bucket", "file.bin", "bad-id", nil)
		Expect(err).To(MatchError(storage.ErrUploadNotFound))
	})

	It("returns bucket-not-found for missing multipart buckets", func() {
		_, err := b.CreateMultipartUpload(ctx, "nope", "file.bin", "application/octet-stream")
		Expect(err).To(MatchError(storage.ErrBucketNotFound))
	})

	It("filters, skips legacy rows, limits, and sorts multipart upload listings", func() {
		Expect(b.CreateBucket(ctx, "other")).To(Succeed())

		writeMultipartMetaSpec(b, "upload-late", clusterMultipartMeta{
			Bucket: "bucket", Key: "prefix/z.bin", CreatedAt: 300, ContentType: "application/octet-stream", PlacementGroupID: "group-1",
		})
		writeMultipartMetaSpec(b, "upload-early-b", clusterMultipartMeta{
			Bucket: "bucket", Key: "prefix/b.bin", CreatedAt: 100, ContentType: "application/octet-stream", PlacementGroupID: "group-1",
		})
		writeMultipartMetaSpec(b, "upload-early-a", clusterMultipartMeta{
			Bucket: "bucket", Key: "prefix/a.bin", CreatedAt: 100, ContentType: "application/octet-stream", PlacementGroupID: "group-1",
		})
		writeMultipartMetaSpec(b, "upload-other-prefix", clusterMultipartMeta{
			Bucket: "bucket", Key: "else/a.bin", CreatedAt: 50, ContentType: "application/octet-stream", PlacementGroupID: "group-1",
		})
		writeMultipartMetaSpec(b, "upload-other-bucket", clusterMultipartMeta{
			Bucket: "other", Key: "prefix/a.bin", CreatedAt: 25, ContentType: "application/octet-stream", PlacementGroupID: "group-1",
		})
		writeMultipartMetaSpec(b, "upload-legacy", clusterMultipartMeta{
			ContentType: "application/octet-stream", PlacementGroupID: "group-1",
		})

		out, err := b.ListMultipartUploads(ctx, "bucket", "prefix/", 2)
		Expect(err).NotTo(HaveOccurred())
		Expect(out).To(HaveLen(2))
		Expect(out[0].UploadID).To(Equal("upload-early-a"))
		Expect(out[0].Key).To(Equal("prefix/a.bin"))
		Expect(out[0].CreatedAt).To(Equal(int64(100)))
		Expect(out[1].UploadID).To(Equal("upload-early-b"))
		Expect(out[1].Key).To(Equal("prefix/b.bin"))

		all, err := b.ListMultipartUploads(ctx, "bucket", "prefix/", 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(multipartUploadIDs(all)).To(Equal([]string{"upload-early-a", "upload-early-b", "upload-late"}))
	})
})

func configureChunkedMultipartTestBackend(b *DistributedBackend) {
	GinkgoHelper()
	nodes := []string{b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr, b.selfAddr}
	b.SetShardService(NewShardService(b.root, nil, withTestWAL(GinkgoT())), nodes)
	b.SetECConfig(ECConfig{DataShards: 4, ParityShards: 2})
	b.chunkedPutChunkSize = testChunkedMultipartChunkSize
	b.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-0": {ID: "group-0", PeerIDs: nodes},
	}})
}

const testChunkedMultipartChunkSize = 5 << 20

func writeMultipartMetaSpec(b *DistributedBackend, uploadID string, meta clusterMultipartMeta) {
	GinkgoHelper()
	raw, err := marshalClusterMultipartMeta(meta)
	Expect(err).NotTo(HaveOccurred())
	Expect(b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(b.ks().MultipartKey(uploadID), raw)
	})).To(Succeed())
}

func multipartUploadIDs(uploads []*storage.MultipartUpload) []string {
	out := make([]string, len(uploads))
	for i, upload := range uploads {
		out[i] = upload.UploadID
	}
	return out
}
