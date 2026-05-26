package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/gritive/GrainFS/internal/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type backendReaderOnly struct {
	io.Reader
}

var _ = Describe("Backend object integration", func() {
	var (
		b   *DistributedBackend
		ctx context.Context
	)

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
		ctx = context.Background()
		Expect(b.CreateBucket(ctx, "bucket")).To(Succeed())
	})

	configureParityEC := func() {
		GinkgoHelper()
		b.SetECConfig(ECConfig{DataShards: 2, ParityShards: 1})
		b.SetShardService(NewShardService(b.root, nil), []string{b.selfAddr, b.selfAddr, b.selfAddr})
	}

	It("puts and gets objects", func() {
		obj, err := b.PutObject(ctx, "bucket", "hello.txt", strings.NewReader("hello world"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Size).To(Equal(int64(11)))
		Expect(obj.ContentType).To(Equal("text/plain"))
		Expect(obj.ETag).NotTo(BeEmpty())

		rc, gotObj, err := b.GetObject(ctx, "bucket", "hello.txt")
		Expect(err).NotTo(HaveOccurred())
		data, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal("hello world"))
		Expect(gotObj.ETag).To(Equal(obj.ETag))
		Expect(gotObj.Size).To(Equal(obj.Size))
	})

	It("skips body and EC shard spools for small sized-reader parity EC puts", func() {
		configureParityEC()

		payload := bytes.Repeat([]byte("a"), 64<<10)
		obj, err := b.PutObject(ctx, "bucket", "small.bin", bytes.NewReader(payload), "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Size).To(Equal(int64(len(payload))))

		rc, gotObj, err := b.GetObject(ctx, "bucket", "small.bin")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(payload))
		Expect(gotObj.ETag).To(Equal(obj.ETag))

		_, err = os.Stat(b.spoolDir())
		Expect(errors.Is(err, os.ErrNotExist)).To(BeTrue())
		_, err = os.Stat(b.ecSpoolDir())
		Expect(errors.Is(err, os.ErrNotExist)).To(BeTrue())
	})

	It("uses the body spool but skips EC shard spool for small streaming parity EC puts", func() {
		configureParityEC()

		payload := bytes.Repeat([]byte("a"), 64<<10)
		body := io.LimitReader(bytes.NewReader(payload), int64(len(payload)))
		obj, err := b.PutObject(ctx, "bucket", "small-streaming.bin", body, "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Size).To(Equal(int64(len(payload))))

		rc, gotObj, err := b.GetObject(ctx, "bucket", "small-streaming.bin")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(payload))
		Expect(gotObj.ETag).To(Equal(obj.ETag))

		_, err = os.Stat(b.spoolDir())
		Expect(err).NotTo(HaveOccurred())
		_, err = os.Stat(b.ecSpoolDir())
		Expect(errors.Is(err, os.ErrNotExist)).To(BeTrue())
	})

	It("streams single-local shards with request size hints", func() {
		payload := bytes.Repeat([]byte("x"), 2<<20)
		sizeHint := int64(len(payload))
		obj, err := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
			Bucket:      "bucket",
			Key:         "stream.bin",
			Body:        backendReaderOnly{Reader: bytes.NewReader(payload)},
			SizeHint:    &sizeHint,
			ContentType: "application/octet-stream",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.Size).To(Equal(sizeHint))
		Expect(obj.ETag).To(Equal(backendMD5Hex(payload)))

		rc, gotObj, err := b.GetObject(ctx, "bucket", "stream.bin")
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(gotObj.VersionID).To(Equal(obj.VersionID))
		Expect(got).To(Equal(payload))
	})
})

func backendMD5Hex(data []byte) string {
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:])
}
