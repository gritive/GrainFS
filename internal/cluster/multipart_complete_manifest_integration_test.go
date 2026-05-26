package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"runtime"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var testMultipartMinPartBody = bytes.Repeat([]byte("a"), 5<<20)

var _ = Describe("Multipart complete manifest integration", func() {
	var b *DistributedBackend

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
	})

	createUpload := func() *storage.MultipartUpload {
		Expect(b.CreateBucket(context.Background(), "bucket")).To(Succeed())
		up, err := b.CreateMultipartUpload(context.Background(), "bucket", "mp.bin", "application/octet-stream")
		Expect(err).NotTo(HaveOccurred())
		return up
	}

	uploadPart := func(uploadID string, partNumber int, body []byte) storage.Part {
		part, err := b.UploadPart(context.Background(), "bucket", "mp.bin", uploadID, partNumber, bytes.NewReader(body))
		Expect(err).NotTo(HaveOccurred())
		return *part
	}

	It("rejects duplicate part numbers", func() {
		up := createUpload()
		p := uploadPart(up.UploadID, 1, testMultipartMinPartBody)

		_, err := b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p, p})
		Expect(errors.Is(err, storage.ErrInvalidPart)).To(BeTrue())
	})

	It("rejects an empty request", func() {
		_, err := b.buildMultipartCompleteManifest("upload-id", nil)
		Expect(errors.Is(err, storage.ErrInvalidPart)).To(BeTrue())
	})

	It("rejects invalid part numbers", func() {
		_, err := b.buildMultipartCompleteManifest("upload-id", []storage.Part{{PartNumber: 0, ETag: "etag"}})
		Expect(errors.Is(err, storage.ErrInvalidPart)).To(BeTrue())
	})

	It("rejects part numbers above the S3 limit", func() {
		up := createUpload()
		p := uploadPart(up.UploadID, s3MultipartMaxPartNumber+1, []byte("tail"))

		_, err := b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p})
		Expect(errors.Is(err, storage.ErrInvalidPart)).To(BeTrue())
	})

	It("rejects ETag mismatches", func() {
		up := createUpload()
		p1 := uploadPart(up.UploadID, 1, testMultipartMinPartBody)
		p2 := uploadPart(up.UploadID, 2, []byte("tail"))
		p2.ETag = "not-the-real-etag"

		_, err := b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p1, p2})
		Expect(errors.Is(err, storage.ErrInvalidPart)).To(BeTrue())
	})

	It("rejects missing part files", func() {
		up := createUpload()
		p1 := uploadPart(up.UploadID, 1, testMultipartMinPartBody)
		p2 := uploadPart(up.UploadID, 2, []byte("tail"))
		Expect(os.Remove(b.partPath(up.UploadID, 2))).To(Succeed())

		_, err := b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p1, p2})
		Expect(errors.Is(err, storage.ErrInvalidPart)).To(BeTrue())
	})

	It("rejects unreadable part files when chmod enforcement applies", func() {
		if runtime.GOOS == "windows" {
			Skip("chmod-based unreadable file test is not deterministic on windows")
		}
		up := createUpload()
		p := uploadPart(up.UploadID, 1, testMultipartMinPartBody)
		partPath := b.partPath(up.UploadID, 1)
		Expect(os.Chmod(partPath, 0)).To(Succeed())
		DeferCleanup(func() {
			_ = os.Chmod(partPath, 0o600)
		})

		_, err := b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p})
		if err == nil {
			Skip("chmod-based unreadable file test is not deterministic for this user/platform")
		}
		Expect(errors.Is(err, storage.ErrInvalidPart)).To(BeTrue())
	})

	It("rejects small non-final parts", func() {
		up := createUpload()
		p1 := uploadPart(up.UploadID, 1, []byte("too-small"))
		p2 := uploadPart(up.UploadID, 2, []byte("tail"))

		_, err := b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p1, p2})
		Expect(errors.Is(err, storage.ErrInvalidPart)).To(BeTrue())
	})

	It("returns sorted parts and total size", func() {
		up := createUpload()
		p1 := uploadPart(up.UploadID, 1, testMultipartMinPartBody)
		p2 := uploadPart(up.UploadID, 2, []byte("tail"))

		manifest, err := b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p2, p1})
		Expect(err).NotTo(HaveOccurred())
		Expect(manifest.UploadID).To(Equal(up.UploadID))
		Expect(manifest.TotalSize).To(Equal(int64((5 << 20) + 4)))
		Expect(manifest.Parts).To(Equal([]storage.MultipartPartEntry{
			{PartNumber: 1, Size: int64(5 << 20), ETag: p1.ETag},
			{PartNumber: 2, Size: 4, ETag: p2.ETag},
		}))
	})

	It("streams manifest parts in order", func() {
		up := createUpload()
		p1Body := testMultipartMinPartBody
		p2Body := []byte("tail")
		p1 := uploadPart(up.UploadID, 1, p1Body)
		p2 := uploadPart(up.UploadID, 2, p2Body)

		manifest, err := b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p1, p2})
		Expect(err).NotTo(HaveOccurred())
		rc, err := manifest.Open()
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(len(p1Body) + len(p2Body)))
		Expect(got[:len(p1Body)]).To(Equal(p1Body))
		Expect(got[len(p1Body):]).To(Equal(p2Body))
	})

	It("propagates deleted part errors while reading", func() {
		up := createUpload()
		p1 := uploadPart(up.UploadID, 1, testMultipartMinPartBody)
		p2 := uploadPart(up.UploadID, 2, []byte("tail"))

		manifest, err := b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p1, p2})
		Expect(err).NotTo(HaveOccurred())
		Expect(os.Remove(b.partPath(up.UploadID, 2))).To(Succeed())
		rc, err := manifest.Open()
		Expect(err).NotTo(HaveOccurred())
		_, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).To(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
	})

	It("returns plaintext from encrypted part storage", func() {
		enc := testMultipartEncryptor()
		b.SetShardService(NewShardService(b.root, nil, WithEncryptor(enc)), []string{b.selfAddr})
		up := createUpload()
		p1Body := testMultipartMinPartBody
		p2Body := []byte("tail")
		p1 := uploadPart(up.UploadID, 1, p1Body)
		p2 := uploadPart(up.UploadID, 2, p2Body)

		raw, err := os.ReadFile(b.partPath(up.UploadID, 1))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(raw)).NotTo(ContainSubstring(string(p1Body[:64])))

		manifest, err := b.buildMultipartCompleteManifest(up.UploadID, []storage.Part{p1, p2})
		Expect(err).NotTo(HaveOccurred())
		rc, err := manifest.Open()
		Expect(err).NotTo(HaveOccurred())
		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal(append(append([]byte{}, p1Body...), p2Body...)))
	})
})

var _ = Describe("Multipart complete manifest reader", func() {
	It("opens fresh readers", func() {
		manifest := multipartCompleteManifest{
			Parts: []storage.MultipartPartEntry{
				{PartNumber: 1, Size: 3, ETag: "etag-1"},
				{PartNumber: 2, Size: 3, ETag: "etag-2"},
			},
			openPartFn: func(partNumber int) (io.ReadCloser, error) {
				switch partNumber {
				case 1:
					return io.NopCloser(bytes.NewReader([]byte("one"))), nil
				case 2:
					return io.NopCloser(bytes.NewReader([]byte("two"))), nil
				default:
					Fail("unexpected part number")
					return nil, nil
				}
			},
		}

		first, err := manifest.Open()
		Expect(err).NotTo(HaveOccurred())
		second, err := manifest.Open()
		Expect(err).NotTo(HaveOccurred())
		firstBody, firstReadErr := io.ReadAll(first)
		secondBody, secondReadErr := io.ReadAll(second)
		Expect(firstReadErr).NotTo(HaveOccurred())
		Expect(secondReadErr).NotTo(HaveOccurred())
		Expect(first.Close()).To(Succeed())
		Expect(second.Close()).To(Succeed())
		Expect(firstBody).To(Equal([]byte("onetwo")))
		Expect(secondBody).To(Equal([]byte("onetwo")))
	})

	It("opens parts lazily", func() {
		var opened []int
		manifest := multipartCompleteManifest{
			Parts: []storage.MultipartPartEntry{
				{PartNumber: 1, Size: 2, ETag: "etag-1"},
				{PartNumber: 2, Size: 2, ETag: "etag-2"},
			},
			openPartFn: func(partNumber int) (io.ReadCloser, error) {
				opened = append(opened, partNumber)
				return io.NopCloser(bytes.NewReader([]byte("ab"))), nil
			},
		}

		rc, err := manifest.Open()
		Expect(err).NotTo(HaveOccurred())
		Expect(opened).To(BeEmpty())

		buf := make([]byte, 1)
		n, err := rc.Read(buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(1))
		Expect(opened).To(Equal([]int{1}))
		Expect(rc.Close()).To(Succeed())
	})

	It("closes each part once", func() {
		var parts []*countingReadCloser
		manifest := multipartCompleteManifest{
			Parts: []storage.MultipartPartEntry{
				{PartNumber: 1, Size: 3, ETag: "etag-1"},
				{PartNumber: 2, Size: 3, ETag: "etag-2"},
			},
			openPartFn: func(partNumber int) (io.ReadCloser, error) {
				rc := &countingReadCloser{Reader: bytes.NewReader([]byte{byte('a' + partNumber - 1), byte('a' + partNumber - 1), byte('a' + partNumber - 1)})}
				parts = append(parts, rc)
				return rc, nil
			},
		}
		rc, err := manifest.Open()
		Expect(err).NotTo(HaveOccurred())

		got, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		Expect(readErr).NotTo(HaveOccurred())
		Expect(closeErr).NotTo(HaveOccurred())
		Expect(got).To(Equal([]byte("aaabbb")))
		Expect(parts).To(HaveLen(2))
		Expect(parts[0].closeCount).To(Equal(1))
		Expect(parts[1].closeCount).To(Equal(1))
	})

	It("returns closed pipe when read after close", func() {
		manifest := multipartCompleteManifest{
			Parts: []storage.MultipartPartEntry{{PartNumber: 1, Size: 3, ETag: "etag"}},
			openPartFn: func(partNumber int) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader([]byte("abc"))), nil
			},
		}
		rc, err := manifest.Open()
		Expect(err).NotTo(HaveOccurred())
		Expect(rc.Close()).To(Succeed())

		n, err := rc.Read(make([]byte, 1))
		Expect(n).To(BeZero())
		Expect(errors.Is(err, io.ErrClosedPipe)).To(BeTrue())
	})

	It("propagates read errors", func() {
		readErr := errors.New("read failed")
		manifest := multipartCompleteManifest{
			Parts: []storage.MultipartPartEntry{{PartNumber: 1, Size: 3, ETag: "etag"}},
			openPartFn: func(partNumber int) (io.ReadCloser, error) {
				Expect(partNumber).To(Equal(1))
				return &errorReadCloser{err: readErr}, nil
			},
		}
		rc, err := manifest.Open()
		Expect(err).NotTo(HaveOccurred())

		buf := make([]byte, 8)
		n, err := rc.Read(buf)
		Expect(n).To(BeZero())
		Expect(errors.Is(err, readErr)).To(BeTrue())
		Expect(rc.Close()).To(Succeed())
	})

	It("defers close errors after returning bytes", func() {
		closeErr := errors.New("close failed")
		manifest := multipartCompleteManifest{
			Parts: []storage.MultipartPartEntry{{PartNumber: 1, Size: 3, ETag: "etag"}},
			openPartFn: func(partNumber int) (io.ReadCloser, error) {
				Expect(partNumber).To(Equal(1))
				return &bytesEOFReadCloser{body: []byte("abc"), closeErr: closeErr}, nil
			},
		}
		rc, err := manifest.Open()
		Expect(err).NotTo(HaveOccurred())

		buf := make([]byte, 8)
		n, err := rc.Read(buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(3))
		Expect(string(buf[:n])).To(Equal("abc"))

		n, err = rc.Read(buf)
		Expect(n).To(BeZero())
		Expect(errors.Is(err, closeErr)).To(BeTrue())
		Expect(rc.Close()).To(Succeed())
	})

	It("returns pending close errors from Close", func() {
		closeErr := errors.New("close failed")
		manifest := multipartCompleteManifest{
			Parts: []storage.MultipartPartEntry{{PartNumber: 1, Size: 3, ETag: "etag"}},
			openPartFn: func(partNumber int) (io.ReadCloser, error) {
				Expect(partNumber).To(Equal(1))
				return &bytesEOFReadCloser{body: []byte("abc"), closeErr: closeErr}, nil
			},
		}
		rc, err := manifest.Open()
		Expect(err).NotTo(HaveOccurred())

		buf := make([]byte, 8)
		n, err := rc.Read(buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(3))
		Expect(string(buf[:n])).To(Equal("abc"))

		Expect(errors.Is(rc.Close(), closeErr)).To(BeTrue())
	})

	It("normalizes complete part ETags and wraps invalid part errors", func() {
		sum := md5.Sum([]byte("abc"))
		Expect(trimCompletePartETag(`"` + hex.EncodeToString(sum[:]) + `"`)).To(Equal(hex.EncodeToString(sum[:])))
		Expect(errors.Is(fmtInvalidPart("x"), storage.ErrInvalidPart)).To(BeTrue())
	})
})

func testMultipartEncryptor() *encrypt.Encryptor {
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x45}, 32))
	Expect(err).NotTo(HaveOccurred())
	return enc
}

type bytesEOFReadCloser struct {
	body     []byte
	closeErr error
	read     bool
}

func (r *bytesEOFReadCloser) Read(p []byte) (int, error) {
	if r.read {
		return 0, io.EOF
	}
	r.read = true
	return copy(p, r.body), io.EOF
}

func (r *bytesEOFReadCloser) Close() error {
	return r.closeErr
}

type countingReadCloser struct {
	*bytes.Reader
	closeCount int
}

func (r *countingReadCloser) Close() error {
	r.closeCount++
	return nil
}

type errorReadCloser struct {
	err error
}

func (r *errorReadCloser) Read([]byte) (int, error) {
	return 0, r.err
}

func (r *errorReadCloser) Close() error {
	return nil
}
