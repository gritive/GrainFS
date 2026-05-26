package cluster

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/eccodec"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ scrubber.Scrubbable = (*DistributedBackend)(nil)
var _ scrubber.ShardIntegrityReader = (*DistributedBackend)(nil)

var _ = Describe("Scrubbable integration", func() {
	var b *DistributedBackend

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
	})

	Describe("object scanning", func() {
		BeforeEach(func() {
			enableECForSpec(b, 2, 1)
		})

		It("emits no records for an empty bucket", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())

			ch, err := b.ScanObjects("bkt")
			Expect(err).NotTo(HaveOccurred())
			Expect(scrubDrainObjectRecords(ch)).To(BeEmpty())
		})

		It("rejects a missing bucket", func() {
			_, err := b.ScanObjects("no-such-bucket")
			Expect(err).To(HaveOccurred())
		})

		It("emits a single object record with EC configuration", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			writeScrubVersionedObjectMeta(b, "bkt", "hello.txt", "01HZXYZABC", "etag-hello", 11, nil)

			ch, err := b.ScanObjects("bkt")
			Expect(err).NotTo(HaveOccurred())
			recs := scrubDrainObjectRecords(ch)

			Expect(recs).To(HaveLen(1))
			Expect(recs[0].Bucket).To(Equal("bkt"))
			Expect(recs[0].Key).To(Equal("hello.txt"))
			Expect(recs[0].VersionID).To(Equal("01HZXYZABC"))
			Expect(recs[0].DataShards).To(Equal(2))
			Expect(recs[0].ParityShards).To(Equal(1))
			Expect(recs[0].ETag).To(Equal("etag-hello"))
		})

		It("preserves slash-containing keys while scanning", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			writeScrubVersionedObjectMeta(b, "bkt", "folder/nested/file.bin", "01A", "etag-a", 100, nil)
			writeScrubVersionedObjectMeta(b, "bkt", "top.txt", "01B", "etag-b", 7, nil)

			ch, err := b.ScanObjects("bkt")
			Expect(err).NotTo(HaveOccurred())
			recs := scrubDrainObjectRecords(ch)

			Expect(recs).To(HaveLen(2))
			keys := map[string]bool{}
			for _, r := range recs {
				keys[r.Key] = true
			}
			Expect(keys).To(HaveKey("folder/nested/file.bin"))
			Expect(keys).To(HaveKey("top.txt"))
		})

		It("skips tombstones while scanning", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			writeScrubVersionedObjectMeta(b, "bkt", "alive", "01A", "etag-alive", 5, nil)
			writeScrubVersionedObjectMeta(b, "bkt", "dead", "01D", deleteMarkerETag, 0, nil)

			ch, err := b.ScanObjects("bkt")
			Expect(err).NotTo(HaveOccurred())
			recs := scrubDrainObjectRecords(ch)

			Expect(recs).To(HaveLen(1))
			Expect(recs[0].Key).To(Equal("alive"))
		})
	})

	Describe("object existence", func() {
		It("reports only live objects as existing", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			writeScrubVersionedObjectMeta(b, "bkt", "present", "01A", "etag", 10, nil)
			writeScrubVersionedObjectMeta(b, "bkt", "tomb", "01B", deleteMarkerETag, 0, nil)

			ok, err := b.ObjectExists("bkt", "present")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())

			ok, err = b.ObjectExists("bkt", "missing")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())

			ok, err = b.ObjectExists("bkt", "tomb")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())

			ok, err = b.ObjectExists("no-such-bucket", "k")
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse())
		})
	})

	Describe("shard paths and integrity", func() {
		It("returns versioned shard paths", func() {
			paths := b.ShardPaths("bkt", "objects/big.bin", "01HZXYZ", 6)

			Expect(paths).To(HaveLen(6))
			for i, p := range paths {
				Expect(p).To(Equal(filepath.Join(b.root, "shards", "bkt", "objects/big.bin", "01HZXYZ", fmt.Sprintf("shard_%d", i))))
			}
		})

		It("matches the shard service layout", func() {
			svc := NewShardService(b.root, nil)
			Expect(svc.WriteLocalShard("bkt", "key/01VID", 3, []byte("payload"))).To(Succeed())

			paths := b.ShardPaths("bkt", "key", "01VID", 4)
			data, err := os.ReadFile(paths[3])
			Expect(err).NotTo(HaveOccurred())
			decoded, err := eccodec.DecodeShard(data)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(decoded)).To(Equal("payload"))
		})

		It("uses the shared shard service root", func() {
			shardRoot := GinkgoT().TempDir()
			svc := NewShardService(shardRoot, nil)
			b.SetShardService(svc, []string{"test-node"})
			Expect(svc.WriteLocalShard("bkt", "key/01VID", 0, []byte("payload"))).To(Succeed())

			paths := b.ShardPaths("bkt", "key", "01VID", 1)
			data, err := os.ReadFile(paths[0])
			Expect(err).NotTo(HaveOccurred())
			decoded, err := eccodec.DecodeShard(data)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(decoded)).To(Equal("payload"))
		})

		It("round-trips shard writes and reads", func() {
			path := filepath.Join(GinkgoT().TempDir(), "nested", "shard_0")
			payload := []byte("ec-shard-payload-0x42")

			Expect(b.WriteShard("bkt", "k", path, payload)).To(Succeed())
			got, err := b.ReadShard("bkt", "k", path)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(payload))
		})

		It("verifies encoded shard integrity", func() {
			path := filepath.Join(GinkgoT().TempDir(), "shard_0")
			payload := []byte("encoded-payload")
			Expect(b.WriteShard("bkt", "k", path, payload)).To(Succeed())

			got, err := b.ReadShardIntegrity("bkt", "k", path)
			Expect(err).NotTo(HaveOccurred())
			Expect(got.Status).To(Equal(scrubber.ShardIntegrityVerified))
			Expect(got.Payload).To(Equal(payload))
		})

		It("verifies encrypted shard-service shards", func() {
			enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{7}, 32))
			Expect(err).NotTo(HaveOccurred())
			svc := NewShardService(GinkgoT().TempDir(), nil, WithEncryptor(enc))
			b.SetShardService(svc, []string{"test-node"})
			Expect(svc.WriteLocalShard("bkt", "key/01VID", 0, []byte("payload"))).To(Succeed())

			path := b.ShardPaths("bkt", "key", "01VID", 1)[0]
			got, err := b.ReadShardIntegrity("bkt", "key", path)
			Expect(err).NotTo(HaveOccurred())
			Expect(got.Status).To(Equal(scrubber.ShardIntegrityVerified))
			Expect(string(got.Payload)).To(Equal("payload"))
		})

		It("verifies packed shards through shared shard service", func() {
			enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{8}, 32))
			Expect(err).NotTo(HaveOccurred())
			svc := NewShardService(GinkgoT().TempDir(), nil, WithEncryptor(enc), WithShardPackThreshold(1024))
			b.SetShardService(svc, []string{"test-node"})
			Expect(svc.WriteLocalShard("bkt", "key/01VID", 0, []byte("packed-payload"))).To(Succeed())

			path := b.ShardPaths("bkt", "key", "01VID", 1)[0]
			_, err = os.Stat(path)
			Expect(os.IsNotExist(err)).To(BeTrue())

			got, err := b.ReadShardIntegrity("bkt", "key", path)
			Expect(err).NotTo(HaveOccurred())
			Expect(got.Status).To(Equal(scrubber.ShardIntegrityVerified))
			Expect(string(got.Payload)).To(Equal("packed-payload"))
		})

		It("keeps legacy raw shards readable but unverified", func() {
			path := filepath.Join(GinkgoT().TempDir(), "shard_0")
			payload := []byte("legacy-raw-payload")
			Expect(os.WriteFile(path, payload, 0o600)).To(Succeed())

			got, err := b.ReadShardIntegrity("bkt", "k", path)
			Expect(err).NotTo(HaveOccurred())
			Expect(got.Status).To(Equal(scrubber.ShardIntegrityUnverifiedLegacy))
			Expect(got.Payload).To(Equal(payload))

			compat, err := b.ReadShard("bkt", "k", path)
			Expect(err).NotTo(HaveOccurred())
			Expect(compat).To(Equal(payload))
		})

		It("overwrites shards atomically without leftover temp files", func() {
			dir := GinkgoT().TempDir()
			path := filepath.Join(dir, "shard_0")

			Expect(b.WriteShard("bkt", "k", path, []byte("v1"))).To(Succeed())
			Expect(b.WriteShard("bkt", "k", path, []byte("v2"))).To(Succeed())

			got, err := b.ReadShard("bkt", "k", path)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(got)).To(Equal("v2"))

			entries, err := os.ReadDir(dir)
			Expect(err).NotTo(HaveOccurred())
			for _, entry := range entries {
				Expect(strings.HasSuffix(entry.Name(), ".tmp")).To(BeFalse(), "leftover tmp: %s", entry.Name())
			}
		})

		It("errors for missing shard files", func() {
			_, err := b.ReadShard("bkt", "k", filepath.Join(GinkgoT().TempDir(), "nope"))
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("grouped object scanning", func() {
		It("emits no groups for an empty bucket", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())

			ch, err := b.ScanObjectsGrouped("bkt")
			Expect(err).NotTo(HaveOccurred())
			Expect(scrubDrainObjectKeyGroups(ch)).To(BeEmpty())
		})

		It("rejects a missing bucket", func() {
			_, err := b.ScanObjectsGrouped("no-such-bucket")
			Expect(err).To(HaveOccurred())
		})

		It("groups versions by key in key order", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			writeScrubVersionedObjectMeta(b, "bkt", "a", "01AAAA0001", "etag-a-v1", 11, nil)
			writeScrubVersionedObjectMeta(b, "bkt", "a", "01AAAA0002", "etag-a-v2", 22, nil)
			writeScrubVersionedObjectMeta(b, "bkt", "b", "01BBBB0001", "etag-b-v1", 33, nil)

			ch, err := b.ScanObjectsGrouped("bkt")
			Expect(err).NotTo(HaveOccurred())
			groups := scrubDrainObjectKeyGroups(ch)

			Expect(groups).To(HaveLen(2))
			Expect(groups[0].Bucket).To(Equal("bkt"))
			Expect(groups[0].Key).To(Equal("a"))
			Expect(groups[0].Versions).To(HaveLen(2))
			Expect(groups[0].Versions[0].VersionID).To(Equal("01AAAA0002"))
			Expect(groups[0].Versions[0].ETag).To(Equal("etag-a-v2"))
			Expect(groups[0].Versions[0].IsLatest).To(BeTrue())
			Expect(groups[0].Versions[1].VersionID).To(Equal("01AAAA0001"))
			Expect(groups[0].Versions[1].IsLatest).To(BeFalse())

			Expect(groups[1].Key).To(Equal("b"))
			Expect(groups[1].Versions).To(HaveLen(1))
			Expect(groups[1].Versions[0].VersionID).To(Equal("01BBBB0001"))
		})

		It("passes delete markers through grouped scans", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			writeScrubVersionedObjectMeta(b, "bkt", "k", "01AAAA0001", "etag-live", 10, nil)
			writeScrubVersionedObjectMeta(b, "bkt", "k", "01AAAA0002", deleteMarkerETag, 0, nil)

			ch, err := b.ScanObjectsGrouped("bkt")
			Expect(err).NotTo(HaveOccurred())
			groups := scrubDrainObjectKeyGroups(ch)

			Expect(groups).To(HaveLen(1))
			Expect(groups[0].Versions).To(HaveLen(2))
			Expect(groups[0].Versions[0].IsDeleteMarker).To(BeTrue())
			Expect(groups[0].Versions[1].IsDeleteMarker).To(BeFalse())
		})

		It("preserves tags in grouped scans", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			tags := []storage.Tag{{Key: "env", Value: "prod"}, {Key: "team", Value: "data"}}
			writeScrubVersionedObjectMeta(b, "bkt", "tagged", "01AAAA0001", "etag", 5, tags)

			ch, err := b.ScanObjectsGrouped("bkt")
			Expect(err).NotTo(HaveOccurred())
			groups := scrubDrainObjectKeyGroups(ch)

			Expect(groups).To(HaveLen(1))
			Expect(groups[0].Versions).To(HaveLen(1))
			Expect(groups[0].Versions[0].Tags).To(Equal(tags))
		})

		It("includes legacy unversioned objects", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			tags := []storage.Tag{{Key: "expire", Value: "yes"}}
			writeScrubLegacyObjectMeta(b, "bkt", "folder/tagged", "etag", 42, tags)

			ch, err := b.ScanObjectsGrouped("bkt")
			Expect(err).NotTo(HaveOccurred())
			groups := scrubDrainObjectKeyGroups(ch)

			Expect(groups).To(HaveLen(1))
			Expect(groups[0].Key).To(Equal("folder/tagged"))
			Expect(groups[0].Versions).To(HaveLen(1))
			Expect(groups[0].Versions[0].VersionID).To(BeEmpty())
			Expect(groups[0].Versions[0].IsLatest).To(BeTrue())
			Expect(groups[0].Versions[0].Size).To(Equal(int64(42)))
			Expect(groups[0].Versions[0].Tags).To(Equal(tags))
		})
	})

	Describe("local multipart upload scanning", func() {
		It("emits no records for an empty bucket", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())

			ch, err := b.ScanLocalMultipartUploads("bkt")
			Expect(err).NotTo(HaveOccurred())
			Expect(scrubDrainMultipartRecords(ch)).To(BeEmpty())
		})

		It("rejects a missing bucket", func() {
			_, err := b.ScanLocalMultipartUploads("no-such-bucket")
			Expect(err).To(HaveOccurred())
		})

		It("emits active uploads for the requested bucket", func() {
			Expect(b.CreateBucket(context.Background(), "bkt")).To(Succeed())
			Expect(b.CreateBucket(context.Background(), "other")).To(Succeed())

			writeScrubMultipartMeta(b, "upload-aaa", clusterMultipartMeta{
				Bucket: "bkt", Key: "a.bin", ContentType: "application/octet-stream", CreatedAt: 100,
			})
			writeScrubMultipartMeta(b, "upload-bbb", clusterMultipartMeta{
				Bucket: "bkt", Key: "b.bin", ContentType: "application/octet-stream", CreatedAt: 200,
			})
			writeScrubMultipartMeta(b, "upload-other", clusterMultipartMeta{
				Bucket: "other", Key: "x.bin", ContentType: "application/octet-stream", CreatedAt: 300,
			})

			ch, err := b.ScanLocalMultipartUploads("bkt")
			Expect(err).NotTo(HaveOccurred())
			recs := scrubDrainMultipartRecords(ch)

			Expect(recs).To(HaveLen(2))
			got := map[string]storage.MultipartUploadRecord{}
			for _, r := range recs {
				got[r.UploadID] = r
			}
			Expect(got).To(HaveKey("upload-aaa"))
			Expect(got["upload-aaa"].Bucket).To(Equal("bkt"))
			Expect(got["upload-aaa"].Key).To(Equal("a.bin"))
			Expect(got["upload-aaa"].InitiatedAt).To(Equal(int64(100)))
			Expect(got).To(HaveKey("upload-bbb"))
			Expect(got["upload-bbb"].Key).To(Equal("b.bin"))
			Expect(got["upload-bbb"].InitiatedAt).To(Equal(int64(200)))
		})
	})
})

func writeScrubVersionedObjectMeta(b *DistributedBackend, bucket, key, versionID, etag string, size int64, tags []storage.Tag) {
	GinkgoHelper()
	meta, err := marshalObjectMeta(objectMeta{
		Key:          key,
		Size:         size,
		ContentType:  "application/octet-stream",
		ETag:         etag,
		LastModified: time.Now().Unix(),
		Tags:         tags,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(b.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(objectMetaKey(bucket, key), meta); err != nil {
			return err
		}
		if err := txn.Set(objectMetaKeyV(bucket, key, versionID), meta); err != nil {
			return err
		}
		return txn.Set(latestKey(bucket, key), []byte(versionID))
	})).To(Succeed())
}

func writeScrubLegacyObjectMeta(b *DistributedBackend, bucket, key, etag string, size int64, tags []storage.Tag) {
	GinkgoHelper()
	meta, err := marshalObjectMeta(objectMeta{
		Key:          key,
		Size:         size,
		ContentType:  "application/octet-stream",
		ETag:         etag,
		LastModified: time.Now().Unix(),
		Tags:         tags,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(objectMetaKey(bucket, key), meta)
	})).To(Succeed())
}

func writeScrubMultipartMeta(b *DistributedBackend, uploadID string, meta clusterMultipartMeta) {
	GinkgoHelper()
	raw, err := marshalClusterMultipartMeta(meta)
	Expect(err).NotTo(HaveOccurred())
	Expect(b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(b.ks().MultipartKey(uploadID), raw)
	})).To(Succeed())
}

func scrubDrainObjectRecords(ch <-chan scrubber.ObjectRecord) []scrubber.ObjectRecord {
	GinkgoHelper()
	var out []scrubber.ObjectRecord
	timeout := time.After(2 * time.Second)
	for {
		select {
		case rec, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, rec)
		case <-timeout:
			Fail("ObjectRecord channel did not close within timeout")
		}
	}
}

func scrubDrainObjectKeyGroups(ch <-chan storage.ObjectKeyGroup) []storage.ObjectKeyGroup {
	GinkgoHelper()
	var out []storage.ObjectKeyGroup
	timeout := time.After(2 * time.Second)
	for {
		select {
		case group, ok := <-ch:
			if !ok {
				return out
			}
			versions := append([]storage.ObjectVersionRecord(nil), group.Versions...)
			out = append(out, storage.ObjectKeyGroup{Bucket: group.Bucket, Key: group.Key, Versions: versions})
		case <-timeout:
			Fail("ObjectKeyGroup channel did not close within timeout")
		}
	}
}

func drainObjectKeyGroups(t clusterTestTB, ch <-chan storage.ObjectKeyGroup) []storage.ObjectKeyGroup {
	t.Helper()
	var out []storage.ObjectKeyGroup
	timeout := time.After(2 * time.Second)
	for {
		select {
		case group, ok := <-ch:
			if !ok {
				return out
			}
			versions := append([]storage.ObjectVersionRecord(nil), group.Versions...)
			out = append(out, storage.ObjectKeyGroup{Bucket: group.Bucket, Key: group.Key, Versions: versions})
		case <-timeout:
			t.Fatalf("ObjectKeyGroup channel did not close within timeout")
		}
	}
}

func scrubDrainMultipartRecords(ch <-chan storage.MultipartUploadRecord) []storage.MultipartUploadRecord {
	GinkgoHelper()
	var out []storage.MultipartUploadRecord
	timeout := time.After(2 * time.Second)
	for {
		select {
		case rec, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, rec)
		case <-timeout:
			Fail("MultipartUploadRecord channel did not close within timeout")
		}
	}
}

func enableECForSpec(b *DistributedBackend, k, m int) {
	GinkgoHelper()
	b.SetECConfig(ECConfig{DataShards: k, ParityShards: m})
	nodes := make([]string, 0, k+m)
	for i := 0; i < k+m; i++ {
		nodes = append(nodes, fmt.Sprintf("node-%d", i))
	}
	b.allNodes = nodes
	Expect(b.ECActive()).To(BeTrue())
}
