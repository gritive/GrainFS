package cluster

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/storage"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Snapshot integration", func() {
	var b *DistributedBackend

	BeforeEach(func() {
		b = newTestDistributedBackend(GinkgoT())
	})

	It("lists no objects for an empty bucket", func() {
		Expect(b.CreateBucket(context.Background(), "empty")).To(Succeed())

		objs, err := b.ListAllObjects()
		Expect(err).NotTo(HaveOccurred())
		Expect(objs).To(BeEmpty())
	})

	It("lists no objects when no buckets exist", func() {
		objs, err := b.ListAllObjects()
		Expect(err).NotTo(HaveOccurred())
		Expect(objs).To(BeEmpty())
	})

	It("preserves version history and delete markers while listing all objects", func() {
		Expect(b.CreateBucket(context.Background(), "tomb")).To(Succeed())

		v1, err := b.PutObject(context.Background(), "tomb", "doc.txt", strings.NewReader("v1"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		v2, err := b.PutObject(context.Background(), "tomb", "doc.txt", strings.NewReader("v2"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		markerID, err := b.DeleteObjectReturningMarker("tomb", "doc.txt")
		Expect(err).NotTo(HaveOccurred())

		objs, err := b.ListAllObjects()
		Expect(err).NotTo(HaveOccurred())

		byVersion := snapshotObjectsByVersion(objs)
		Expect(byVersion).To(HaveKey(v1.VersionID))
		Expect(byVersion).To(HaveKey(v2.VersionID))
		Expect(byVersion).To(HaveKey(markerID))
		Expect(byVersion[v1.VersionID].IsLatest).To(BeFalse())
		Expect(byVersion[v2.VersionID].IsLatest).To(BeFalse())
		Expect(byVersion[markerID].IsLatest).To(BeTrue())
		Expect(byVersion[markerID].IsDeleteMarker).To(BeTrue())
		Expect(byVersion[markerID].ETag).To(Equal(deleteMarkerETag))
	})

	It("reports stale snapshot objects when blobs are missing", func() {
		Expect(b.CreateBucket(context.Background(), "stale")).To(Succeed())

		snap := []storage.SnapshotObject{{
			Bucket:    "stale",
			Key:       "missing.bin",
			ETag:      "etag-xyz",
			Size:      100,
			Modified:  time.Now().UnixMilli(),
			VersionID: "v-missing",
			IsLatest:  true,
		}}

		count, stale, err := b.RestoreObjects(snap)
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(BeZero())
		Expect(stale).To(HaveLen(1))
		Expect(stale[0].Bucket).To(Equal("stale"))
		Expect(stale[0].Key).To(Equal("missing.bin"))
	})

	It("resolves empty version IDs before deleting current metadata", func() {
		Expect(b.CreateBucket(context.Background(), "legacy-wal")).To(Succeed())
		obj, err := b.PutObject(context.Background(), "legacy-wal", "inc.txt", strings.NewReader("included"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.VersionID).NotTo(BeEmpty())

		snap := []storage.SnapshotObject{{
			Bucket:      "legacy-wal",
			Key:         "inc.txt",
			ETag:        obj.ETag,
			Size:        obj.Size,
			ContentType: obj.ContentType,
			Modified:    obj.LastModified,
			IsLatest:    true,
		}}

		count, stale, err := b.RestoreObjects(snap)
		Expect(err).NotTo(HaveOccurred())
		Expect(stale).To(BeEmpty())
		Expect(count).To(Equal(1))

		versions, err := b.ListObjectVersions("legacy-wal", "inc.txt", 10)
		Expect(err).NotTo(HaveOccurred())
		Expect(versions).To(HaveLen(1))
		Expect(versions[0].VersionID).To(Equal(obj.VersionID))
	})

	It("resolves mismatched version IDs when the bucket row is missing", func() {
		Expect(b.CreateBucket(context.Background(), "wal-replay")).To(Succeed())
		obj, err := b.PutObject(context.Background(), "wal-replay", "inc.txt", strings.NewReader("included"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		Expect(obj.VersionID).NotTo(BeEmpty())
		Expect(b.db.Update(func(txn *badger.Txn) error {
			return txn.Delete(b.ks().BucketKey("wal-replay"))
		})).To(Succeed())

		snap := []storage.SnapshotObject{{
			Bucket:      "wal-replay",
			Key:         "inc.txt",
			ETag:        obj.ETag,
			Size:        obj.Size,
			ContentType: obj.ContentType,
			Modified:    obj.LastModified,
			VersionID:   "wal-version-id",
			IsLatest:    true,
		}}

		count, stale, err := b.RestoreObjects(snap)
		Expect(err).NotTo(HaveOccurred())
		Expect(stale).To(BeEmpty())
		Expect(count).To(Equal(1))

		latestVersionID, meta := readLatestObjectMeta(b, "wal-replay", "inc.txt")
		Expect(latestVersionID).To(Equal(obj.VersionID))
		Expect(meta.ETag).To(Equal(obj.ETag))
		Expect(meta.Size).To(Equal(obj.Size))
	})

	It("preserves current EC placement metadata while restoring objects", func() {
		Expect(b.CreateBucket(context.Background(), "ec")).To(Succeed())
		createSnapshotBlob(GinkgoT(), b, "ec", "k", "v1")
		Expect(b.propose(context.Background(), CmdPutObjectMeta, PutObjectMetaCmd{
			Bucket:           "ec",
			Key:              "k",
			Size:             3,
			ContentType:      "text/plain",
			ETag:             "etag",
			ModTime:          123,
			VersionID:        "v1",
			ECData:           1,
			ECParity:         0,
			NodeIDs:          []string{"node-a"},
			PlacementGroupID: "group-2",
		})).To(Succeed())

		count, stale, err := b.RestoreObjects([]storage.SnapshotObject{{
			Bucket:      "ec",
			Key:         "k",
			ETag:        "etag",
			Size:        3,
			ContentType: "text/plain",
			Modified:    123,
			VersionID:   "v1",
			IsLatest:    true,
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(stale).To(BeEmpty())
		Expect(count).To(Equal(1))

		meta := readObjectMetaVersion(b, "ec", "k", "v1")
		Expect(meta.ECData).To(Equal(uint8(1)))
		Expect(meta.ECParity).To(Equal(uint8(0)))
		Expect(meta.NodeIDs).To(Equal([]string{"node-a"}))
		Expect(meta.PlacementGroupID).To(Equal("group-2"))
	})

	It("preserves tags while listing all objects", func() {
		Expect(b.CreateBucket(context.Background(), "tagrt")).To(Succeed())

		obj, err := b.PutObject(context.Background(), "tagrt", "doc.txt", strings.NewReader("v1"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		tags := []storage.Tag{{Key: "env", Value: "prod"}, {Key: "team", Value: "storage"}}
		Expect(b.SetObjectTags("tagrt", "doc.txt", obj.VersionID, tags)).To(Succeed())

		snap, err := b.ListAllObjects()
		Expect(err).NotTo(HaveOccurred())
		Expect(snap).NotTo(BeEmpty())

		byVersion := snapshotObjectsByVersion(snap)
		Expect(byVersion).To(HaveKey(obj.VersionID))
		Expect(byVersion[obj.VersionID].Tags).To(Equal(tags))
	})

	It("preserves tags while restoring objects", func() {
		Expect(b.CreateBucket(context.Background(), "tagsnap")).To(Succeed())
		createSnapshotBlob(GinkgoT(), b, "tagsnap", "doc.bin", "v1")

		snapTags := []storage.Tag{{Key: "env", Value: "prod"}, {Key: "team", Value: "storage"}}
		count, stale, err := b.RestoreObjects([]storage.SnapshotObject{{
			Bucket:      "tagsnap",
			Key:         "doc.bin",
			ETag:        "etag-tag",
			Size:        4,
			ContentType: "application/octet-stream",
			Modified:    time.Now().UnixMilli(),
			VersionID:   "v1",
			IsLatest:    true,
			Tags:        snapTags,
		}})
		Expect(err).NotTo(HaveOccurred())
		Expect(stale).To(BeEmpty())
		Expect(count).To(Equal(1))

		got, err := b.GetObjectTags("tagsnap", "doc.bin", "v1")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(snapTags))
	})

	It("removes objects outside the snapshot", func() {
		Expect(b.CreateBucket(context.Background(), "extra")).To(Succeed())
		for i, k := range []string{"extra1.txt", "extra2.txt"} {
			vid := strings.Repeat("x", i+1)
			createSnapshotBlob(GinkgoT(), b, "extra", k, vid)
			Expect(b.putMeta("extra", k, vid, "etag", 3, "text/plain")).To(Succeed())
		}

		count, stale, err := b.RestoreObjects(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(BeZero())
		Expect(stale).To(BeEmpty())

		objs, err := b.ListAllObjects()
		Expect(err).NotTo(HaveOccurred())
		Expect(objs).To(BeEmpty())
	})

	It("preserves version history and delete markers while restoring objects", func() {
		Expect(b.CreateBucket(context.Background(), "hist")).To(Succeed())

		v1, err := b.PutObject(context.Background(), "hist", "doc.txt", strings.NewReader("v1"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		v2, err := b.PutObject(context.Background(), "hist", "doc.txt", strings.NewReader("v2"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		markerID, err := b.DeleteObjectReturningMarker("hist", "doc.txt")
		Expect(err).NotTo(HaveOccurred())

		snap, err := b.ListAllObjects()
		Expect(err).NotTo(HaveOccurred())

		v3, err := b.PutObject(context.Background(), "hist", "doc.txt", strings.NewReader("v3"), "text/plain")
		Expect(err).NotTo(HaveOccurred())
		Expect(b.RestoreBuckets([]storage.SnapshotBucket{{Name: "hist", VersioningState: "Enabled"}})).To(Succeed())
		count, stale, err := b.RestoreObjects(snap)
		Expect(err).NotTo(HaveOccurred())
		Expect(stale).To(BeEmpty())
		Expect(count).To(Equal(len(snap)))

		versions, err := b.ListObjectVersions("hist", "doc.txt", 10)
		Expect(err).NotTo(HaveOccurred())
		ids := objectVersionsByID(versions)
		Expect(ids).To(HaveKey(v1.VersionID))
		Expect(ids).To(HaveKey(v2.VersionID))
		Expect(ids).To(HaveKey(markerID))
		Expect(ids).NotTo(HaveKey(v3.VersionID))
		Expect(ids[markerID].IsLatest).To(BeTrue())
		Expect(ids[markerID].IsDeleteMarker).To(BeTrue())
	})

	It("finds blobs by versioned path", func() {
		createSnapshotBlob(GinkgoT(), b, "bkt", "file.dat", "v-abc123")

		Expect(b.blobExists("bkt", "file.dat", "v-abc123")).To(BeTrue())
		Expect(b.blobExists("bkt", "file.dat", "v-nonexistent")).To(BeFalse())
	})

	It("resolves empty version IDs before checking blob existence", func() {
		Expect(b.CreateBucket(context.Background(), "res")).To(Succeed())
		createSnapshotBlob(GinkgoT(), b, "res", "obj.bin", "v-resolved")
		Expect(b.putMeta("res", "obj.bin", "v-resolved", "etag", 7, "application/octet-stream")).To(Succeed())

		Expect(b.blobExists("res", "obj.bin", "")).To(BeTrue())
	})

	It("finds legacy unversioned blob paths", func() {
		legacyPath := filepath.Join(b.root, "data", "bkt2", "legacy.dat")
		Expect(os.MkdirAll(filepath.Dir(legacyPath), 0o755)).To(Succeed())
		Expect(os.WriteFile(legacyPath, []byte("legacy"), 0o644)).To(Succeed())

		Expect(b.blobExists("bkt2", "legacy.dat", "")).To(BeTrue())
	})
})

func snapshotObjectsByVersion(objs []storage.SnapshotObject) map[string]storage.SnapshotObject {
	out := make(map[string]storage.SnapshotObject, len(objs))
	for _, obj := range objs {
		out[obj.VersionID] = obj
	}
	return out
}

func objectVersionsByID(versions []*storage.ObjectVersion) map[string]*storage.ObjectVersion {
	out := make(map[string]*storage.ObjectVersion, len(versions))
	for _, version := range versions {
		out[version.VersionID] = version
	}
	return out
}

func createSnapshotBlob(t clusterTestTB, b *DistributedBackend, bucket, key, versionID string) {
	t.Helper()
	p := b.objectPathV(bucket, key, versionID)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("create blob parent: %v", err)
	}
	if err := os.WriteFile(p, []byte("blob"), 0o644); err != nil {
		t.Fatalf("write blob: %v", err)
	}
}

func readLatestObjectMeta(b *DistributedBackend, bucket, key string) (string, objectMeta) {
	GinkgoHelper()
	var latestVersionID string
	var meta objectMeta
	Expect(b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(b.ks().LatestKey(bucket, key))
		if err != nil {
			return err
		}
		if err := item.Value(func(v []byte) error {
			latestVersionID = string(v)
			return nil
		}); err != nil {
			return err
		}
		item, err = txn.Get(b.ks().ObjectMetaKeyV(bucket, key, latestVersionID))
		if err != nil {
			return err
		}
		v, err := b.itemValueCopy(item)
		if err != nil {
			return err
		}
		var decodeErr error
		meta, decodeErr = unmarshalObjectMeta(v)
		return decodeErr
	})).To(Succeed())
	return latestVersionID, meta
}

func readObjectMetaVersion(b *DistributedBackend, bucket, key, versionID string) objectMeta {
	GinkgoHelper()
	var meta objectMeta
	Expect(b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(b.ks().ObjectMetaKeyV(bucket, key, versionID))
		if err != nil {
			return err
		}
		v, err := b.itemValueCopy(item)
		if err != nil {
			return err
		}
		var decodeErr error
		meta, decodeErr = unmarshalObjectMeta(v)
		return decodeErr
	})).To(Succeed())
	return meta
}

func (b *DistributedBackend) putMeta(bucket, key, versionID, etag string, size int64, ct string) error {
	return b.propose(context.Background(), CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:      bucket,
		Key:         key,
		VersionID:   versionID,
		ETag:        etag,
		Size:        size,
		ContentType: ct,
		ModTime:     time.Now().UnixMilli(),
	})
}
