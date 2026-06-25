package cluster

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/gritive/GrainFS/internal/storage/directio"
)

// LocalManifestStore owns the node-local multipart manifest blob concern under
// {dataDirs[0]}/.qmeta_mpu/{bucket}/{uploadID}. ShardService keeps the RPC and
// cluster fan-out facade; local file I/O is delegated here.
type LocalManifestStore struct {
	dataDirs    []string
	syncDirHook func(string) error
}

// NewLocalManifestStore creates a node-local manifest store rooted at the
// resolved shard data directories used by ShardService.
func NewLocalManifestStore(dataDirs []string) *LocalManifestStore {
	return &LocalManifestStore{dataDirs: dataDirs}
}

func (l *LocalManifestStore) root() (string, bool) {
	if l == nil || len(l.dataDirs) == 0 {
		return "", false
	}
	return filepath.Join(l.dataDirs[0], manifestMPUSubDir), true
}

func (l *LocalManifestStore) fsyncDir(dir string) error {
	if l.syncDirHook != nil {
		return l.syncDirHook(dir)
	}
	return syncDir(dir)
}

func manifestTarget(root, bucket, uploadID string) (string, error) {
	target := filepath.Join(root, bucket, uploadID)
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("uploadID %q escapes root", uploadID)
	}
	return target, nil
}

// Write durably writes the encoded manifest blob for (bucket, uploadID).
func (l *LocalManifestStore) Write(bucket, uploadID string, data []byte) error {
	root, ok := l.root()
	if !ok {
		return fmt.Errorf("manifest blob write: no data dir")
	}
	target, err := manifestTarget(root, bucket, uploadID)
	if err != nil {
		return fmt.Errorf("manifest blob write: %w", err)
	}
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("manifest blob mkdir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".qmeta-*.tmp")
	if err != nil {
		return fmt.Errorf("manifest blob tmp create: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("manifest blob write: %w", err)
	}
	if err := directio.Sync(tmp); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("manifest blob fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("manifest blob tmp close: %w", err)
	}
	if err := os.Rename(tmpName, target); err != nil {
		return fmt.Errorf("manifest blob rename: %w", err)
	}
	if err := syncDirChainNoDedup(dir, root, l.fsyncDir); err != nil {
		return fmt.Errorf("manifest blob dir fsync: %w", err)
	}
	return nil
}

// Read returns the raw manifest blob. Missing files return (nil, false, nil).
func (l *LocalManifestStore) Read(bucket, uploadID string) ([]byte, bool, error) {
	root, ok := l.root()
	if !ok {
		return nil, false, nil
	}
	target, err := manifestTarget(root, bucket, uploadID)
	if err != nil {
		return nil, false, fmt.Errorf("manifest blob read: %w", err)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("manifest blob read: %w", err)
	}
	return data, true, nil
}

// Delete removes the local manifest blob. Missing files are ignored.
func (l *LocalManifestStore) Delete(bucket, uploadID string) error {
	root, ok := l.root()
	if !ok {
		return nil
	}
	target, err := manifestTarget(root, bucket, uploadID)
	if err != nil {
		return fmt.Errorf("manifest blob delete: %w", err)
	}
	err = os.Remove(target)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("manifest blob delete: %w", err)
	}
	return nil
}

// ScanStrict walks .qmeta_mpu/{bucket}/ and returns one manifestEntry per
// uploadID, failing closed on read or decode errors.
func (l *LocalManifestStore) ScanStrict(bucket string) ([]manifestEntry, error) {
	root, ok := l.root()
	if !ok {
		return nil, nil
	}
	bucketRoot := filepath.Join(root, bucket)
	if _, err := os.Stat(bucketRoot); os.IsNotExist(err) {
		return nil, nil
	}
	var out []manifestEntry
	err := filepath.WalkDir(bucketRoot, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			return nil
		}
		if isQuorumMetaTempName(d.Name()) {
			return nil
		}
		uploadID, rerr := filepath.Rel(bucketRoot, path)
		if rerr != nil {
			return rerr
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return fmt.Errorf("manifest blob scan: read %s/%s: %w", bucket, uploadID, rerr)
		}
		meta, derr := unmarshalClusterMultipartMeta(data)
		if derr != nil {
			return fmt.Errorf("manifest blob scan: decode %s/%s: %w", bucket, uploadID, derr)
		}
		out = append(out, manifestEntry{UploadID: uploadID, Meta: meta})
		return nil
	})
	return out, err
}
