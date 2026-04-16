# NFS Client Spike - Testing Approach Research

**Week 1 - NFS Client Spike**

## Problem

We need to test NFS operations in E2E tests (e.g., verify S3 PUT → NFS stat consistency). However, `github.com/willscott/go-nfs` is server-only, with no client implementation.

## Research Options

### Option 1: Shell out to `mount.nfs` CLI

**Approach:** Use OS `mount` command to mount NFS, then use standard Go `os` package for file operations.

**Pros:**
- Uses native NFS client (battle-tested)
- Simple to implement
- Works on both Linux and macOS

**Cons:**
- Requires root/sudo privileges
- Slower (fork/exec mount commands)
- Platform-specific mount options

**Example:**
```go
func mountNFS(addr string, mountPoint string) error {
    return exec.Command("mount", "-t", "nfs", "-o", "port="+addr, addr, mountPoint).Run()
}

func testNFSStat(mountPoint, path string) (os.FileInfo, error) {
    return os.Stat(filepath.Join(mountPoint, path))
}
```

### Option 2: Use Go NFS client library

**Research:** Search for Go NFS client implementations.

**Candidates:**
1. `github.com/sirupsen/go-nfs` - unmaintained, last update 2015
2. `github.com/vlakam/go-nfs` - unknown quality
3. Write custom NFSv3 client - complex, NFS protocol is non-trivial

**Verdict:** No viable Go NFS client libraries. Shell wrapper is safer.

### Option 3: Docker-based NFS Testing

**Approach:** Run NFS server and client in Docker containers.

**Pros:**
- Isolated test environment
- No root required on host
- Reproducible across platforms

**Cons:**
- Complex setup
- Slower (Docker overhead)
- May not work on macOS Docker (networking issues)

## Recommendation

**Use Option 1: Shell wrapper to `mount.nfs`**

**Implementation Plan:**

1. **Create test helper in `tests/e2e/helpers_test.go`:**

```go
// mountNFS mounts the NFS server to a temporary directory.
// Returns mount path and cleanup function.
func mountNFS(t *testing.T, addr string) (string, func()) {
    t.Helper()

    mountDir, err := os.MkdirTemp("", "grainfs-nfs-mount-*")
    if err != nil {
        t.Fatalf("mkdtemp: %v", err)
    }

    // Mount NFS (requires sudo on Linux, different on macOS)
    cmd := exec.Command("sudo", "mount", "-t", "nfs",
        "-o", "port="+addr+",nolock",
        "127.0.0.1:"+addr, mountDir)
    if err := cmd.Run(); err != nil {
        os.RemoveAll(mountDir)
        t.Fatalf("mount nfs: %v", err)
    }

    cleanup := func() {
        exec.Command("sudo", "umount", mountDir).Run()
        os.RemoveAll(mountDir)
    }

    return mountDir, cleanup
}
```

2. **Linux vs macOS considerations:**

**Linux:**
```bash
sudo mount -t nfs -o port=2049,nolock 127.0.0.1:/ /tmp/nfs-mount
```

**macOS:**
```bash
# macOS NFS client is finicky, may require:
sudo mount -t nfs -o vers=3,port=2049,resvport 127.0.0.1:/ /tmp/nfs-mount
```

3. **Skip tests on CI if NFS not available:**

```go
func TestCrossProtocolS3PutNFSStat(t *testing.T) {
    if _, err := exec.LookPath("mount"); err != nil {
        t.Skip("mount command not available")
    }

    // ... test implementation ...
}
```

## Alternative: Unit Test VFS Directly

If NFS mounting proves too complex, we can unit test VFS cache invalidation directly:

```go
func TestVFSCacheInvalidation(t *testing.T) {
    backend := storage.NewCachedBackend(...)
    fs, err := vfs.New(backend, "test-volume")
    if err != nil {
        t.Fatal(err)
    }

    // Create file via backend (simulates S3 PUT)
    backend.PutObject("test-bucket", "file.txt", ...)

    // Stat via VFS (should cache result)
    info1, _ := fs.Stat("file.txt")

    // Invalidate cache via backend OnApply callback
    backend.InvalidateKey("test-bucket", "file.txt")
    // TODO: Need VFS.Invalidate() method here

    // Stat again (should hit storage, not cache)
    info2, _ := fs.Stat("file.txt")

    // Verify caches were cleared
}
```

## Decision Gate

**Week 1:** Try shell wrapper approach. If too complex or unreliable, fall back to unit testing VFS directly and defer integration testing to Week 3-4 when we have more infrastructure.

**Week 2 Technical Spike:** Implement whichever approach works and measure performance regression.
