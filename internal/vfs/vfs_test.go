package vfs

import (
	"io"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupFS(t *testing.T) *GrainVFS {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	fs, err := New(backend, "test-vol")
	require.NoError(t, err)
	return fs
}

func TestCreateAndReadFile(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("hello.txt")
	require.NoError(t, err)

	_, err = f.Write([]byte("Hello, GrainFS!"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Read it back
	f2, err := fs.Open("hello.txt")
	require.NoError(t, err)

	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	assert.Equal(t, "Hello, GrainFS!", string(data))
	require.NoError(t, f2.Close())
}

func TestOpenFile(t *testing.T) {
	fs := setupFS(t)

	// Create and write
	f, err := fs.OpenFile("test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Open for reading
	f2, err := fs.OpenFile("test.txt", os.O_RDONLY, 0)
	require.NoError(t, err)
	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	assert.Equal(t, "data", string(data))
	require.NoError(t, f2.Close())
}

func TestStat(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("stat-test.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("12345"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	info, err := fs.Stat("stat-test.txt")
	require.NoError(t, err)
	assert.Equal(t, "stat-test.txt", info.Name())
	assert.Equal(t, int64(5), info.Size())
	assert.False(t, info.IsDir())
}

func TestStatNonexistent(t *testing.T) {
	fs := setupFS(t)
	_, err := fs.Stat("nonexistent.txt")
	assert.Error(t, err)
}

func TestRemove(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("remove-me.txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	err = fs.Remove("remove-me.txt")
	require.NoError(t, err)

	_, err = fs.Stat("remove-me.txt")
	assert.Error(t, err)
}

func TestRename(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("old-name.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("content"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	err = fs.Rename("old-name.txt", "new-name.txt")
	require.NoError(t, err)

	_, err = fs.Stat("old-name.txt")
	assert.Error(t, err)

	f2, err := fs.Open("new-name.txt")
	require.NoError(t, err)
	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	assert.Equal(t, "content", string(data))
	require.NoError(t, f2.Close())
}

func TestMkdirAllAndReadDir(t *testing.T) {
	fs := setupFS(t)

	err := fs.MkdirAll("a/b/c", 0755)
	require.NoError(t, err)

	// Create files in directories
	f, err := fs.Create("a/file1.txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f, err = fs.Create("a/file2.txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	entries, err := fs.ReadDir("a")
	require.NoError(t, err)

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	// Should contain at least the files and the subdirectory
	assert.Contains(t, names, "b")
	assert.Contains(t, names, "file1.txt")
	assert.Contains(t, names, "file2.txt")
}

func TestReadDirRoot(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("root-file.txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	entries, err := fs.ReadDir("/")
	require.NoError(t, err)
	assert.NotEmpty(t, entries)
}

func TestStatDirectory(t *testing.T) {
	fs := setupFS(t)

	err := fs.MkdirAll("mydir", 0755)
	require.NoError(t, err)

	info, err := fs.Stat("mydir")
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestChroot(t *testing.T) {
	fs := setupFS(t)

	err := fs.MkdirAll("sub", 0755)
	require.NoError(t, err)

	f, err := fs.Create("sub/inner.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("inner data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	sub, err := fs.Chroot("sub")
	require.NoError(t, err)

	f2, err := sub.Open("inner.txt")
	require.NoError(t, err)
	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	assert.Equal(t, "inner data", string(data))
	require.NoError(t, f2.Close())
}

func TestRoot(t *testing.T) {
	fs := setupFS(t)
	assert.Equal(t, "/", fs.Root())
}

func TestSeek(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("seek-test.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("abcdefghij"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f2, err := fs.Open("seek-test.txt")
	require.NoError(t, err)

	// Seek to position 5
	pos, err := f2.Seek(5, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(5), pos)

	buf := make([]byte, 5)
	n, err := f2.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "fghij", string(buf))
	require.NoError(t, f2.Close())
}

func TestTruncate(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("trunc.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("1234567890"))
	require.NoError(t, err)

	err = f.Truncate(5)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	info, err := fs.Stat("trunc.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(5), info.Size())
}

func TestTruncateExtend(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("trunc-ext.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("abc"))
	require.NoError(t, err)

	// Truncate to larger size pads with zeros
	err = f.Truncate(6)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	info, err := fs.Stat("trunc-ext.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(6), info.Size())

	f2, err := fs.Open("trunc-ext.txt")
	require.NoError(t, err)
	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	assert.Equal(t, []byte("abc\x00\x00\x00"), data)
	require.NoError(t, f2.Close())
}

func TestJoin(t *testing.T) {
	fs := setupFS(t)
	assert.Equal(t, "a/b/c", fs.Join("a", "b", "c"))
	assert.Equal(t, "a/b", fs.Join("a", "b"))
	assert.Equal(t, "file.txt", fs.Join("file.txt"))
}

func TestSymlinkNotSupported(t *testing.T) {
	fs := setupFS(t)
	err := fs.Symlink("target", "link")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "symlinks not supported")
}

func TestReadlinkNotSupported(t *testing.T) {
	fs := setupFS(t)
	_, err := fs.Readlink("link")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "symlinks not supported")
}

func TestLstat(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("lstat.txt")
	require.NoError(t, err)
	_, _ = f.Write([]byte("hello"))
	require.NoError(t, f.Close())

	info, err := fs.Lstat("lstat.txt")
	require.NoError(t, err)
	assert.Equal(t, "lstat.txt", info.Name())
	assert.Equal(t, int64(5), info.Size())
	assert.False(t, info.IsDir())
}

func TestTempFile(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.TempFile("", "tmp-")
	require.NoError(t, err)

	_, err = f.Write([]byte("temp data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Verify the temp file can be read back
	f2, err := fs.Open(f.Name())
	require.NoError(t, err)
	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	assert.Equal(t, "temp data", string(data))
	require.NoError(t, f2.Close())
}

func TestChmodChownChtimes(t *testing.T) {
	fs := setupFS(t)

	// These are no-ops but should not error
	assert.NoError(t, fs.Chmod("anything", 0755))
	assert.NoError(t, fs.Lchown("anything", 1000, 1000))
	assert.NoError(t, fs.Chown("anything", 1000, 1000))

	now := time.Now()
	assert.NoError(t, fs.Chtimes("anything", now, now))
}

func TestLockUnlock(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("lock.txt")
	require.NoError(t, err)

	assert.NoError(t, f.Lock())
	assert.NoError(t, f.Unlock())
	require.NoError(t, f.Close())
}

func TestReadAt(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("readat.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("abcdefghij"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f2, err := fs.Open("readat.txt")
	require.NoError(t, err)

	// Read from offset 3
	buf := make([]byte, 4)
	n, err := f2.ReadAt(buf, 3)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, "defg", string(buf))

	// Read past end of file
	buf2 := make([]byte, 20)
	n, err = f2.ReadAt(buf2, 7)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 3, n)
	assert.Equal(t, "hij", string(buf2[:n]))

	// Read at offset beyond file size
	_, err = f2.ReadAt(buf, 100)
	assert.ErrorIs(t, err, io.EOF)

	require.NoError(t, f2.Close())
}

func TestReadAtNilBuffer(t *testing.T) {
	fs := setupFS(t)

	// Open a file that doesn't load data (O_CREATE without O_TRUNC sets buf via loadExisting)
	f, err := fs.OpenFile("empty-readat.txt", os.O_CREATE|os.O_TRUNC, 0666)
	require.NoError(t, err)
	// Don't write anything, close immediately so buf is empty
	require.NoError(t, f.Close())

	// Reopen and attempt ReadAt on empty file
	f2, err := fs.Open("empty-readat.txt")
	require.NoError(t, err)
	buf := make([]byte, 4)
	_, err = f2.ReadAt(buf, 0)
	assert.ErrorIs(t, err, io.EOF)
	require.NoError(t, f2.Close())
}

func TestFileInfoMethods(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("info-test.txt")
	require.NoError(t, err)
	_, _ = f.Write([]byte("content"))
	require.NoError(t, f.Close())

	info, err := fs.Stat("info-test.txt")
	require.NoError(t, err)

	assert.Equal(t, "info-test.txt", info.Name())
	assert.Equal(t, int64(7), info.Size())
	assert.Equal(t, os.FileMode(0644), info.Mode())
	assert.False(t, info.ModTime().IsZero())
	assert.False(t, info.IsDir())
	assert.Nil(t, info.Sys())
}

func TestFileName(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("named-file.txt")
	require.NoError(t, err)
	assert.Equal(t, "named-file.txt", f.Name())
	require.NoError(t, f.Close())
}

func TestSeekCurrentAndEnd(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("seek-modes.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("0123456789"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f2, err := fs.Open("seek-modes.txt")
	require.NoError(t, err)

	// SeekCurrent from position 0
	pos, err := f2.Seek(3, io.SeekCurrent)
	require.NoError(t, err)
	assert.Equal(t, int64(3), pos)

	// Read one byte to verify position
	buf := make([]byte, 1)
	n, err := f2.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, "3", string(buf))

	// SeekEnd: go to 2 bytes before end
	pos, err = f2.Seek(-2, io.SeekEnd)
	require.NoError(t, err)
	assert.Equal(t, int64(8), pos)

	buf2 := make([]byte, 2)
	n, err = f2.Read(buf2)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, "89", string(buf2))

	// Seek to negative position clamps to 0
	pos, err = f2.Seek(-100, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(0), pos)

	require.NoError(t, f2.Close())
}

func TestRootAfterChroot(t *testing.T) {
	fs := setupFS(t)

	require.NoError(t, fs.MkdirAll("sub/dir", 0755))

	sub, err := fs.Chroot("sub/dir")
	require.NoError(t, err)
	assert.Equal(t, "/sub/dir", sub.Root())
}

func TestRemoveDirectory(t *testing.T) {
	fs := setupFS(t)

	require.NoError(t, fs.MkdirAll("rmdir", 0755))

	// Verify it exists as a directory
	info, err := fs.Stat("rmdir")
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	// Remove directory — exercises the directory marker path in Remove.
	// Note: LocalBackend's DeleteObject succeeds silently for non-existent keys,
	// so the first attempt (as file) succeeds and the marker remains.
	err = fs.Remove("rmdir")
	require.NoError(t, err)
}

func TestCloseAlreadyClosed(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("double-close.txt")
	require.NoError(t, err)
	_, _ = f.Write([]byte("data"))

	require.NoError(t, f.Close())
	// Second close should be no-op
	require.NoError(t, f.Close())
}

func TestOpenFileCreateWithoutTrunc(t *testing.T) {
	fs := setupFS(t)

	// First create the file with some content
	f, err := fs.Create("existing.txt")
	require.NoError(t, err)
	_, _ = f.Write([]byte("original"))
	require.NoError(t, f.Close())

	// Open with O_CREATE but without O_TRUNC — should load existing
	f2, err := fs.OpenFile("existing.txt", os.O_CREATE|os.O_RDWR, 0666)
	require.NoError(t, err)
	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	assert.Equal(t, "original", string(data))
	require.NoError(t, f2.Close())
}

func TestRename_LargeFile_MemoryBounded(t *testing.T) {
	fs := setupFS(t)

	const fileSize = 5 * 1024 * 1024
	data := make([]byte, fileSize)
	rand.Read(data)

	// 파일 생성
	f, err := fs.Create("big.bin")
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// GC 후 메모리 측정
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	require.NoError(t, fs.Rename("big.bin", "big2.bin"))

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// io.Pipe 사용 시 힙 증가는 OS 파이프 버퍼(~64KB) 수준이어야 함
	heapGrowth := int64(after.TotalAlloc) - int64(before.TotalAlloc)
	t.Logf("Rename heap alloc during: %d bytes (file size: %d)", heapGrowth, fileSize)
	assert.Less(t, heapGrowth, int64(fileSize), "Rename should not allocate a full copy of the file in one shot")

	// 데이터 정확성 확인
	f2, err := fs.Open("big2.bin")
	require.NoError(t, err)
	got, err := io.ReadAll(f2)
	require.NoError(t, err)
	require.NoError(t, f2.Close())
	assert.Equal(t, data, got)
}

func TestOpenNonexistent(t *testing.T) {
	fs := setupFS(t)

	_, err := fs.Open("does-not-exist.txt")
	assert.Error(t, err)
}

func TestReadEmptyFile(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("empty.txt")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f2, err := fs.Open("empty.txt")
	require.NoError(t, err)
	data, err := io.ReadAll(f2)
	require.NoError(t, err)
	assert.Empty(t, data)
	require.NoError(t, f2.Close())
}

func TestRenameNonexistent(t *testing.T) {
	fs := setupFS(t)

	err := fs.Rename("nonexistent.txt", "new.txt")
	assert.Error(t, err)
}

// TestGrainFileStreamRead: 읽기 전용 Open은 스트리밍 모드(rc != nil, buf == nil)여야 하며
// io.ReadAll로 전체 내용을 올바르게 반환해야 한다.
func TestGrainFileStreamRead(t *testing.T) {
	fs := setupFS(t)

	content := []byte("hello streaming world")
	f, err := fs.Create("stream.txt")
	require.NoError(t, err)
	_, err = f.Write(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// 읽기 전용으로 열면 스트리밍 모드여야 함
	f2, err := fs.Open("stream.txt")
	require.NoError(t, err)

	gf := f2.(*grainFile)
	assert.NotNil(t, gf.rc, "읽기 전용 Open은 rc가 설정되어야 함 (스트리밍 모드)")
	assert.Nil(t, gf.buf, "스트리밍 모드에서 buf는 nil이어야 함")

	got, err := io.ReadAll(f2)
	require.NoError(t, err)
	assert.Equal(t, content, got)
	require.NoError(t, f2.Close())
}

// TestGrainFileSeekFallback: 스트리밍 모드 중 Seek 호출 시 loadExisting()으로 fallback해
// buf 모드로 전환되어야 하며, 이후 읽기는 파일 전체를 정확히 반환해야 한다.
func TestGrainFileSeekFallback(t *testing.T) {
	fs := setupFS(t)

	content := []byte("0123456789")
	f, err := fs.Create("seek.txt")
	require.NoError(t, err)
	_, err = f.Write(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f2, err := fs.Open("seek.txt")
	require.NoError(t, err)

	// 앞 5바이트 스트리밍 읽기
	buf := make([]byte, 5)
	n, err := f2.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("01234"), buf)

	// Seek(0, SeekStart) → rc fallback 트리거
	pos, err := f2.Seek(0, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(0), pos)

	gf := f2.(*grainFile)
	assert.Nil(t, gf.rc, "Seek 후 rc는 nil이어야 함 (buf 모드로 전환)")
	assert.NotNil(t, gf.buf, "Seek 후 buf가 로드되어야 함")

	// 처음부터 전체 읽기
	got, err := io.ReadAll(f2)
	require.NoError(t, err)
	assert.Equal(t, content, got)
	require.NoError(t, f2.Close())
}

// TestGrainFileCloseWithUnconsumedRC: Read 없이 Close하면 rc.Close()가 호출되어
// 리소스 릭이 발생하지 않아야 하며, 이후 파일 재접근이 가능해야 한다.
func TestGrainFileCloseWithUnconsumedRC(t *testing.T) {
	fs := setupFS(t)

	f, err := fs.Create("no-read.txt")
	require.NoError(t, err)
	_, err = f.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// 열고 읽지 않은 채 닫기
	f2, err := fs.Open("no-read.txt")
	require.NoError(t, err)

	gf := f2.(*grainFile)
	assert.NotNil(t, gf.rc, "Open 직후 스트리밍 모드여야 함")

	err = f2.Close()
	require.NoError(t, err)
	assert.Nil(t, gf.rc, "Close 후 rc는 nil이어야 함")

	// 파일이 손상되지 않았는지 재확인
	f3, err := fs.Open("no-read.txt")
	require.NoError(t, err)
	got, err := io.ReadAll(f3)
	require.NoError(t, err)
	require.NoError(t, f3.Close())
	assert.Equal(t, []byte("data"), got)
}

// TestGrainFileReadAtSequential: 스트리밍 모드에서 순차 ReadAt(off==pos)은 rc에서 직접 읽어야 한다.
// NFS READ RPC 핫패스(go-nfs onRead: Open → ReadAt)가 이 경로를 사용한다.
func TestGrainFileReadAtSequential(t *testing.T) {
	fs := setupFS(t)

	content := []byte("ABCDEFGHIJ")
	f, err := fs.Create("seq-readat.txt")
	require.NoError(t, err)
	_, err = f.Write(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f2, err := fs.Open("seq-readat.txt")
	require.NoError(t, err)

	gf := f2.(*grainFile)
	assert.NotNil(t, gf.rc, "Open 후 스트리밍 모드여야 함")

	// 순차 ReadAt: off=0 → off=5 (pos가 이전 읽기에 의해 진행됨)
	buf1 := make([]byte, 5)
	n, err := f2.(*grainFile).ReadAt(buf1, 0)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("ABCDE"), buf1)
	assert.NotNil(t, gf.rc, "순차 ReadAt 후 여전히 스트리밍 모드여야 함")
	assert.Equal(t, int64(5), gf.pos)

	buf2 := make([]byte, 5)
	n, err = f2.(*grainFile).ReadAt(buf2, 5)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("FGHIJ"), buf2)
	assert.NotNil(t, gf.rc, "순차 ReadAt 2회 후 여전히 스트리밍 모드여야 함")

	require.NoError(t, f2.Close())
}

// TestGrainFileReadAtRandom: 스트리밍 모드에서 랜덤 ReadAt(off!=pos)은 loadExisting fallback으로 전환해야 한다.
func TestGrainFileReadAtRandom(t *testing.T) {
	fs := setupFS(t)

	content := []byte("0123456789")
	f, err := fs.Create("rand-readat.txt")
	require.NoError(t, err)
	_, err = f.Write(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f2, err := fs.Open("rand-readat.txt")
	require.NoError(t, err)

	gf := f2.(*grainFile)
	assert.NotNil(t, gf.rc, "Open 후 스트리밍 모드여야 함")

	// 랜덤 접근 (off=5, pos=0 → off != pos)
	buf := make([]byte, 3)
	n, err := f2.(*grainFile).ReadAt(buf, 5)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, []byte("567"), buf)
	assert.Nil(t, gf.rc, "랜덤 ReadAt 후 rc는 nil이어야 함 (buf 모드)")
	assert.NotNil(t, gf.buf, "랜덤 ReadAt 후 buf가 로드되어야 함")

	require.NoError(t, f2.Close())
}

func TestGrainFile_BufPool_Reuse(t *testing.T) {
	fs := setupFS(t)

	// 첫 번째 write+close: Close 후 buf가 nil이어야 함 (pool에 반환)
	f1, err := fs.Create("pool-test.txt")
	require.NoError(t, err)
	_, err = f1.Write([]byte("hello pool"))
	require.NoError(t, err)
	gf1 := f1.(*grainFile)
	require.NotNil(t, gf1.buf)
	require.NoError(t, f1.Close())
	assert.Nil(t, gf1.buf, "Close 후 grainFile.buf는 nil이어야 함 (pool 반환)")

	// 두 번째 write+close: 정상 동작 확인 (pool 재사용은 런타임 결정)
	f2, err := fs.Create("pool-test2.txt")
	require.NoError(t, err)
	_, err = f2.Write([]byte("reused"))
	require.NoError(t, err)
	gf2 := f2.(*grainFile)
	require.NotNil(t, gf2.buf)
	require.NoError(t, f2.Close())
	assert.Nil(t, gf2.buf, "Close 후 grainFile.buf는 nil이어야 함 (pool 반환)")
}

func TestGrainFile_RandomWrite_HonorsPos(t *testing.T) {
	// Iteration 3 fix — without this, every Write appended regardless of pos.
	fs := setupFS(t)

	f, err := fs.OpenFile("rand.bin", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	defer f.Close()

	// Pre-fill with 16 bytes so we can overlay.
	_, err = f.Write([]byte("AAAAAAAAAAAAAAAA"))
	require.NoError(t, err)

	// Overlay write at offset 4 with 4 bytes 'B'.
	_, err = f.Seek(4, io.SeekStart)
	require.NoError(t, err)
	n, err := f.Write([]byte("BBBB"))
	require.NoError(t, err)
	require.Equal(t, 4, n)

	// Overlay+extend at offset 14 with 6 bytes 'C' (4 overlay + 2 append).
	_, err = f.Seek(14, io.SeekStart)
	require.NoError(t, err)
	n, err = f.Write([]byte("CCCCCC"))
	require.NoError(t, err)
	require.Equal(t, 6, n)

	// Pos beyond end at offset 22 — gap of 2 zeros.
	_, err = f.Seek(22, io.SeekStart)
	require.NoError(t, err)
	n, err = f.Write([]byte("DD"))
	require.NoError(t, err)
	require.Equal(t, 2, n)

	require.NoError(t, f.Close())

	// Read back.
	r, err := fs.Open("rand.bin")
	require.NoError(t, err)
	defer r.Close()
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "AAAABBBBAAAAAACCCCCC\x00\x00DD", string(got))
}

func TestGrainFile_RandomWrite_FixedSize(t *testing.T) {
	// 4 KB writes at 100 random offsets in a 64 KB file MUST keep file ≤ 64 KB.
	// Pre-fix: file would grow to 100 × 4 KB = 400 KB.
	fs := setupFS(t)

	f, err := fs.OpenFile("rand2.bin", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	const fileSize = 64 * 1024
	const blkSize = 4 * 1024
	// Pre-fill 64 KB.
	_, err = f.Write(make([]byte, fileSize))
	require.NoError(t, err)

	// 100 random 4 KB writes within [0, 60KB).
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 100; i++ {
		off := int64(rng.Intn(fileSize - blkSize))
		_, err = f.Seek(off, io.SeekStart)
		require.NoError(t, err)
		n, err := f.Write(make([]byte, blkSize))
		require.NoError(t, err)
		require.Equal(t, blkSize, n)
	}
	require.NoError(t, f.Close())

	info, err := fs.Stat("rand2.bin")
	require.NoError(t, err)
	require.Equal(t, int64(fileSize), info.Size(),
		"random writes must not inflate file size beyond initial extent")
}
