package wal

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func walSegmentsIn(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var files []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "wal-") && strings.HasSuffix(e.Name(), ".bin") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	return files
}

// fakeGenSealer is a single-key RecordSealer whose reported generation is
// mutable, so an internal test can drive rotate() with explicit gens without the
// storage adapter (which package wal cannot import — cycle). Open ignores gen.
type fakeGenSealer struct {
	enc *encrypt.Encryptor
	cid []byte
	gen uint32
}

func newFakeGenSealer(t *testing.T) *fakeGenSealer {
	t.Helper()
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte("k"), 32))
	require.NoError(t, err)
	return &fakeGenSealer{enc: enc, cid: make([]byte, 16)}
}

func (s *fakeGenSealer) Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	ct, err := s.enc.SealValueAADTo(nil, encrypt.BuildAAD(domain, s.cid, fields...), plain)
	return ct, s.gen, err
}

func (s *fakeGenSealer) Open(domain encrypt.AADDomain, fields []encrypt.AADField, _ uint32, ct []byte) ([]byte, error) {
	return s.enc.OpenValueAADTo(nil, encrypt.BuildAAD(domain, s.cid, fields...), ct)
}

func stagedHeaderGen(t *testing.T, path string) uint32 {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(b), 12)
	return binary.BigEndian.Uint32(b[8:12])
}

func writeV4Header(t *testing.T, path string, gen uint32, extra int) {
	t.Helper()
	buf := make([]byte, 12+extra)
	binary.BigEndian.PutUint32(buf[0:4], fileMagic)
	binary.BigEndian.PutUint32(buf[4:8], fileVersionV4)
	binary.BigEndian.PutUint32(buf[8:12], gen)
	require.NoError(t, os.WriteFile(path, buf, 0o644))
}

// rotate(seq, gen) pins the passed gen in a fresh segment, and reuses a
// header-only crash leftover under the same name by re-pinning it at the live gen.
func TestRotatePinsGenAndReusesHeaderOnlyLeftover(t *testing.T) {
	dir := t.TempDir()
	w, err := open(dir, newFakeGenSealer(t), "ns", fileVersionV4)
	require.NoError(t, err)
	defer w.Close()

	w.mu.Lock()
	require.NoError(t, w.rotate(5, 3))
	w.mu.Unlock()
	require.Equal(t, uint32(3), stagedHeaderGen(t, filepath.Join(dir, "wal-0000000005.bin")))
	require.Equal(t, uint32(3), w.dekGen)

	// Pre-stage a header-only (12-byte) v4 leftover at a higher name, rotate onto it.
	p9 := filepath.Join(dir, "wal-0000000009.bin")
	writeV4Header(t, p9, 0, 0)
	w.mu.Lock()
	err = w.rotate(9, 7)
	w.mu.Unlock()
	require.NoError(t, err, "header-only leftover must be reused, not errored")
	require.Equal(t, uint32(7), stagedHeaderGen(t, p9), "leftover re-pinned at the live gen")
	fi, err := os.Stat(p9)
	require.NoError(t, err)
	require.Equal(t, int64(12), fi.Size(), "reuse truncates to a bare header")
}

// The gen-change roll clause is the heart of S3: with an open, non-full segment,
// a rotation (sealed gen != pinned header gen) MUST roll to a new segment.
// Deterministic — deleting the `gen != w.dekGen` clause makes entry 2 land in the
// gen-0 segment (one segment, not two) and fails this test.
func TestWriteEntryRollsOnGenChange(t *testing.T) {
	dir := t.TempDir()
	s := newFakeGenSealer(t)
	w, err := open(dir, s, "ns", fileVersionV4)
	require.NoError(t, err)
	defer w.Close()

	s.gen = 0
	w.mu.Lock()
	require.NoError(t, w.writeEntry(Entry{Seq: 1, Op: OpPut, Bucket: "b", Key: "k1"}))
	w.mu.Unlock()

	s.gen = 1 // DEK rotation: next seal returns gen 1 against the gen-0 header
	w.mu.Lock()
	require.NoError(t, w.writeEntry(Entry{Seq: 2, Op: OpPut, Bucket: "b", Key: "k2"}))
	w.mu.Unlock()

	files := walSegmentsIn(t, dir)
	require.Len(t, files, 2, "gen change with an open segment must roll to a new segment")
	require.Equal(t, uint32(0), stagedHeaderGen(t, files[0]), "first entry pinned at gen 0")
	require.Equal(t, uint32(1), stagedHeaderGen(t, files[1]), "post-rotation entry rolled to a gen-1 segment")
}

// rotate must fail closed (never truncate) when the target name already holds a
// segment with bytes past the header — defensive against clobbering committed data.
func TestRotateFailsClosedOnNonEmptyLeftover(t *testing.T) {
	dir := t.TempDir()
	w, err := open(dir, newFakeGenSealer(t), "ns", fileVersionV4)
	require.NoError(t, err)
	defer w.Close()

	p := filepath.Join(dir, "wal-0000000005.bin")
	writeV4Header(t, p, 0, 20) // header + 20 bytes "past the header"

	w.mu.Lock()
	err = w.rotate(5, 7)
	w.mu.Unlock()
	require.Error(t, err, "rotate onto a non-empty segment must fail closed")

	fi, statErr := os.Stat(p)
	require.NoError(t, statErr)
	require.Equal(t, int64(32), fi.Size(), "fail-closed must not truncate the segment")
}
