package nfs4server

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// buildWriteOp builds an OpWrite XDR op for the given offset and data.
func buildWriteOp(offset uint64, data []byte) []byte {
	w := &XDRWriter{}
	w.WriteUint32(uint32(OpWrite))
	w.buf.Write(make([]byte, 16)) // stateid
	w.WriteUint64(offset)
	w.WriteUint32(2) // stable = DATA_SYNC4
	w.WriteOpaque(data)
	return w.Bytes()
}

// buildReadOp builds an OpRead XDR op for the given offset and count.
func buildReadOp(offset uint64, count uint32) []byte {
	w := &XDRWriter{}
	w.WriteUint32(uint32(OpRead))
	w.buf.Write(make([]byte, 16)) // stateid
	w.WriteUint64(offset)
	w.WriteUint32(count)
	return w.Bytes()
}

// readFileBytes reads up to count bytes at offset from name and returns content.
// Fails the test if status is not NFS4_OK.
func readFileBytes(t *testing.T, conn net.Conn, xid uint32, name string, offset uint64, count uint32) []byte {
	t.Helper()
	compound := buildCompound40(
		buildPutRootFHOp(),
		buildLookupOp(name),
		buildReadOp(offset, count),
	)
	require.NoError(t, writeRPCFrame(conn, buildRPCCallFrame(xid, compound)))
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)
	status, r := parseCompoundReply(t, reply)
	require.Equal(t, uint32(NFS4_OK), status, "readFileBytes compound failed")
	r.ReadUint32() // opCount
	r.ReadUint32()
	r.ReadUint32() // PUTROOTFH
	r.ReadUint32()
	r.ReadUint32() // LOOKUP
	r.ReadUint32()
	r.ReadUint32() // READ opcode+status
	r.ReadUint32() // eof
	data, _ := r.ReadOpaque()
	return data
}

// writeAt sends a single OpWrite via the connection. Returns the NFS status.
func writeAt(t *testing.T, conn net.Conn, xid uint32, name string, offset uint64, data []byte) uint32 {
	t.Helper()
	compound := buildCompound40(
		buildPutRootFHOp(),
		buildLookupOp(name),
		buildWriteOp(offset, data),
	)
	if err := writeRPCFrame(conn, buildRPCCallFrame(xid, compound)); err != nil {
		t.Fatalf("writeRPCFrame: %v", err)
	}
	reply, err := readRPCFrame(conn)
	require.NoError(t, err)
	status, _ := parseCompoundReply(t, reply)
	return status
}

// TestConcurrentWrite_OffsetZeroVsRMW exposes a race between an offset=0 writer
// (which currently bypasses LockPath) and an offset>0 RMW writer (which uses
// LockPath). On buggy code, the offset=0 writer can clobber the RMW writer's
// data because the two paths are not mutually serialised.
func TestConcurrentWrite_OffsetZeroVsRMW(t *testing.T) {
	addr, _ := startTestNFS4Server(t)

	// Create the file via conn0 (offset=0 initial write).
	conn0, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn0.Close()

	const (
		chunkSize  = 4096
		offsetHigh = 4096
		fileSize   = 8192
		iterations = 50
	)

	chunkA := bytes.Repeat([]byte{0xAA}, chunkSize) // offset 0
	chunkB := bytes.Repeat([]byte{0xBB}, chunkSize) // offset 4096

	createAndWriteFile(t, conn0, 10, "race.bin", chunkA)

	// Pre-populate offset 4096..8191 with chunkB so we have a known good state.
	require.Equal(t, uint32(NFS4_OK), writeAt(t, conn0, 11, "race.bin", offsetHigh, chunkB))

	// Two connections for concurrent writes.
	conn1, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn1.Close()
	conn2, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn2.Close()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []string
	addFailure := func(s string) {
		mu.Lock()
		failures = append(failures, s)
		mu.Unlock()
	}

	var start sync.WaitGroup
	start.Add(2)

	wg.Add(2)
	// Goroutine 1: re-write chunkA at offset=0 repeatedly (no LockPath in buggy code).
	go func() {
		defer wg.Done()
		start.Done()
		start.Wait()
		for i := 0; i < iterations; i++ {
			st := writeAt(t, conn1, uint32(100+i), "race.bin", 0, chunkA)
			if st != uint32(NFS4_OK) {
				addFailure(fmt.Sprintf("writer1 i=%d status=%d", i, st))
				return
			}
		}
	}()
	// Goroutine 2: rewrite chunkB at offset=4096 repeatedly (RMW path with LockPath).
	go func() {
		defer wg.Done()
		start.Done()
		start.Wait()
		for i := 0; i < iterations; i++ {
			st := writeAt(t, conn2, uint32(200+i), "race.bin", offsetHigh, chunkB)
			if st != uint32(NFS4_OK) {
				addFailure(fmt.Sprintf("writer2 i=%d status=%d", i, st))
				return
			}
		}
	}()
	wg.Wait()

	for _, f := range failures {
		t.Error(f)
	}

	// After all writes, the file should contain chunkA at [0,4096) and chunkB
	// at [4096,8192). Read each region and check byte-by-byte.
	conn3, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn3.Close()

	gotA := readFileBytes(t, conn3, 1000, "race.bin", 0, chunkSize)
	gotB := readFileBytes(t, conn3, 1001, "race.bin", offsetHigh, chunkSize)

	if !bytes.Equal(gotA, chunkA) {
		t.Errorf("chunkA at offset=0 corrupted: first 8 bytes got=%x want=%x", gotA[:min(8, len(gotA))], chunkA[:8])
	}
	if !bytes.Equal(gotB, chunkB) {
		t.Errorf("chunkB at offset=%d corrupted (lost write): first 8 bytes got=%x want=%x",
			offsetHigh, gotB[:min(8, len(gotB))], chunkB[:8])
	}

	// File size must remain fileSize; if writer1's offset=0 write clobbered the
	// file, GetObject would return 4KB and writer2's RMW reads it as 4KB,
	// then extends — but reads would still come back with chunkB. The actual
	// failure mode shows up as gotB containing zero-fill (extension bytes)
	// instead of chunkB's 0xBB pattern.
	if size := readFileSize(t, conn3, 1002, "race.bin"); size != fileSize {
		t.Errorf("file size: got=%d want=%d", size, fileSize)
	}
}

// TestConcurrentWrite_ReaderVsWriter exposes the reader-vs-writer race.
// Without atomic rename, os.Create in PutObject truncates the file
// immediately. A concurrent reader can os.Open after the truncate but before
// any data is written — seeing fileSize=0 — which fio reports as full resid
// EIO.
func TestConcurrentWrite_ReaderVsWriter(t *testing.T) {
	addr, _ := startTestNFS4Server(t)

	conn0, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn0.Close()

	const (
		chunkSize  = 64 * 1024
		iterations = 100
	)

	chunk := bytes.Repeat([]byte{0xCD}, chunkSize)
	createAndWriteFile(t, conn0, 10, "rw.bin", chunk)

	connW, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer connW.Close()
	connR, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer connR.Close()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var shortReads int

	var start sync.WaitGroup
	start.Add(2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		start.Done()
		start.Wait()
		for i := 0; i < iterations; i++ {
			st := writeAt(t, connW, uint32(100+i), "rw.bin", 0, chunk)
			if st != uint32(NFS4_OK) {
				mu.Lock()
				shortReads++ // reuse counter for any failure
				mu.Unlock()
			}
		}
	}()
	go func() {
		defer wg.Done()
		start.Done()
		start.Wait()
		for i := 0; i < iterations; i++ {
			data := readFileBytes(t, connR, uint32(200+i), "rw.bin", 0, chunkSize)
			if len(data) != chunkSize {
				mu.Lock()
				shortReads++
				mu.Unlock()
				continue
			}
			// Content should be 0xCD throughout. If we caught a torn read,
			// we'd see zero-fill or partial data.
			for j := 0; j < len(data); j++ {
				if data[j] != 0xCD {
					mu.Lock()
					shortReads++
					mu.Unlock()
					break
				}
			}
		}
	}()
	wg.Wait()

	if shortReads > 0 {
		t.Errorf("detected %d short or torn reads out of %d iterations — reader saw a writer-truncated file",
			shortReads, iterations)
	}
}
