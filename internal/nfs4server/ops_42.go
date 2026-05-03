package nfs4server

import (
	"bytes"
	"context"
	"io"
	"math"

	"github.com/gritive/GrainFS/internal/storage"
)

const (
	seek4WData = 0 // NFS4_CONTENT_DATA
	seek4WHole = 1 // NFS4_CONTENT_HOLE

	// maxObjectBytes caps read-modify-write ops so object storage backends
	// don't OOM on huge files. Clients get NFS4ERR_FBIG/NFS4ERR_NOTSUPP for
	// files exceeding this limit; SETATTR/WRITE are not affected.
	maxObjectBytes = 64 << 20 // 64 MB
)

// opSeek handles SEEK (op 69, RFC 7862 §15.11).
// GrainFS has no sparse-file holes: DATA whence returns the requested offset,
// HOLE whence reports EOF (offset = file size).
func (d *Dispatcher) opSeek(data []byte) OpResult {
	if len(data) < 28 {
		return OpResult{OpCode: OpSeek, Status: NFS4ERR_INVAL}
	}
	r := NewXDRReader(data)
	r.ReadFixed(16) //nolint:errcheck // stateid
	offset, _ := r.ReadUint64()
	whence, _ := r.ReadUint32()

	if d.currentPath == "" {
		return OpResult{OpCode: OpSeek, Status: NFS4ERR_NOFILEHANDLE}
	}

	key := pathToKey(d.currentPath)
	var fileSize int64
	if d.backend != nil {
		if info, err := d.backend.HeadObject(context.Background(), nfs4Bucket, key); err == nil {
			fileSize = info.Size
		}
	}

	var resEOF uint32
	var resOffset uint64

	switch whence {
	case seek4WData:
		if offset >= uint64(fileSize) {
			resEOF, resOffset = 1, uint64(fileSize)
		} else {
			resEOF, resOffset = 0, offset
		}
	case seek4WHole:
		resEOF, resOffset = 1, uint64(fileSize)
	default:
		return OpResult{OpCode: OpSeek, Status: NFS4ERR_INVAL}
	}

	w := getXDRWriter()
	w.WriteUint32(resEOF)
	w.WriteUint64(resOffset)
	return OpResult{OpCode: OpSeek, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

// opAllocate handles ALLOCATE (op 59, RFC 7862 §15.1).
// RFC requires that after ALLOCATE, the file size MUST be at least offset+length.
func (d *Dispatcher) opAllocate(data []byte) OpResult {
	if len(data) < 32 {
		return OpResult{OpCode: OpAllocate, Status: NFS4ERR_INVAL}
	}
	if d.currentPath == "" {
		return OpResult{OpCode: OpAllocate, Status: NFS4ERR_NOFILEHANDLE}
	}
	r := NewXDRReader(data)
	r.ReadFixed(16) //nolint:errcheck // stateid
	offset, _ := r.ReadUint64()
	length, _ := r.ReadUint64()

	key := pathToKey(d.currentPath)
	if d.backend == nil {
		return OpResult{OpCode: OpAllocate, Status: NFS4_OK}
	}

	if offset > math.MaxInt64 || length > uint64(math.MaxInt64)-offset {
		return OpResult{OpCode: OpAllocate, Status: NFS4ERR_FBIG}
	}
	required := int64(offset) + int64(length)
	if required > maxObjectBytes {
		return OpResult{OpCode: OpAllocate, Status: NFS4ERR_FBIG}
	}
	release := d.state.LockPath(d.currentPath)
	defer release()

	// Recheck size inside the lock to avoid TOCTOU: another writer may have
	// extended the file between an earlier HeadObject and now.
	if info, err := d.backend.HeadObject(context.Background(), nfs4Bucket, key); err == nil && info.Size >= required {
		return OpResult{OpCode: OpAllocate, Status: NFS4_OK}
	}

	if tr, ok := d.backend.(storage.Truncatable); ok {
		if err := tr.Truncate(context.Background(), nfs4Bucket, key, required); err != nil {
			return OpResult{OpCode: OpAllocate, Status: NFS4ERR_IO}
		}
		return OpResult{OpCode: OpAllocate, Status: NFS4_OK}
	}

	var existing []byte
	if body, _, err := d.backend.GetObject(context.Background(), nfs4Bucket, key); err == nil {
		existing, err = io.ReadAll(body)
		body.Close()
		if err != nil {
			return OpResult{OpCode: OpAllocate, Status: NFS4ERR_IO}
		}
	}
	if int64(len(existing)) < required {
		existing = append(existing, make([]byte, required-int64(len(existing)))...)
	}
	if _, err := d.backend.PutObject(context.Background(), nfs4Bucket, key, bytes.NewReader(existing), "application/octet-stream"); err != nil {
		return OpResult{OpCode: OpAllocate, Status: NFS4ERR_IO}
	}
	return OpResult{OpCode: OpAllocate, Status: NFS4_OK}
}

// opDeallocate handles DEALLOCATE (op 62, RFC 7862 §15.3).
// Punches holes by zeroing the byte range via read-modify-write.
func (d *Dispatcher) opDeallocate(data []byte) OpResult {
	if len(data) < 32 {
		return OpResult{OpCode: OpDeallocate, Status: NFS4ERR_INVAL}
	}
	if d.currentPath == "" {
		return OpResult{OpCode: OpDeallocate, Status: NFS4ERR_NOFILEHANDLE}
	}
	r := NewXDRReader(data)
	r.ReadFixed(16) //nolint:errcheck // stateid
	offset, _ := r.ReadUint64()
	length, _ := r.ReadUint64()

	key := pathToKey(d.currentPath)
	if d.backend == nil {
		return OpResult{OpCode: OpDeallocate, Status: NFS4_OK}
	}

	if info, err := d.backend.HeadObject(context.Background(), nfs4Bucket, key); err == nil && info.Size > maxObjectBytes {
		return OpResult{OpCode: OpDeallocate, Status: NFS4ERR_NOTSUPP}
	}

	release := d.state.LockPath(d.currentPath)
	defer release()

	body, _, err := d.backend.GetObject(context.Background(), nfs4Bucket, key)
	if err != nil {
		return OpResult{OpCode: OpDeallocate, Status: NFS4ERR_IO}
	}
	defer body.Close()

	current, err := io.ReadAll(body)
	if err != nil {
		return OpResult{OpCode: OpDeallocate, Status: NFS4ERR_IO}
	}

	end := offset + length
	if end < offset { // uint64 overflow: clamp to file size
		end = uint64(len(current))
	}
	if end > uint64(len(current)) {
		end = uint64(len(current))
	}
	if offset < uint64(len(current)) {
		zeros := make([]byte, end-offset)
		copy(current[offset:end], zeros)
	}

	if _, err := d.backend.PutObject(context.Background(), nfs4Bucket, key, bytes.NewReader(current), "application/octet-stream"); err != nil {
		return OpResult{OpCode: OpDeallocate, Status: NFS4ERR_IO}
	}
	return OpResult{OpCode: OpDeallocate, Status: NFS4_OK}
}

// opCopy handles COPY (op 60, RFC 7862 §15.2).
// Server-side copy from saved FH path to current FH path.
func (d *Dispatcher) opCopy(data []byte) OpResult {
	if len(data) < 56 {
		return OpResult{OpCode: OpCopy, Status: NFS4ERR_INVAL}
	}
	if d.currentPath == "" || d.savedPath == "" {
		return OpResult{OpCode: OpCopy, Status: NFS4ERR_NOFILEHANDLE}
	}
	r := NewXDRReader(data)
	r.ReadFixed(16) //nolint:errcheck // src stateid
	r.ReadFixed(16) //nolint:errcheck // dst stateid
	r.ReadUint64()  // src_offset (whole-file copy only)
	r.ReadUint64()  // dst_offset
	r.ReadUint64()  // count

	srcKey := pathToKey(d.savedPath)
	dstKey := pathToKey(d.currentPath)

	if d.backend == nil {
		return OpResult{OpCode: OpCopy, Status: NFS4_OK}
	}

	releaseSrc := d.state.LockPath(d.savedPath)
	srcBody, _, err := d.backend.GetObject(context.Background(), nfs4Bucket, srcKey)
	if err != nil {
		releaseSrc()
		return OpResult{OpCode: OpCopy, Status: NFS4ERR_IO}
	}
	srcData, err := io.ReadAll(srcBody)
	srcBody.Close()
	releaseSrc()
	if err != nil {
		return OpResult{OpCode: OpCopy, Status: NFS4ERR_IO}
	}

	release := d.state.LockPath(d.currentPath)
	defer release()

	if _, err := d.backend.PutObject(context.Background(), nfs4Bucket, dstKey, bytes.NewReader(srcData), "application/octet-stream"); err != nil {
		return OpResult{OpCode: OpCopy, Status: NFS4ERR_IO}
	}

	// RFC 7862 §15.2: return COPY4resok { write_response4, cr_consecutive, cr_synchronous }
	// write_response4: wr_callback_id<1>=[] + wr_bytes_written + wr_stable + wr_writeverf(8)
	w := getXDRWriter()
	w.WriteUint32(0)                    // wr_callback_id count = 0 (synchronous)
	w.WriteUint64(uint64(len(srcData))) // wr_bytes_written
	w.WriteUint32(2)                    // wr_stable = FILE_SYNC
	w.buf.Write(d.state.WriteVerf[:])   // wr_writeverf (8 bytes)
	w.WriteUint32(1)                    // cr_consecutive = TRUE
	w.WriteUint32(1)                    // cr_synchronous = TRUE
	return OpResult{OpCode: OpCopy, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

// opIOAdvise handles IO_ADVISE (op 63, RFC 7862 §15.6).
// GrainFS ignores hints and returns an empty hint bitmask.
func (d *Dispatcher) opIOAdvise(_ []byte) OpResult {
	if d.currentPath == "" {
		return OpResult{OpCode: OpIOAdvise, Status: NFS4ERR_NOFILEHANDLE}
	}
	w := getXDRWriter()
	w.WriteUint32(1) // bitmap length = 1
	w.WriteUint32(0) // no hints honored
	return OpResult{OpCode: OpIOAdvise, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

// pathToKey strips the leading "/" from NFS paths to form storage keys.
func pathToKey(p string) string {
	if len(p) > 0 && p[0] == '/' {
		return p[1:]
	}
	return p
}
