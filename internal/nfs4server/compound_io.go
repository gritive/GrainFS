package nfs4server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/storage"
)

func (d *Dispatcher) opRead(data []byte) OpResult {
	if d.anonRejected(d.currentFH) {
		return OpResult{OpCode: OpRead, Status: NFS4ERR_ACCESS}
	}
	if d.backend == nil || len(data) < 28 {
		return OpResult{OpCode: OpRead, Status: NFS4ERR_SERVERFAULT}
	}

	// Skip stateid (16), read offset (8) + count (4)
	offset := binary.BigEndian.Uint64(data[16:24])
	count := binary.BigEndian.Uint32(data[24:28])

	bucket, key := extractBucketAndKey(d.currentPath)
	if d.writeBuffer != nil {
		data, hit, err := d.writeBuffer.Read(context.Background(), bucket, key, offset, count)
		if err != nil {
			return OpResult{OpCode: OpRead, Status: NFS4ERR_IO}
		}
		if hit {
			eof := uint64(len(data)) < uint64(count)
			return OpResult{OpCode: OpRead, Status: NFS4_OK, readData: data, readEOF: eof}
		}
	}
	// Fast path: pread(2) directly — skips HeadObject + GetObject + Seek.
	if ra, ok := partialIOBackend(d.backend); ok && preferReadAt(d.backend, bucket) {
		var buf []byte
		var pooled bool
		if count <= nfsMaxReadBlock {
			buf = opReadAtBufPool.Get()
			buf = buf[:count]
			pooled = true
		} else {
			buf = make([]byte, count)
		}
		n, err := ra.ReadAt(context.Background(), bucket, key, int64(offset), buf)
		if err != nil && !errors.Is(err, io.EOF) {
			if pooled {
				opReadAtBufPool.Put(buf[:nfsMaxReadBlock])
			}
			if os.IsNotExist(err) {
				return OpResult{OpCode: OpRead, Status: NFS4ERR_NOENT}
			}
			return OpResult{OpCode: OpRead, Status: NFS4ERR_IO}
		}
		eof := errors.Is(err, io.EOF) || n < int(count)
		result := OpResult{OpCode: OpRead, Status: NFS4_OK, readData: buf[:n], readEOF: eof}
		if pooled {
			result.readPoolSize = nfsMaxReadBlock
		}
		return result
	}

	// Slow path: HeadObject + GetObject + Seek (backends without ReadAt).
	obj, err := d.backend.HeadObject(context.Background(), bucket, key)
	if err != nil {
		return OpResult{OpCode: OpRead, Status: NFS4ERR_NOENT}
	}
	fileSize := obj.Size

	if offset >= uint64(fileSize) {
		w := getXDRWriter()
		w.WriteUint32(1) // eof = TRUE
		w.WriteOpaque(nil)
		return OpResult{OpCode: OpRead, Status: NFS4_OK, Data: xdrWriterBytes(w)}
	}
	remainingSize := fileSize - int64(offset)

	if uint64(remainingSize) > uint64(count) {
		remainingSize = int64(count)
	}

	rc, _, err := d.backend.GetObject(context.Background(), bucket, key)
	if err != nil {
		return OpResult{OpCode: OpRead, Status: NFS4ERR_NOENT}
	}
	defer rc.Close()

	if offset > 0 {
		if seeker, ok := rc.(io.Seeker); ok {
			_, err = seeker.Seek(int64(offset), io.SeekStart)
			if err != nil {
				rc.Close()
				return OpResult{OpCode: OpRead, Status: NFS4ERR_IO}
			}
		} else {
			discard := make([]byte, 4096)
			remainingToSkip := int64(offset)
			for remainingToSkip > 0 {
				toSkip := min(remainingToSkip, int64(len(discard)))
				n, err := rc.Read(discard[:toSkip])
				if err != nil {
					rc.Close()
					return OpResult{OpCode: OpRead, Status: NFS4ERR_IO}
				}
				remainingToSkip -= int64(n)
			}
		}
	}

	buffer := opReadBufPool.Get()
	buffer.Reset()
	defer func() { buffer.Reset(); opReadBufPool.Put(buffer) }()
	_, err = bufferedCopy(buffer, rc, remainingSize)
	if err != nil {
		return OpResult{OpCode: OpRead, Status: NFS4ERR_IO}
	}

	readData := buffer.Bytes()
	if uint32(len(readData)) > count {
		readData = readData[:count]
	}

	eof := (offset + uint64(len(readData))) >= uint64(fileSize)

	w := getXDRWriter()
	w.Grow(4 + opaqueEncodedSize(len(readData)))
	w.WriteUint32(boolToUint32(eof))
	w.WriteOpaque(readData)

	return OpResult{OpCode: OpRead, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

func (d *Dispatcher) opWrite(data []byte) OpResult {
	if d.anonRejected(d.currentFH) {
		return OpResult{OpCode: OpWrite, Status: NFS4ERR_ACCESS}
	}
	if d.backend == nil || len(data) < 28 {
		return OpResult{OpCode: OpWrite, Status: NFS4ERR_SERVERFAULT}
	}
	if d.isPathReadOnly(d.currentPath) {
		return OpResult{OpCode: OpWrite, Status: NFS4ERR_ROFS}
	}

	r := NewXDRReader(data[16:]) // skip stateid (16 bytes)
	offset, _ := r.ReadUint64()
	r.ReadUint32() // stable
	writeData, err := r.ReadOpaqueView()
	if err != nil {
		return OpResult{OpCode: OpWrite, Status: NFS4ERR_IO}
	}

	const maxWriteSize = 1 * 1024 * 1024
	if len(writeData) > maxWriteSize {
		return OpResult{OpCode: OpWrite, Status: NFS4ERR_FBIG}
	}
	log.Debug().Str("path", d.currentPath).Uint64("offset", offset).Int("len", len(writeData)).Msg("nfs4: WRITE")

	bucket, key := extractBucketAndKey(d.currentPath)

	// Serialise all writes to the same path. Without this, an offset=0
	// PutObject can run concurrently with an offset>0 RMW, clobbering data.
	release := d.state.LockPath(objectLockKey(bucket, key))
	defer release()

	if d.writeBuffer != nil {
		// WriteBuffer is the source of truth for this key while wired —
		// it must preempt both the WriteAt fast path and the RMW fallback
		// so concurrent reads/truncates see consistent state and coalescing
		// applies regardless of backend capabilities (plan §D8).
		existingContentType := "application/octet-stream"
		if obj, herr := d.backend.HeadObject(context.Background(), bucket, key); herr == nil {
			if obj.ContentType != "" {
				existingContentType = obj.ContentType
			}
		}
		if err = d.writeBuffer.Write(context.Background(), bucket, key, offset, writeData, existingContentType); err != nil {
			return OpResult{OpCode: OpWrite, Status: NFS4ERR_IO}
		}
	} else if wa, ok := partialIOBackend(d.backend); ok && preferWriteAt(d.backend, bucket) {
		// Fast path: stream prefix+data+suffix via kernel I/O — no heap allocation.
		if _, err = wa.WriteAt(context.Background(), bucket, key, offset, writeData); err != nil {
			return OpResult{OpCode: OpWrite, Status: NFS4ERR_IO}
		}
	} else {
		// Fallback: RMW for backends that don't implement WriteAt.
		// Capture existingSize and contentType in a single HeadObject call.
		var existingSize uint64
		existingContentType := "application/octet-stream"
		if obj, herr := d.backend.HeadObject(context.Background(), bucket, key); herr == nil {
			existingSize = uint64(obj.Size)
			if obj.ContentType != "" {
				existingContentType = obj.ContentType
			}
		}
		writeLen := uint64(len(writeData))
		if offset > maxInt64Uint || writeLen > maxInt64Uint-offset {
			return OpResult{OpCode: OpWrite, Status: NFS4ERR_FBIG}
		}
		end := offset + writeLen
		if offset > existingSize && end > nfsMaxFallbackSparseSize {
			return OpResult{OpCode: OpWrite, Status: NFS4ERR_FBIG}
		}

		br := bytesReaderPool.Get()
		defer bytesReaderPool.Put(br)

		if offset == 0 && end >= existingSize {
			br.Reset(writeData)
			_, err = d.backend.PutObject(context.Background(), bucket, key, br, existingContentType)
		} else {
			var ra storage.PartialIO
			if partial, ok := partialIOBackend(d.backend); ok && preferReadAt(d.backend, bucket) {
				ra = partial
			}
			err = d.putObjectRMWStreaming(context.Background(), bucket, key, offset, writeData, existingSize, existingContentType, ra)
		}
		if err != nil {
			return OpResult{OpCode: OpWrite, Status: NFS4ERR_IO}
		}
	}
	d.state.InvalidateObject(bucket, key)

	w := getXDRWriter()
	w.WriteUint32(uint32(len(writeData))) // count
	w.WriteUint32(2)                      // committed = FILE_SYNC
	w.WriteUint64(0)                      // writeverf (8 bytes)
	return OpResult{OpCode: OpWrite, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

// resolveContentType returns the existing object's Content-Type, or
// "application/octet-stream" for new objects. It is used at write sites that do
// not have a prior HeadObject result available (SETATTR truncate). One
// HeadObject RTT is acceptable because these paths are not on the hot data
// path (they imply a full object rewrite anyway).

func (d *Dispatcher) opOpen(data []byte) OpResult {
	if d.anonRejected(d.currentFH) {
		return OpResult{OpCode: OpOpen, Status: NFS4ERR_ACCESS}
	}
	if len(data) < 8 {
		return OpResult{OpCode: OpOpen, Status: NFS4ERR_INVAL}
	}

	// data is pre-processed by readOpArgs(xdr.go): shareAccess(4) + openType(4) + fileName(string)
	r := NewXDRReader(data)
	shareAccess, _ := r.ReadUint32()
	openType, _ := r.ReadUint32() // 0=NOCREATE, 1=CREATE
	fileName, _ := r.ReadString()
	if err := validateComponentName(fileName); err != nil {
		return OpResult{OpCode: OpOpen, Status: NFS4ERR_INVAL}
	}
	if openType == 1 && d.isPathReadOnly(d.currentPath) {
		return OpResult{OpCode: OpOpen, Status: NFS4ERR_ROFS}
	}

	childPath := path.Join(d.currentPath, fileName)
	log.Debug().Str("file", fileName).Str("child", childPath).Uint32("access", shareAccess).Uint32("type", openType).Msg("nfs4: OPEN")

	// If CREATE, ensure the object exists
	if openType == 1 && d.backend != nil {
		bucket, key := extractBucketAndKey(childPath)
		_, headErr := d.backend.HeadObject(context.Background(), bucket, key)
		if headErr != nil {
			created := false
			if tr, ok := truncatableBackend(d.backend); ok {
				if truncErr := tr.Truncate(context.Background(), bucket, key, 0); truncErr == nil {
					created = true
				} else {
					log.Debug().Err(truncErr).Str("key", key).Msg("nfs4: OPEN CREATE truncate fallback")
				}
			}
			if !created {
				if _, putErr := d.backend.PutObject(context.Background(), bucket, key, bytes.NewReader(nil), "application/octet-stream"); putErr != nil {
					return OpResult{OpCode: OpOpen, Status: NFS4ERR_IO}
				}
			}
			d.state.InvalidateObject(bucket, key)
			log.Debug().Str("key", key).Msg("nfs4: OPEN CREATE created file")
		}
	}

	fh := d.state.GetOrCreateFH(childPath)
	if d.server != nil {
		bucket, _ := extractBucketAndKey(childPath)
		gen := d.server.exportGeneration(bucket)
		// T12: propagate parent's saID so the new fh inherits the session
		// binding (anon "" vs mount-SA "<name>").
		parentBind, ok := d.state.FHBinding(d.currentFH)
		if ok && parentBind.saID != "" && parentBind.saID != fhSAIDPending {
			d.state.BindFHWithBinding(fh, bucket, parentBind.saID, parentBind.readOnly, gen)
		} else {
			d.state.BindFHGeneration(fh, bucket, gen)
		}
	}
	d.currentFH = fh
	d.currentPath = childPath

	// Generate a stateid
	stateID := d.state.nextStateID.Add(1) - 1

	w := getXDRWriter()
	// stateid (seqid:4 + other:12)
	w.WriteUint32(1) // seqid
	w.WriteUint64(stateID)
	w.WriteUint32(0) // padding to 12 bytes
	// cinfo (atomic:bool + before:changeid + after:changeid)
	w.WriteUint32(1) // atomic = TRUE
	w.WriteUint64(uint64(time.Now().UnixNano()))
	w.WriteUint64(uint64(time.Now().UnixNano()))
	// rflags
	w.WriteUint32(4) // OPEN4_RESULT_LOCKTYPE_POSIX
	// bitmap of attrs set (empty)
	w.WriteUint32(0)
	// delegation type = OPEN_DELEGATE_NONE
	w.WriteUint32(0)

	return OpResult{OpCode: OpOpen, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

func (d *Dispatcher) opOpenConfirm(_ []byte) OpResult {
	w := getXDRWriter()
	// Return the same stateid
	w.WriteUint32(1) // seqid
	w.WriteUint64(0)
	w.WriteUint32(0)
	return OpResult{OpCode: OpOpenConfirm, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

func (d *Dispatcher) opClose(_ []byte) OpResult {
	w := getXDRWriter()
	// Return zeroed stateid
	w.WriteUint32(0)
	w.WriteUint64(0)
	w.WriteUint32(0)
	return OpResult{OpCode: OpClose, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

// opCommit translates the NFSv4 COMMIT op into a backend Sync request.
// Backends that own a data WAL implement Syncable.Sync as a WAL flush, so
// COMMIT returns once the WAL is durable (not once the object file is
// fsynced).
func (d *Dispatcher) opCommit() OpResult {
	if d.currentPath == "" {
		return OpResult{OpCode: OpCommit, Status: NFS4ERR_NOFILEHANDLE}
	}
	bucket, key := extractBucketAndKey(d.currentPath)
	if d.writeBuffer != nil {
		if err := d.writeBuffer.Flush(context.Background(), bucket, key); err != nil {
			return OpResult{OpCode: OpCommit, Status: NFS4ERR_IO}
		}
	}
	if s, ok := d.backend.(storage.Syncable); ok {
		if err := s.Sync(bucket, key); err != nil {
			return OpResult{OpCode: OpCommit, Status: NFS4ERR_IO}
		}
	}
	w := getXDRWriter()
	w.buf.Write(d.state.WriteVerf[:]) // writeverf4: 8 bytes
	return OpResult{OpCode: OpCommit, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}
