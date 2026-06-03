package nfs4server

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"path"
	"strings"
	"time"
)

func (d *Dispatcher) opGetAttr(data []byte) OpResult {
	if d.anonRejected(d.currentFH) {
		return OpResult{OpCode: OpGetAttr, Status: NFS4ERR_ACCESS}
	}
	// Parse client's requested bitmap (GETATTR4args.attr_request = bitmap4<>)
	var reqBit attrBitmap
	if len(data) >= 4 {
		reqBit = readAttrBitmap(NewXDRReader(data))
	}

	return OpResult{OpCode: OpGetAttr, Status: NFS4_OK, Data: d.encodeAttrs(d.currentPath, reqBit)}
}

func (d *Dispatcher) opAccess(data []byte) OpResult {
	if d.anonRejected(d.currentFH) {
		return OpResult{OpCode: OpAccess, Status: NFS4ERR_ACCESS}
	}
	var requested uint32
	if len(data) >= 4 {
		requested = binary.BigEndian.Uint32(data)
	}
	w := getXDRWriter()
	w.WriteUint32(0)         // supported
	w.WriteUint32(requested) // access (grant all requested)
	return OpResult{OpCode: OpAccess, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

func (d *Dispatcher) opReadDir(data []byte) OpResult {
	if d.anonRejected(d.currentFH) {
		return OpResult{OpCode: OpReadDir, Status: NFS4ERR_ACCESS}
	}
	var reqBit attrBitmap
	if len(data) >= 28 {
		bitmapLen := binary.BigEndian.Uint32(data[24:28])
		for i := uint32(0); i < bitmapLen; i++ {
			off := 28 + int(i)*4
			if off+4 > len(data) {
				break
			}
			if i < uint32(len(reqBit)) {
				reqBit[i] = binary.BigEndian.Uint32(data[off : off+4])
			}
		}
	}

	w := getXDRWriter()

	// cookieverf (8 bytes)
	if d.currentPath == "/" && d.server != nil {
		w.WriteUint64(d.server.loadExports().verifier)
		snap := d.server.loadExports()
		cookie := uint64(0)
		for _, name := range snap.sortedNames {
			cookie++
			w.WriteUint32(1)
			w.WriteUint64(cookie)
			w.WriteString(name)
			w.buf.Write(d.encodeAttrs(path.Join(d.currentPath, name), reqBit))
		}
		w.WriteUint32(0)
		w.WriteUint32(1)
		return OpResult{OpCode: OpReadDir, Status: NFS4_OK, Data: xdrWriterBytes(w)}
	}

	w.WriteUint64(0)

	if d.backend != nil {
		bucket, prefix := extractBucketAndKey(d.currentPath)
		if prefix == "" {
			// root lists all keys; no change needed
		} else if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}

		objects, _ := d.backend.ListObjects(context.Background(), bucket, prefix, 1000)
		cookie := uint64(0)
		for _, obj := range objects {
			name := obj.Key
			if prefix != "" {
				name = name[len(prefix):]
			}
			// Skip sub-directory entries (contain '/')
			if strings.ContainsRune(name, '/') {
				continue
			}
			cookie++
			w.WriteUint32(1) // value_follows = TRUE
			w.WriteUint64(cookie)
			w.WriteString(name)
			w.buf.Write(d.encodeAttrsWithObject(path.Join(d.currentPath, name), reqBit, obj))
		}
	}

	w.WriteUint32(0) // value_follows = FALSE (end of entries)
	w.WriteUint32(1) // eof = TRUE

	return OpResult{OpCode: OpReadDir, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

func (d *Dispatcher) opSetAttr(data []byte) OpResult {
	if d.anonRejected(d.currentFH) {
		return OpResult{OpCode: OpSetAttr, Status: NFS4ERR_ACCESS}
	}
	if d.currentPath == "" {
		return OpResult{OpCode: OpSetAttr, Status: NFS4ERR_NOFILEHANDLE}
	}
	if d.isPathReadOnly(d.currentPath) {
		return OpResult{OpCode: OpSetAttr, Status: NFS4ERR_ROFS}
	}
	// data layout: stateid(16) + bm0(4) + bm1(4) + attrVals(opaque)
	if len(data) < 24 {
		return OpResult{OpCode: OpSetAttr, Status: NFS4ERR_INVAL}
	}
	r := NewXDRReader(data[16:]) // skip stateid
	bm0, _ := r.ReadUint32()
	bm1, _ := r.ReadUint32()
	attrVals, _ := r.ReadOpaque()

	bucket, key := extractBucketAndKey(d.currentPath)

	ar := NewXDRReader(attrVals)

	// FATTR4_SIZE (word0 bit 4)
	if bm0&(1<<4) != 0 {
		size, _ := ar.ReadUint64()
		release := d.state.LockPath(objectLockKey(bucket, key))
		defer release()
		if tr, ok := truncatableBackend(d.backend); ok && preferWriteAt(d.backend, bucket) {
			if d.writeBuffer != nil {
				if err := d.writeBuffer.Discard(context.Background(), bucket, key); err != nil {
					return OpResult{OpCode: OpSetAttr, Status: NFS4ERR_IO}
				}
			}
			if err := tr.Truncate(context.Background(), bucket, key, int64(size)); err != nil {
				return OpResult{OpCode: OpSetAttr, Status: NFS4ERR_IO}
			}
		} else {
			ct := d.resolveContentType(context.Background(), bucket, key)
			var existing []byte
			if rc, _, err := d.backend.GetObject(context.Background(), bucket, key); err == nil {
				existing, _ = io.ReadAll(rc)
				rc.Close()
			}
			sz := int64(size)
			cur := int64(len(existing))
			if cur > sz {
				existing = existing[:sz]
			} else if cur < sz {
				existing = append(existing, make([]byte, sz-cur)...)
			}
			if _, err := d.backend.PutObject(context.Background(), bucket, key, bytes.NewReader(existing), ct); err != nil {
				return OpResult{OpCode: OpSetAttr, Status: NFS4ERR_IO}
			}
		}
		d.state.InvalidateObject(bucket, key)
	}

	meta := d.loadFileMeta(bucket, key)
	metaChanged := false

	// FATTR4_MODE (word1 bit 1 = bit 33)
	if bm1&(1<<1) != 0 {
		mode, _ := ar.ReadUint32()
		meta.Mode = mode
		metaChanged = true
	}

	// FATTR4_TIME_ACCESS_SET (word1 bit 16 = attr 48) — consume bytes to keep reader aligned; atime not stored
	if bm1&(1<<16) != 0 {
		how, _ := ar.ReadUint32()
		if how == 1 { // SET_TO_CLIENT_TIME4: skip nfstime4 (hi+lo+nsecs)
			ar.ReadUint32()
			ar.ReadUint32()
			ar.ReadUint32()
		}
	}

	// FATTR4_TIME_MODIFY_SET (word1 bit 22 = attr 54)
	if bm1&(1<<22) != 0 {
		how, _ := ar.ReadUint32()
		var mtimeNs int64
		if how == 1 { // SET_TO_CLIENT_TIME4
			hi, _ := ar.ReadUint32()
			lo, _ := ar.ReadUint32()
			secs := int64(hi)<<32 | int64(lo)
			nsecs, _ := ar.ReadUint32()
			mtimeNs = secs*1e9 + int64(nsecs)
		} else { // SET_TO_SERVER_TIME4
			mtimeNs = time.Now().UnixNano()
		}
		meta.Mtime = mtimeNs
		metaChanged = true
	}

	if metaChanged {
		if err := d.saveFileMeta(bucket, key, meta); err != nil {
			return OpResult{OpCode: OpSetAttr, Status: NFS4ERR_IO}
		}
		d.state.InvalidateObject(bucket, key)
	}

	return OpResult{OpCode: OpSetAttr, Status: NFS4_OK, Data: encodeSetAttrResult(bm0, bm1)}
}
