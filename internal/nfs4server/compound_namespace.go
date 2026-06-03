package nfs4server

import (
	"context"
	"io"
	"path"
	"time"

	"github.com/rs/zerolog/log"
)

func (d *Dispatcher) opLookup(data []byte) OpResult {
	if d.anonRejected(d.currentFH) {
		return OpResult{OpCode: OpLookup, Status: NFS4ERR_ACCESS}
	}
	name := string(data)
	if err := validateComponentName(name); err != nil {
		return OpResult{OpCode: OpLookup, Status: NFS4ERR_INVAL}
	}
	childPath := path.Join(d.currentPath, name)
	childBucket, childKey := extractBucketAndKey(childPath)

	// NFS§B T8: 2-phase lazy binding (spec D#5).
	// When the IAM gate is wired (server.mountSAStore != nil) and the current fh
	// is a bucket-level pending fh (saID="(pending)"), this LOOKUP is the
	// resolution step: determine if 'name' is a mount-SA or an anon file path.
	if d.server != nil && (d.server.mountSAStore != nil || d.server.protocolCredentials != nil) {
		if binding, ok := d.state.FHBinding(d.currentFH); ok && binding.saID == fhSAIDPending {
			return d.opLookupResolvePending(name, childPath, childBucket, childKey, binding)
		}
	}

	// Check existence: backend file, tracked directory, or root
	exists := d.state.IsDir(childPath)
	if !exists && d.currentPath == "/" && childKey == "" && d.server != nil {
		exists = d.server.isExportRegistered(childBucket)
		if !exists {
			if d.server.lookups != nil {
				d.server.lookups.Record(LookupRecord{
					Client: d.clientAddr,
					Bucket: childBucket,
					Result: "unknown_bucket",
				})
			}
			if d.hinter != nil {
				d.hinter.emit(d.clientAddr, childBucket)
			}
			log.Debug().Str("name", name).Str("child", childPath).Bool("exists", false).Msg("nfs4: LOOKUP")
			return OpResult{OpCode: OpLookup, Status: NFS4ERR_NOENT}
		}
		if d.server.lookups != nil {
			d.server.lookups.Record(LookupRecord{
				Client: d.clientAddr,
				Bucket: childBucket,
				Result: "ok",
			})
		}
	}
	if !exists && d.backend != nil {
		_, err := d.backend.HeadObject(context.Background(), childBucket, childKey)
		exists = err == nil
	}
	log.Debug().Str("name", name).Str("child", childPath).Bool("exists", exists).Msg("nfs4: LOOKUP")
	if !exists {
		return OpResult{OpCode: OpLookup, Status: NFS4ERR_NOENT}
	}

	fh := d.state.GetOrCreateFH(childPath)
	if d.server != nil && childBucket != "" {
		gen := d.server.exportGeneration(childBucket)
		// When mount-SA store is wired: issue bucket fh with saID="(pending)" so the
		// next LOOKUP can resolve whether this is a mount-SA path or anon access.
		if (d.server.mountSAStore != nil || d.server.protocolCredentials != nil) && childKey == "" {
			d.state.BindFHWithSAID(fh, childBucket, fhSAIDPending, gen)
		} else {
			// T12 propagation fix: a fresh subdir fh must inherit the parent's
			// saID binding so mount-SA-bound subdir sessions stay bound to the
			// same principal.
			// BindFHGeneration preserves an existing saID, but a freshly-
			// created fh has saID="" which would mis-classify as anon. Pull
			// parent's saID explicitly when it is set.
			parentBind, ok := d.state.FHBinding(d.currentFH)
			if ok && parentBind.saID != "" && parentBind.saID != fhSAIDPending {
				d.state.BindFHWithBinding(fh, childBucket, parentBind.saID, parentBind.readOnly, gen)
			} else {
				d.state.BindFHGeneration(fh, childBucket, gen)
			}
		}
	}
	d.currentFH = fh
	d.currentPath = childPath
	return OpResult{OpCode: OpLookup, Status: NFS4_OK}
}

func (d *Dispatcher) opCreate(data []byte) OpResult {
	if d.anonRejected(d.currentFH) {
		return OpResult{OpCode: OpCreate, Status: NFS4ERR_ACCESS}
	}
	// data pre-processed by readOpArgs: objType(uint32) + objname(string)
	if len(data) < 8 {
		return OpResult{OpCode: OpCreate, Status: NFS4ERR_INVAL}
	}
	r := NewXDRReader(data)
	objType, _ := r.ReadUint32()
	objName, _ := r.ReadString()
	if err := validateComponentName(objName); err != nil {
		return OpResult{OpCode: OpCreate, Status: NFS4ERR_INVAL}
	}
	if d.isPathReadOnly(d.currentPath) {
		return OpResult{OpCode: OpCreate, Status: NFS4ERR_ROFS}
	}

	newPath := path.Join(d.currentPath, objName)

	if objType == NF4DIR {
		d.state.MarkDir(newPath)
		d.invalidatePath(newPath)
	}
	fh := d.state.GetOrCreateFH(newPath)
	if d.server != nil {
		bucket, _ := extractBucketAndKey(newPath)
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
	d.currentPath = newPath

	now := uint64(time.Now().UnixNano())
	w := getXDRWriter()
	// change_info4: atomic + before + after
	w.WriteUint32(1)
	w.WriteUint64(now)
	w.WriteUint64(now)
	// attrset: empty bitmap
	w.WriteUint32(0)
	return OpResult{OpCode: OpCreate, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

func (d *Dispatcher) opRemove(data []byte) OpResult {
	if d.anonRejected(d.currentFH) {
		return OpResult{OpCode: OpRemove, Status: NFS4ERR_ACCESS}
	}
	name := string(data)
	if err := validateComponentName(name); err != nil {
		return OpResult{OpCode: OpRemove, Status: NFS4ERR_INVAL}
	}
	targetPath := path.Join(d.currentPath, name)
	if d.isPathReadOnly(targetPath) {
		return OpResult{OpCode: OpRemove, Status: NFS4ERR_ROFS}
	}
	bucket, key := extractBucketAndKey(targetPath)

	if _, err := d.backend.HeadObject(context.Background(), bucket, key); err != nil {
		// May be a directory tracked in state only.
		if d.state.IsDir(targetPath) {
			d.state.RemoveDir(targetPath)
			d.state.InvalidateFH(targetPath)
			d.invalidatePath(targetPath)
			now := uint64(time.Now().UnixNano())
			w := getXDRWriter()
			w.WriteUint32(1)
			w.WriteUint64(now)
			w.WriteUint64(now)
			return OpResult{OpCode: OpRemove, Status: NFS4_OK, Data: xdrWriterBytes(w)}
		}
		return OpResult{OpCode: OpRemove, Status: NFS4ERR_NOENT}
	}
	if err := d.backend.DeleteObject(context.Background(), bucket, key); err != nil {
		return OpResult{OpCode: OpRemove, Status: NFS4ERR_IO}
	}
	d.state.InvalidateFH(targetPath)
	d.state.InvalidateObject(bucket, key)

	w := getXDRWriter()
	// change_info4: atomic + before + after
	w.WriteUint32(1)
	w.WriteUint64(uint64(time.Now().UnixNano()))
	w.WriteUint64(uint64(time.Now().UnixNano()))
	return OpResult{OpCode: OpRemove, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

func (d *Dispatcher) opRename(data []byte) OpResult {
	// Rename involves both savedFH (source parent) and currentFH (dest parent).
	if d.anonRejected(d.savedFH) || d.anonRejected(d.currentFH) {
		return OpResult{OpCode: OpRename, Status: NFS4ERR_ACCESS}
	}
	r := NewXDRReader(data)
	oldName, err := r.ReadString()
	if err != nil {
		return OpResult{OpCode: OpRename, Status: NFS4ERR_INVAL}
	}
	newName, err := r.ReadString()
	if err != nil {
		return OpResult{OpCode: OpRename, Status: NFS4ERR_INVAL}
	}
	if err := validateComponentName(oldName); err != nil {
		return OpResult{OpCode: OpRename, Status: NFS4ERR_INVAL}
	}
	if err := validateComponentName(newName); err != nil {
		return OpResult{OpCode: OpRename, Status: NFS4ERR_INVAL}
	}

	// Linux mv: SAVEFH(src_dir) → PUTFH(dst_dir) → RENAME(old,new)
	// Use savedPath as source dir, currentPath as target dir.
	srcDir := d.savedPath
	if srcDir == "" {
		srcDir = d.currentPath
	}
	oldPath := path.Join(srcDir, oldName)
	newPath := path.Join(d.currentPath, newName)
	srcBucket, oldKey := extractBucketAndKey(oldPath)
	dstBucket, newKey := extractBucketAndKey(newPath)
	if srcBucket != dstBucket {
		return OpResult{OpCode: OpRename, Status: NFS4ERR_XDEV}
	}
	if d.isPathReadOnly(oldPath) || d.isPathReadOnly(newPath) {
		return OpResult{OpCode: OpRename, Status: NFS4ERR_ROFS}
	}

	// Object store has no rename — read → write new → delete old.
	rc, _, err := d.backend.GetObject(context.Background(), srcBucket, oldKey)
	if err != nil {
		return OpResult{OpCode: OpRename, Status: NFS4ERR_NOENT}
	}
	if partial, ok := partialIOBackend(d.backend); ok && preferWriteAt(d.backend, dstBucket) {
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return OpResult{OpCode: OpRename, Status: NFS4ERR_IO}
		}
		if _, err := partial.WriteAt(context.Background(), dstBucket, newKey, 0, data); err != nil {
			return OpResult{OpCode: OpRename, Status: NFS4ERR_IO}
		}
	} else {
		if _, err := d.backend.PutObject(context.Background(), dstBucket, newKey, rc, "application/octet-stream"); err != nil {
			rc.Close()
			return OpResult{OpCode: OpRename, Status: NFS4ERR_IO}
		}
		rc.Close()
	}
	d.backend.DeleteObject(context.Background(), srcBucket, oldKey) //nolint:errcheck

	d.state.InvalidateFH(oldPath)
	d.state.GetOrCreateFH(newPath)
	d.state.InvalidateObject(srcBucket, oldKey)
	d.state.InvalidateObject(dstBucket, newKey)

	now := uint64(time.Now().UnixNano())
	w := getXDRWriter()
	// source_cinfo + target_cinfo (each: atomic + before + after)
	w.WriteUint32(1)
	w.WriteUint64(now)
	w.WriteUint64(now)
	w.WriteUint32(1)
	w.WriteUint64(now)
	w.WriteUint64(now)
	return OpResult{OpCode: OpRename, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

// --- NFSv4.1/4.2 session op handlers ---
