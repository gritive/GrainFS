package nfs4server

func (d *Dispatcher) opPutRootFH() OpResult {
	d.currentFH = d.state.RootFH()
	d.currentPath = "/"
	return OpResult{OpCode: OpPutRootFH, Status: NFS4_OK}
}

func (d *Dispatcher) opPutFH(data []byte) OpResult {
	if len(data) != 16 {
		return OpResult{OpCode: OpPutFH, Status: NFS4ERR_BADHANDLE}
	}
	var fh FileHandle
	copy(fh[:], data)
	p, ok := d.state.ResolveFH(fh)
	if !ok {
		return OpResult{OpCode: OpPutFH, Status: NFS4ERR_STALE}
	}
	if d.server != nil {
		bucket, _ := extractBucketAndKey(p)
		if bucket != "" {
			cfg, ok := d.server.loadExports().byBucket[bucket]
			if !ok {
				return OpResult{OpCode: OpPutFH, Status: NFS4ERR_ADMIN_REVOKED}
			}
			if binding, ok := d.state.FHBinding(fh); ok && binding.generation != cfg.generation {
				return OpResult{OpCode: OpPutFH, Status: NFS4ERR_FHEXPIRED}
			}
		}
	}
	d.currentFH = fh
	d.currentPath = p
	return OpResult{OpCode: OpPutFH, Status: NFS4_OK}
}

func (d *Dispatcher) opGetFH() OpResult {
	if d.currentPath == "" {
		return OpResult{OpCode: OpGetFH, Status: NFS4ERR_BADHANDLE}
	}
	w := getXDRWriter()
	w.WriteOpaque(d.currentFH[:])
	return OpResult{OpCode: OpGetFH, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

func (d *Dispatcher) opSaveFH() OpResult {
	d.savedFH = d.currentFH
	d.savedPath = d.currentPath
	return OpResult{OpCode: OpSaveFH, Status: NFS4_OK}
}

func (d *Dispatcher) opRestoreFH() OpResult {
	if d.savedPath == "" {
		return OpResult{OpCode: OpRestoreFH, Status: NFS4ERR_RESTOREFH}
	}
	d.currentFH = d.savedFH
	d.currentPath = d.savedPath
	return OpResult{OpCode: OpRestoreFH, Status: NFS4_OK}
}

// bindFHInheritingParent binds fh for bucket at the given export generation,
// propagating the current fh's saID binding (T12). A freshly created fh has
// saID="" which would mis-classify as anon, so when the parent fh (d.currentFH)
// carries a concrete principal — saID set and not the "(pending)" sentinel —
// the child inherits its saID + readOnly; otherwise it gets a generation-only
// binding. Shared by opOpen, opLookup, and opCreate.
func (d *Dispatcher) bindFHInheritingParent(fh FileHandle, bucket string, gen uint64) {
	parentBind, ok := d.state.FHBinding(d.currentFH)
	if ok && parentBind.saID != "" && parentBind.saID != fhSAIDPending {
		d.state.BindFHWithBinding(fh, bucket, parentBind.saID, parentBind.readOnly, gen)
	} else {
		d.state.BindFHGeneration(fh, bucket, gen)
	}
}
