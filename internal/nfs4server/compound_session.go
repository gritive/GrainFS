package nfs4server

import (
	"encoding/binary"
	"io"
)

func (d *Dispatcher) opSetClientID(data []byte) OpResult {
	var verf [8]byte
	if len(data) >= 8 {
		copy(verf[:], data[:8])
	}

	clientID := d.state.SetClientID(verf)

	w := getXDRWriter()
	w.WriteUint64(clientID)
	// setclientid_confirm verifier (8 bytes)
	w.WriteUint64(clientID) // use clientID as confirm verifier
	return OpResult{OpCode: OpSetClientID, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

func (d *Dispatcher) opSetClientIDConfirm(data []byte) OpResult {
	if len(data) >= 8 {
		clientID := binary.BigEndian.Uint64(data[:8])
		d.state.ConfirmClientID(clientID)
	}
	return OpResult{OpCode: OpSetClientIDConfirm, Status: NFS4_OK}
}

func (d *Dispatcher) opExchangeID(data []byte) OpResult {
	if len(data) < 8 {
		return OpResult{OpCode: OpExchangeID, Status: NFS4ERR_INVAL}
	}
	r := NewXDRReader(data)
	var verf [8]byte
	io.ReadFull(&r.r, verf[:])
	ownerID, _ := r.ReadOpaque()

	res := d.state.ExchangeID(verf, ownerID)

	// RFC 5661 §18.35.3: server MUST set at least one of USE_NON_PNFS/USE_PNFS_MDS/USE_PNFS_DS.
	// Without this bit, the Linux kernel's nfs4_discover_server_trunking returns EINVAL.
	const eirFlagUseNonPNFS = uint32(0x00010000)

	w := getXDRWriter()
	w.WriteUint64(res.ClientID)      // eir_clientid
	w.WriteUint32(res.SequenceID)    // eir_sequenceid
	w.WriteUint32(eirFlagUseNonPNFS) // eir_flags
	w.WriteUint32(0)                 // eir_state_protect.spr_how = SP4_NONE
	// eir_server_owner: minor_id + major_id
	w.WriteUint64(1)         // server minor id
	w.WriteString("grainfs") // server major id
	// eir_server_scope
	w.WriteString("grainfs-scope")
	// eir_server_impl_id count = 0
	w.WriteUint32(0)
	return OpResult{OpCode: OpExchangeID, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

func (d *Dispatcher) opCreateSession(data []byte) OpResult {
	if len(data) < 12 {
		return OpResult{OpCode: OpCreateSession, Status: NFS4ERR_INVAL}
	}
	r := NewXDRReader(data)
	clientID, _ := r.ReadUint64()
	_, _ = r.ReadUint32() // sequence

	fore := ChannelAttrs{
		MaxRequestSize:        262144,
		MaxResponseSize:       262144,
		MaxResponseSizeCached: 4096,
		MaxOperations:         64,
		MaxRequests:           64,
	}
	if r.Remaining() >= 24 {
		fore.HeaderPadSize, _ = r.ReadUint32()
		fore.MaxRequestSize, _ = r.ReadUint32()
		fore.MaxResponseSize, _ = r.ReadUint32()
		fore.MaxResponseSizeCached, _ = r.ReadUint32()
		fore.MaxOperations, _ = r.ReadUint32()
		fore.MaxRequests, _ = r.ReadUint32()
	}

	sid, seq := d.state.CreateSession(clientID, fore)

	w := getXDRWriter()
	w.buf.Write(sid[:]) // csr_sessionid: fixed 16 bytes (no length prefix)
	w.WriteUint32(seq)  // csr_sequence
	w.WriteUint32(0)    // csr_flags
	// fore channel attrs echo
	w.WriteUint32(fore.HeaderPadSize)
	w.WriteUint32(fore.MaxRequestSize)
	w.WriteUint32(fore.MaxResponseSize)
	w.WriteUint32(fore.MaxResponseSizeCached)
	w.WriteUint32(fore.MaxOperations)
	w.WriteUint32(fore.MaxRequests)
	w.WriteUint32(0) // ca_rdma_ird count
	// back channel attrs (minimal echo)
	w.WriteUint32(0)
	w.WriteUint32(4096)
	w.WriteUint32(4096)
	w.WriteUint32(4096)
	w.WriteUint32(4)
	w.WriteUint32(4)
	w.WriteUint32(0) // ca_rdma_ird count
	return OpResult{OpCode: OpCreateSession, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

func (d *Dispatcher) opDestroySession(data []byte) OpResult {
	if len(data) < 16 {
		return OpResult{OpCode: OpDestroySession, Status: NFS4ERR_INVAL}
	}
	var sid SessionID
	copy(sid[:], data[:16])
	if !d.state.DestroySession(sid) {
		return OpResult{OpCode: OpDestroySession, Status: NFS4ERR_BADSESSION}
	}
	return OpResult{OpCode: OpDestroySession, Status: NFS4_OK}
}

func (d *Dispatcher) opSequence(data []byte) OpResult {
	if len(data) < 32 {
		return OpResult{OpCode: OpSequence, Status: NFS4ERR_INVAL}
	}
	var sid SessionID
	copy(sid[:], data[:16])
	seqID := binary.BigEndian.Uint32(data[16:20])
	slotID := binary.BigEndian.Uint32(data[20:24])
	highSlot := binary.BigEndian.Uint32(data[24:28])
	cacheThis := binary.BigEndian.Uint32(data[28:32])

	sess := d.state.GetSession(sid)
	if sess == nil {
		return OpResult{OpCode: OpSequence, Status: NFS4ERR_BADSESSION}
	}

	d.state.slotMu.Lock()
	if int(slotID) >= len(sess.Slots) {
		d.state.slotMu.Unlock()
		return OpResult{OpCode: OpSequence, Status: NFS4ERR_BADSLOT}
	}
	slot := &sess.Slots[slotID]

	// Replay detection: same seqID → return cached full COMPOUND response if available.
	if slot.SeqID == seqID && slot.SeqID > 0 {
		if slot.HasCache {
			d.replayFull = slot.Response // full COMPOUND bytes; used by server.go
			d.state.slotMu.Unlock()
			return OpResult{OpCode: OpSequence, Status: NFS4_OK}
		}
		if slot.WasCacheThis {
			// cacheThis=1 was set on the original request but the response was never
			// stored (e.g. server crashed between request and commit). RFC 5661 §18.46.3
			// requires NFS4ERR_RETRY_UNCACHED_REP in this case.
			d.state.slotMu.Unlock()
			return OpResult{OpCode: OpSequence, Status: NFS4ERR_RETRY_UNCACHED_REP}
		}
		// cacheThis was false on first call; re-execute remaining ops (RFC 5661 §2.10.5.1.3).
		d.state.slotMu.Unlock()
		// fall through to re-build response below
	} else if slot.SeqID > 0 && seqID != slot.SeqID+1 {
		// Stale or future seqID (not the expected next or a replay).
		d.state.slotMu.Unlock()
		return OpResult{OpCode: OpSequence, Status: NFS4ERR_SEQ_MISORDERED}
	} else {
		// First request on this slot: RFC 5661 §18.46.3 requires seqID == 1.
		if slot.SeqID == 0 && seqID != 1 {
			d.state.slotMu.Unlock()
			return OpResult{OpCode: OpSequence, Status: NFS4ERR_SEQ_MISORDERED}
		}
		d.state.slotMu.Unlock()
	}

	w := getXDRWriter()
	w.buf.Write(sid[:])     // sr_sessionid: fixed 16 bytes (no length prefix)
	w.WriteUint32(seqID)    // sr_sequenceid
	w.WriteUint32(slotID)   // sr_slotid
	w.WriteUint32(highSlot) // sr_highest_slotid
	w.WriteUint32(highSlot) // sr_target_highest_slotid
	w.WriteUint32(0)        // sr_status_flags
	seqResp := xdrWriterBytes(w)

	d.state.slotMu.Lock()
	slot.SeqID = seqID
	if cacheThis != 0 {
		// Full COMPOUND response will be stored by storePendingCache after encoding.
		d.pendingCacheSlot = slot
		slot.HasCache = false
		slot.WasCacheThis = true
	} else {
		slot.HasCache = false
		slot.WasCacheThis = false
		slot.Response = nil
	}
	d.state.slotMu.Unlock()

	return OpResult{OpCode: OpSequence, Status: NFS4_OK, Data: seqResp}
}

func (d *Dispatcher) opDestroyClientID(data []byte) OpResult {
	if len(data) < 8 {
		return OpResult{OpCode: OpDestroyClientID, Status: NFS4ERR_INVAL}
	}
	clientID := binary.BigEndian.Uint64(data[:8])
	if !d.state.ClientExists(clientID) {
		return OpResult{OpCode: OpDestroyClientID, Status: NFS4ERR_STALE_CLIENTID}
	}
	d.state.DestroyClientID(clientID)
	return OpResult{OpCode: OpDestroyClientID, Status: NFS4_OK}
}

func (d *Dispatcher) opFreeStateID(data []byte) OpResult {
	if len(data) < 16 {
		return OpResult{OpCode: OpFreeStateID, Status: NFS4ERR_INVAL}
	}
	stateID := binary.BigEndian.Uint64(data[:8])
	d.state.FreeStateID(stateID)
	return OpResult{OpCode: OpFreeStateID, Status: NFS4_OK}
}

func (d *Dispatcher) opTestStateID(data []byte) OpResult {
	if len(data) < 4 {
		return OpResult{OpCode: OpTestStateID, Status: NFS4ERR_INVAL}
	}
	count := binary.BigEndian.Uint32(data[:4])
	statuses := d.state.TestStateIDs(int(count))
	w := getXDRWriter()
	w.WriteUint32(uint32(len(statuses)))
	for _, s := range statuses {
		w.WriteUint32(s)
	}
	return OpResult{OpCode: OpTestStateID, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}
