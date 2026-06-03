package nfs4server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/pool"
	"github.com/gritive/GrainFS/internal/protocred"
	"github.com/gritive/GrainFS/internal/storage"
)

// decisionAllow mirrors policy.DecisionAllow (iota value 1) without an import
// cycle. The package-level unit test in nfs4_mount_auth_test.go verifies the
// value stays in sync.
const decisionAllow = policy.DecisionAllow

// nfsMountReqCtx is the immutable policy.RequestContext used for every
// grainfs:NFSMount evaluation. Resource="*" per spec D#5.
// PrincipalType=PrincipalTypeMount routes resolver lookups to the
// mount-SA pool (FU#5 / F-§B-resolver-mountsa); the anon (saID="") path
// short-circuits before the pool is consulted so the type tag is a no-op
// for anon callers.
var nfsMountReqCtx = policy.RequestContext{
	Action:        "grainfs:NFSMount",
	Resource:      "*",
	PrincipalType: policy.PrincipalTypeMount,
}

var opReadBufPool = pool.New(func() *bytes.Buffer { return new(bytes.Buffer) })
var bytesReaderPool = pool.New(func() *bytes.Reader { return new(bytes.Reader) })

// nfsMaxReadBlock matches the rsize mount option (131072 = 128 KiB).
// Pooling at this size eliminates per-call allocs in the ReadAt fast path.
const nfsMaxReadBlock = 131072

const (
	maxInt64Uint             = uint64(1<<63 - 1)
	nfsMaxFallbackSparseSize = 1 << 30
)

var opReadAtBufPool = pool.New(func() []byte { return make([]byte, nfsMaxReadBlock) })

var compoundReqPool = pool.New(func() *CompoundRequest {
	return &CompoundRequest{Ops: make([]Op, 0, maxCompoundOps)}
})

var compoundRespPool = pool.New(func() *CompoundResponse {
	return &CompoundResponse{Results: make([]OpResult, 0, maxCompoundOps)}
})

func putCompoundResponse(resp *CompoundResponse) {
	for i := range resp.Results {
		resp.Results[i].release()
	}
	resp.Results = resp.Results[:0]
	compoundRespPool.Put(resp)
}

var dispatcherPool = pool.New(func() *Dispatcher { return &Dispatcher{} })

func getDispatcherWithClient(backend storage.Backend, state *StateManager, server *Server, clientAddr string, hinter *unknownExportHinter) *Dispatcher {
	d := dispatcherPool.Get()
	d.backend = backend
	d.state = state
	d.server = server
	d.clientAddr = clientAddr
	d.hinter = hinter
	d.currentFH = FileHandle{}
	d.currentPath = ""
	d.savedFH = FileHandle{}
	d.savedPath = ""
	d.minorVer = 0
	d.replayFull = nil
	d.pendingCacheSlot = nil
	if server != nil {
		d.writeBuffer = server.writeBuffer
	}
	return d
}

func putDispatcher(d *Dispatcher) {
	d.backend = nil
	d.state = nil
	d.server = nil
	d.clientAddr = ""
	d.hinter = nil
	d.replayFull = nil
	d.pendingCacheSlot = nil
	d.writeBuffer = nil
	dispatcherPool.Put(d)
}

// storePendingCache stores the full encoded COMPOUND response in the pending slot cache.
func (d *Dispatcher) storePendingCache(response []byte) {
	if d.pendingCacheSlot == nil {
		return
	}
	d.state.slotMu.Lock()
	d.pendingCacheSlot.Response = response
	d.pendingCacheSlot.HasCache = true
	d.state.slotMu.Unlock()
	d.pendingCacheSlot = nil
}

// NFSv4 status codes (RFC 7530)
const (
	NFS4_OK                     = 0
	NFS4ERR_PERM                = 1
	NFS4ERR_NOENT               = 2
	NFS4ERR_IO                  = 5
	NFS4ERR_ACCESS              = 13
	NFS4ERR_XDEV                = 18
	NFS4ERR_NOTDIR              = 20
	NFS4ERR_INVAL               = 22
	NFS4ERR_FBIG                = 27
	NFS4ERR_NOSPC               = 28
	NFS4ERR_ROFS                = 30
	NFS4ERR_STALE               = 70
	NFS4ERR_BADHANDLE           = 10001
	NFS4ERR_BAD_STATEID         = 10025
	NFS4ERR_NOT_SAME            = 10027
	NFS4ERR_RESOURCE            = 10018
	NFS4ERR_SERVERFAULT         = 10006
	NFS4ERR_NOTSUPP             = 10004
	NFS4ERR_FHEXPIRED           = 10014
	NFS4ERR_RESTOREFH           = 10030
	NFS4ERR_NOFILEHANDLE        = 10020
	NFS4ERR_BADSESSION          = 10052
	NFS4ERR_BADSLOT             = 10060
	NFS4ERR_SEQ_MISORDERED      = 10063
	NFS4ERR_RETRY_UNCACHED_REP  = 10070
	NFS4ERR_STALE_CLIENTID      = 10022
	NFS4ERR_MINOR_VERS_MISMATCH = 10021
	NFS4ERR_OP_ILLEGAL          = 10044
	NFS4ERR_ADMIN_REVOKED       = 10047

	SEQ4_STATUS_ADMIN_STATE_REVOKED = 0x00000020
)

// NFSv4 operation codes
const (
	OpAccess             = 3
	OpClose              = 4
	OpCommit             = 5
	OpCreate             = 6
	OpGetAttr            = 9
	OpGetFH              = 10
	OpLookup             = 15
	OpOpen               = 18
	OpOpenConfirm        = 20
	OpPutFH              = 22
	OpPutRootFH          = 24
	OpRead               = 25
	OpReadDir            = 26
	OpRemove             = 28
	OpRename             = 29
	OpSetAttr            = 34
	OpSetClientID        = 35
	OpSetClientIDConfirm = 36
	OpWrite              = 38

	// NFSv4.1 ops (RFC 5661)
	OpExchangeID      = 42
	OpCreateSession   = 43
	OpDestroySession  = 44
	OpFreeStateID     = 45
	OpSequence        = 53
	OpTestStateID     = 55
	OpDestroyClientID = 57
	OpReclaimComplete = 58

	// NFSv4.2 ops (RFC 7862)
	OpAllocate   = 59
	OpCopy       = 60
	OpDeallocate = 62
	OpIOAdvise   = 63
	OpSeek       = 69

	maxCompoundOps = 64
)

type attrBitmap [3]uint32

func readAttrBitmap(r *XDRReader) attrBitmap {
	var bm attrBitmap
	bitmapLen, _ := r.ReadUint32()
	for i := uint32(0); i < bitmapLen; i++ {
		word, _ := r.ReadUint32()
		if i < uint32(len(bm)) {
			bm[i] = word
		}
	}
	return bm
}

func attrBitmapLen(bm attrBitmap) uint32 {
	for i := len(bm) - 1; i >= 0; i-- {
		if bm[i] != 0 {
			return uint32(i + 1)
		}
	}
	return 0
}

// NFS file types
const (
	NF4REG = 1
	NF4DIR = 2
)

type Op struct {
	OpCode  int
	Data    []byte
	poolKey int // 0=no pool, 8/16/32=opArgPool{8,16,32}
}

type OpResult struct {
	OpCode       int
	Status       int
	Data         []byte
	readData     []byte
	readEOF      bool
	readPoolSize int
}

type CompoundRequest struct {
	Tag      string
	MinorVer uint32
	Ops      []Op
}

type CompoundResponse struct {
	Status  int
	Tag     string
	Results []OpResult
}

type Dispatcher struct {
	backend          storage.Backend
	state            *StateManager
	server           *Server
	clientAddr       string
	hinter           *unknownExportHinter
	currentFH        FileHandle
	currentPath      string
	savedFH          FileHandle
	savedPath        string
	minorVer         uint32
	replayFull       []byte     // non-nil when a SEQUENCE cache hit provides the full COMPOUND response
	pendingCacheSlot *SlotEntry // non-nil when cacheThis=1; filled after full response is encoded
	writeBuffer      *writeBuffer
}

const (
	OpRestoreFH = 31
	OpSaveFH    = 32
)

func NewDispatcher(backend storage.Backend) *Dispatcher {
	return &Dispatcher{backend: backend}
}

func (d *Dispatcher) Dispatch(req *CompoundRequest, resp *CompoundResponse) {
	resp.Tag = req.Tag
	resp.Status = NFS4_OK
	d.minorVer = req.MinorVer

	if req.MinorVer > 2 {
		resp.Status = NFS4ERR_MINOR_VERS_MISMATCH
		return
	}

	if len(req.Ops) > maxCompoundOps {
		resp.Status = NFS4ERR_RESOURCE
		return
	}

	for _, op := range req.Ops {
		result := d.dispatchOp(op)
		if result.Status != NFS4_OK {
			log.Debug().Int("op", op.OpCode).Int("status", result.Status).Msg("nfs4: op failed")
		}
		switch op.poolKey {
		case 8:
			putOpArg8(op.Data)
		case 16:
			putOpArg16(op.Data)
		case 32:
			putOpArg32(op.Data)
		}
		resp.Results = append(resp.Results, result)
		if result.Status != NFS4_OK {
			resp.Status = result.Status
			break
		}
		if d.replayFull != nil {
			// SEQUENCE cache hit: full cached COMPOUND response is ready; skip remaining ops.
			break
		}
	}
}

func minVersionForOp(opCode int) uint32 {
	switch opCode {
	case OpExchangeID, OpCreateSession, OpDestroySession, OpSequence,
		OpReclaimComplete, OpDestroyClientID, OpFreeStateID, OpTestStateID:
		return 1
	case OpSeek, OpAllocate, OpDeallocate, OpCopy, OpIOAdvise:
		return 2
	default:
		return 0
	}
}

func (d *Dispatcher) dispatchOp(op Op) OpResult {
	if minVersionForOp(op.OpCode) > d.minorVer {
		return OpResult{OpCode: op.OpCode, Status: NFS4ERR_OP_ILLEGAL}
	}
	switch op.OpCode {
	case OpPutRootFH:
		return d.opPutRootFH()
	case OpPutFH:
		return d.opPutFH(op.Data)
	case OpGetFH:
		return d.opGetFH()
	case OpLookup:
		return d.opLookup(op.Data)
	case OpCreate:
		return d.opCreate(op.Data)
	case OpGetAttr:
		return d.opGetAttr(op.Data)
	case OpAccess:
		return d.opAccess(op.Data)
	case OpReadDir:
		return d.opReadDir(op.Data)
	case OpRead:
		return d.opRead(op.Data)
	case OpWrite:
		return d.opWrite(op.Data)
	case OpOpen:
		return d.opOpen(op.Data)
	case OpOpenConfirm:
		return d.opOpenConfirm(op.Data)
	case OpClose:
		return d.opClose(op.Data)
	case OpSetClientID:
		return d.opSetClientID(op.Data)
	case OpSetClientIDConfirm:
		return d.opSetClientIDConfirm(op.Data)
	case OpRemove:
		return d.opRemove(op.Data)
	case OpRename:
		return d.opRename(op.Data)
	case OpSetAttr:
		return d.opSetAttr(op.Data)
	case OpCommit:
		return d.opCommit()
	case OpRestoreFH:
		return d.opRestoreFH()
	case OpSaveFH:
		return d.opSaveFH()
	case OpRenew:
		return OpResult{OpCode: OpRenew, Status: NFS4_OK}
	case OpExchangeID:
		return d.opExchangeID(op.Data)
	case OpCreateSession:
		return d.opCreateSession(op.Data)
	case OpDestroySession:
		return d.opDestroySession(op.Data)
	case OpSequence:
		return d.opSequence(op.Data)
	case OpReclaimComplete:
		return OpResult{OpCode: OpReclaimComplete, Status: NFS4_OK}
	case OpDestroyClientID:
		return d.opDestroyClientID(op.Data)
	case OpFreeStateID:
		return d.opFreeStateID(op.Data)
	case OpTestStateID:
		return d.opTestStateID(op.Data)
	case OpSeek:
		return d.opSeek(op.Data)
	case OpAllocate:
		return d.opAllocate(op.Data)
	case OpDeallocate:
		return d.opDeallocate(op.Data)
	case OpCopy:
		return d.opCopy(op.Data)
	case OpIOAdvise:
		return d.opIOAdvise(op.Data)
	default:
		log.Debug().Int("opcode", op.OpCode).Msg("nfs4: unknown opcode → NFS4ERR_NOTSUPP")
		return OpResult{OpCode: op.OpCode, Status: NFS4ERR_NOTSUPP}
	}
}

// fhSAIDPending is the sentinel stored in stateBinding.saID for a bucket-level
// fh that is awaiting the next LOOKUP to determine mount-SA vs anon binding.
const fhSAIDPending = "(pending)"

// anonRejected is retained for the NFS operation call sites. The global
// anonymous transition no longer exists, so established anonymous NFS bindings
// are not revoked by a cluster config flip.
func (d *Dispatcher) anonRejected(fh FileHandle) bool {
	return false
}

// opLookupResolvePending handles the 2nd LOOKUP from a bucket fh with
// saID="(pending)". It resolves the 'name' component as either a mount-SA
// (grainfs:NFSMount eval) or an anon file/dir (anon grainfs:NFSMount eval).
// Spec D#5 error code policy:
//   - mount-SA pool hit + grainfs:NFSMount deny → NFS4ERR_ACCESS
//   - pool miss + file/dir exists + anon deny  → NFS4ERR_ACCESS
//   - pool miss + no file/dir                  → NFS4ERR_NOENT
func (d *Dispatcher) opLookupResolvePending(name, childPath, childBucket, childKey string, parentBinding stateBinding) OpResult {
	ctx := context.Background()
	gen := d.server.exportGeneration(childBucket)

	if _, handled, res := d.resolveProtocolCredentialLookup(ctx, name, childPath, childBucket, gen); handled {
		return res
	}

	if d.server.mountSAStore != nil {
		if _, ok := d.server.mountSAStore.Get(name); ok {
			// mount-SA hit: evaluate grainfs:NFSMount for this SA.
			if d.server.authorizer != nil {
				res := d.server.authorizer.Authorize(ctx, name, childBucket, nfsMountReqCtx)
				if res.Decision != decisionAllow {
					log.Debug().Str("saID", name).Str("bucket", childBucket).
						Str("reason", res.Reason).Msg("nfs4: NFSMount denied")
					if d.server.auditHook != nil {
						d.server.auditHook(audit.S3Event{
							Ts:         time.Now().UnixMicro(),
							SAID:       name,
							SourceIP:   d.clientAddr,
							Bucket:     childBucket,
							AuthStatus: "deny",
							Source:     "nfs4",
						})
					}
					return OpResult{OpCode: OpLookup, Status: NFS4ERR_ACCESS}
				}
			}
			// Confirmed mount-SA binding.
			fh := d.state.GetOrCreateFH(childPath)
			d.state.BindFHWithSAID(fh, childBucket, name, gen)
			d.currentFH = fh
			d.currentPath = childPath
			log.Debug().Str("name", name).Str("bucket", childBucket).Msg("nfs4: LOOKUP mount-SA confirmed")
			if d.server.auditHook != nil {
				d.server.auditHook(audit.S3Event{
					Ts:         time.Now().UnixMicro(),
					SAID:       name,
					SourceIP:   d.clientAddr,
					Bucket:     childBucket,
					AuthStatus: "allow",
					Source:     "nfs4",
				})
			}
			return OpResult{OpCode: OpLookup, Status: NFS4_OK}
		}
	}

	// Pool miss: check if it's a real backend file/dir (anon path).
	exists := d.state.IsDir(childPath)
	if !exists && d.backend != nil {
		_, err := d.backend.HeadObject(ctx, childBucket, childKey)
		exists = err == nil
	}
	if !exists {
		return OpResult{OpCode: OpLookup, Status: NFS4ERR_NOENT}
	}

	// Anon path: evaluate grainfs:NFSMount with saID="" (anon).
	// Error code policy: deny returns ACCESS, not NOENT (mount-SA existence leak avoidance).
	if d.server.authorizer != nil {
		res := d.server.authorizer.Authorize(ctx, "", childBucket, nfsMountReqCtx)
		if res.Decision != decisionAllow {
			log.Debug().Str("bucket", childBucket).
				Str("reason", res.Reason).Msg("nfs4: anon NFSMount denied")
			if d.server.auditHook != nil {
				d.server.auditHook(audit.S3Event{
					Ts:         time.Now().UnixMicro(),
					SAID:       "", // anon: empty string (F#39)
					SourceIP:   d.clientAddr,
					Bucket:     childBucket,
					AuthStatus: "deny",
					Source:     "nfs4",
				})
			}
			return OpResult{OpCode: OpLookup, Status: NFS4ERR_ACCESS}
		}
	}

	// Anon confirmed: issue fh with saID="" (empty = anon).
	fh := d.state.GetOrCreateFH(childPath)
	d.state.BindFHWithSAID(fh, childBucket, "", gen)
	d.currentFH = fh
	d.currentPath = childPath
	log.Debug().Str("name", name).Str("bucket", childBucket).Msg("nfs4: LOOKUP anon path confirmed")
	if d.server.auditHook != nil {
		d.server.auditHook(audit.S3Event{
			Ts:         time.Now().UnixMicro(),
			SAID:       "", // anon: empty string (F#39)
			SourceIP:   d.clientAddr,
			Bucket:     childBucket,
			AuthStatus: "allow",
			Source:     "nfs4",
		})
	}
	return OpResult{OpCode: OpLookup, Status: NFS4_OK}
}

func (d *Dispatcher) resolveProtocolCredentialLookup(ctx context.Context, token, childPath, bucket string, gen uint64) (stateBinding, bool, OpResult) {
	if d.server.protocolCredentials == nil || !strings.Contains(token, ":") {
		return stateBinding{}, false, OpResult{}
	}
	credentialID, secret, ok := strings.Cut(token, ":")
	if !ok || credentialID == "" || secret == "" {
		return stateBinding{}, true, OpResult{OpCode: OpLookup, Status: NFS4ERR_NOENT}
	}
	decision, err := validateProtocolCredentialAttach(ctx, d.server.protocolCredentials, protocred.ProtocolNFS, bucket, credentialID, secret)
	if err != nil {
		if d.server.auditHook != nil {
			d.server.auditHook(audit.S3Event{
				Ts:         time.Now().UnixMicro(),
				SAID:       decision.SAID,
				SourceIP:   d.clientAddr,
				Bucket:     bucket,
				AuthStatus: "deny",
				Source:     "nfs4",
			})
		}
		return stateBinding{}, true, OpResult{OpCode: OpLookup, Status: NFS4ERR_ACCESS}
	}
	fh := d.state.GetOrCreateFH(childPath)
	readOnly := decision.Mode == protocred.ModeRO
	d.state.BindFHWithBinding(fh, bucket, decision.SAID, readOnly, gen)
	d.currentFH = fh
	d.currentPath = childPath
	if d.server.auditHook != nil {
		d.server.auditHook(audit.S3Event{
			Ts:         time.Now().UnixMicro(),
			SAID:       decision.SAID,
			SourceIP:   d.clientAddr,
			Bucket:     bucket,
			AuthStatus: "allow",
			Source:     "nfs4",
		})
	}
	return stateBinding{bucket: bucket, saID: decision.SAID, readOnly: readOnly, generation: gen}, true, OpResult{OpCode: OpLookup, Status: NFS4_OK}
}

func validateProtocolCredentialAttach(ctx context.Context, validator protocolCredentialValidator, protocol protocred.Protocol, bucket, credentialID, secret string) (protocred.AttachDecision, error) {
	req := protocred.AttachRequest{
		Protocol:        protocol,
		Resource:        "bucket/" + bucket,
		CredentialID:    credentialID,
		PresentedSecret: secret,
		RequestedMode:   protocred.ModeRW,
		Strict:          true,
	}
	decision, err := validator.ValidateAttach(ctx, req)
	if err == nil {
		return decision, nil
	}
	req.RequestedMode = protocred.ModeRO
	return validator.ValidateAttach(ctx, req)
}

func (d *Dispatcher) encodeAttrs(p string, reqBit attrBitmap) []byte {
	return d.encodeAttrsWithObject(p, reqBit, nil)
}

func (d *Dispatcher) encodeAttrsWithObject(p string, reqBit attrBitmap, listedObj *storage.Object) []byte {
	if attrBitmapLen(reqBit) == 0 {
		w := getXDRWriter()
		w.WriteUint32(0)
		w.WriteOpaque(nil)
		return xdrWriterBytes(w)
	}
	hasBit := func(bit uint) bool {
		word := bit / 32
		if word >= uint(len(reqBit)) {
			return false
		}
		return reqBit[word]&(1<<(bit%32)) != 0
	}

	// Resolve metadata for the current path
	isDir := d.state.IsDir(p)
	var fileSize uint64
	var lastModUnix int64
	var sidecarMode uint32
	fileid := pathToFileID(p)
	bucket, key := extractBucketAndKey(p)
	isPseudoRoot := p == "/"
	fsidMajor, fsidMinor := uint64(1), uint64(0)
	if !isPseudoRoot && d.server != nil && bucket != "" {
		fsidMajor, fsidMinor = d.server.exportFSID(bucket)
	}
	fhExpireType := uint32(0x00000001) // FH4_VOLATILE_ANY

	if isPseudoRoot {
		isDir = true
		fileSize = 4096
	} else if listedObj != nil {
		isDir = false
		fileSize = uint64(listedObj.Size)
		lastModUnix = listedObj.LastModified

		meta := d.loadFileMeta(bucket, key)
		sidecarMode = meta.Mode
		if meta.Mtime > 0 {
			lastModUnix = meta.Mtime / 1e9
		}
	} else if !isDir && d.backend != nil {
		// WriteBuffer (when wired) holds pending writes that haven't flushed
		// to backend yet — its file size must take precedence over backend
		// HeadObject, otherwise NFS clients see stale size (often 0 for new
		// objects) and short-circuit reads as past-EOF.
		bufferHit := false
		if d.writeBuffer != nil {
			if sz, ok := d.writeBuffer.Size(bucket, key); ok {
				isDir = false
				fileSize = uint64(sz)
				lastModUnix = time.Now().Unix()
				bufferHit = true
			}
		}
		if !bufferHit {
			obj, err := d.backend.HeadObject(context.Background(), bucket, key)
			if err == nil {
				isDir = false
				fileSize = uint64(obj.Size)
				lastModUnix = obj.LastModified
			} else {
				// Unknown path: treat as directory (fallback for uncreated subdirs)
				isDir = true
				fileSize = 4096
			}
		}

		// Read sidecar for NFS-specific mode/mtime (key is in scope here)
		if !isDir {
			meta := d.loadFileMeta(bucket, key)
			sidecarMode = meta.Mode
			if meta.Mtime > 0 {
				lastModUnix = meta.Mtime / 1e9
			}
		}
	} else {
		fileSize = 4096
	}
	if isDir {
		// Use creation timestamp so CHANGE is non-zero and stable within a session.
		lastModUnix = d.state.DirMtime(p) / 1e9
	}

	fileType := uint32(NF4REG)
	mode := uint32(0644)
	if sidecarMode != 0 {
		mode = sidecarMode
	}
	numlinks := uint32(1)
	if isDir {
		fileType = NF4DIR
		mode = 0755
		numlinks = 2
	}

	// Build attrvals in RFC 7530 §5 ascending bit order, track which bits we set.
	attrW := getXDRWriter()
	var respBit attrBitmap

	setBit := func(bit uint) {
		word := bit / 32
		if word < uint(len(respBit)) {
			respBit[word] |= 1 << (bit % 32)
		}
	}

	// Bit 0: SUPPORTED_ATTRS — bitmap4<> listing all attrs we support
	if hasBit(0) {
		setBit(0)
		supW0 := uint32(1<<0 | 1<<1 | 1<<2 | 1<<3 | 1<<4 | 1<<5 | 1<<6 |
			1<<7 | 1<<8 | 1<<9 | 1<<10 | 1<<13 | 1<<15 | 1<<20 | 1<<27 | 1<<30 | 1<<31)
		supW1 := uint32(1<<1 | 1<<3 | 1<<4 | 1<<5 | 1<<13 | 1<<15 | 1<<16 | 1<<20 | 1<<21 | 1<<22 | 1<<23)
		supW2 := uint32(1 << (75 - 64))
		attrW.WriteUint32(3)
		attrW.WriteUint32(supW0)
		attrW.WriteUint32(supW1)
		attrW.WriteUint32(supW2)
	}
	// Bit 1: TYPE — ftype4
	if hasBit(1) {
		setBit(1)
		attrW.WriteUint32(fileType)
	}
	// Bit 2: FH_EXPIRE_TYPE
	if hasBit(2) {
		setBit(2)
		attrW.WriteUint32(fhExpireType)
	}
	// Bit 3: CHANGE — uint64
	if hasBit(3) {
		setBit(3)
		attrW.WriteUint64(uint64(lastModUnix))
	}
	// Bit 4: SIZE — uint64
	if hasBit(4) {
		setBit(4)
		attrW.WriteUint64(fileSize)
	}
	// Bit 5: LINK_SUPPORT — bool
	if hasBit(5) {
		setBit(5)
		attrW.WriteUint32(0)
	}
	// Bit 6: SYMLINK_SUPPORT — bool
	if hasBit(6) {
		setBit(6)
		attrW.WriteUint32(0)
	}
	// Bit 7: NAMED_ATTR — bool
	if hasBit(7) {
		setBit(7)
		attrW.WriteUint32(0)
	}
	// Bit 8: FSID — {major:uint64, minor:uint64}
	if hasBit(8) {
		setBit(8)
		attrW.WriteUint64(fsidMajor)
		attrW.WriteUint64(fsidMinor)
	}
	// Bit 9: UNIQUE_HANDLES — bool
	if hasBit(9) {
		setBit(9)
		attrW.WriteUint32(1)
	}
	// Bit 10: LEASE_TIME — uint32 seconds
	if hasBit(10) {
		setBit(10)
		attrW.WriteUint32(90)
	}
	// Bit 13: ACLSUPPORT — uint32 (0 = no ACL)
	if hasBit(13) {
		setBit(13)
		attrW.WriteUint32(0)
	}
	// Bit 15: CANSETTIME — bool
	if hasBit(15) {
		setBit(15)
		attrW.WriteUint32(1)
	}
	// Bit 20: FILEID — uint64
	if hasBit(20) {
		setBit(20)
		attrW.WriteUint64(fileid)
	}
	// Bit 27: MAXFILESIZE — uint64
	if hasBit(27) {
		setBit(27)
		attrW.WriteUint64(1 << 53)
	}
	// Bit 30: MAXREAD — uint64
	if hasBit(30) {
		setBit(30)
		attrW.WriteUint64(1 * 1024 * 1024)
	}
	// Bit 31: MAXWRITE — uint64
	if hasBit(31) {
		setBit(31)
		attrW.WriteUint64(1 * 1024 * 1024)
	}
	// Bit 33 (word1 bit 1): MODE — uint32
	if hasBit(33) {
		setBit(33)
		attrW.WriteUint32(mode)
	}
	// Bit 35 (word1 bit 3): NUMLINKS — uint32
	if hasBit(35) {
		setBit(35)
		attrW.WriteUint32(numlinks)
	}
	// Bit 36 (word1 bit 4): OWNER — utf8str_mixed
	if hasBit(36) {
		setBit(36)
		attrW.WriteString("root")
	}
	// Bit 37 (word1 bit 5): OWNER_GROUP — utf8str_mixed
	if hasBit(37) {
		setBit(37)
		attrW.WriteString("root")
	}
	// Bit 45 (word1 bit 13): SPACE_USED — uint64
	if hasBit(45) {
		setBit(45)
		attrW.WriteUint64(fileSize)
	}
	// Bit 47 (word1 bit 15): TIME_ACCESS — nfstime4{seconds:int64, nseconds:uint32}
	if hasBit(47) {
		setBit(47)
		attrW.WriteUint64(uint64(lastModUnix))
		attrW.WriteUint32(0)
	}
	// Bit 52 (word1 bit 20): TIME_METADATA — nfstime4
	if hasBit(52) {
		setBit(52)
		attrW.WriteUint64(uint64(lastModUnix))
		attrW.WriteUint32(0)
	}
	// Bit 53 (word1 bit 21): TIME_MODIFY — nfstime4
	if hasBit(53) {
		setBit(53)
		attrW.WriteUint64(uint64(lastModUnix))
		attrW.WriteUint32(0)
	}
	// Bit 55 (word1 bit 23): MOUNTED_ON_FILEID — uint64
	if hasBit(55) {
		setBit(55)
		if !isPseudoRoot && bucket != "" && key == "" {
			attrW.WriteUint64(pathToFileID("/"))
		} else {
			attrW.WriteUint64(fileid)
		}
	}
	// Bit 75 (word2 bit 11): SUPPATTR_EXCLCREAT — bitmap4
	if hasBit(75) {
		setBit(75)
		attrW.WriteUint32(0)
	}

	attrBytes := xdrWriterBytes(attrW)

	// GETATTR4resok: bitmap4 attrmask + attrlist4 attr_vals
	w := getXDRWriter()
	respLen := attrBitmapLen(respBit)
	if respLen > 0 && respLen < 2 {
		respLen = 2
	}
	w.WriteUint32(respLen)
	for i := uint32(0); i < respLen; i++ {
		w.WriteUint32(respBit[i])
	}
	w.WriteOpaque(attrBytes)
	return xdrWriterBytes(w)
}

// pathToFileID returns a stable uint64 file ID for a path using FNV-1a.
func pathToFileID(p string) uint64 {
	h := uint64(14695981039346656037) // FNV-1a offset basis
	for i := 0; i < len(p); i++ {
		h ^= uint64(p[i])
		h *= 1099511628211 // FNV-1a prime
	}
	if h == 0 {
		h = 1
	}
	return h
}

func (d *Dispatcher) invalidatePath(p string) {
	bucket, key := extractBucketAndKey(p)
	d.state.InvalidateObject(bucket, key)
}

func (d *Dispatcher) isPathReadOnly(p string) bool {
	if d.server == nil {
		return false
	}
	if binding, ok := d.state.FHBinding(d.currentFH); ok && binding.readOnly {
		return true
	}
	bucket, _ := extractBucketAndKey(p)
	return d.server.isExportReadOnly(bucket)
}

func (d *Dispatcher) resolveContentType(ctx context.Context, bucket, key string) string {
	if obj, err := d.backend.HeadObject(ctx, bucket, key); err == nil && obj.ContentType != "" {
		return obj.ContentType
	}
	return "application/octet-stream"
}

func (d *Dispatcher) putObjectRMWStreaming(ctx context.Context, bucket, key string, offset uint64, writeData []byte, existingSize uint64, contentType string, ra storage.PartialIO) error {
	var (
		rc      io.ReadCloser
		readers []io.Reader
	)
	if existingSize > 0 {
		prefixLen := min(offset, existingSize)
		if ra != nil {
			if prefixLen > 0 {
				readers = append(readers, &backendReadAtReader{
					ctx:       ctx,
					backend:   ra,
					bucket:    bucket,
					key:       key,
					remaining: int64(prefixLen),
				})
			}
		} else {
			var err error
			rc, _, err = d.backend.GetObject(ctx, bucket, key)
			if err != nil {
				return err
			}
			defer rc.Close()
			if prefixLen > 0 {
				readers = append(readers, io.LimitReader(rc, int64(prefixLen)))
			}
		}
		if offset > existingSize {
			readers = append(readers, io.LimitReader(zeroReader{}, int64(offset-existingSize)))
		}
	} else if offset > 0 {
		readers = append(readers, io.LimitReader(zeroReader{}, int64(offset)))
	}

	readers = append(readers, bytes.NewReader(writeData))
	end := offset + uint64(len(writeData))
	if end < existingSize {
		if ra != nil {
			readers = append(readers, &backendReadAtReader{
				ctx:       ctx,
				backend:   ra,
				bucket:    bucket,
				key:       key,
				offset:    int64(end),
				remaining: int64(existingSize - end),
			})
		} else if rc != nil {
			readers = append(readers, &skipThenReader{r: rc, n: int64(end - min(offset, existingSize))})
		}
	}
	_, err := d.backend.PutObject(ctx, bucket, key, io.MultiReader(readers...), contentType)
	return err
}

type backendReadAtReader struct {
	ctx       context.Context
	backend   storage.PartialIO
	bucket    string
	key       string
	offset    int64
	remaining int64
}

func (r *backendReadAtReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.backend.ReadAt(r.ctx, r.bucket, r.key, r.offset, p)
	r.offset += int64(n)
	r.remaining -= int64(n)
	if err != nil {
		if errors.Is(err, io.EOF) && n > 0 {
			return n, nil
		}
		return n, err
	}
	if n == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	return n, nil
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

type skipThenReader struct {
	r io.Reader
	n int64
}

func (r *skipThenReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for r.n > 0 {
		toSkip := min(int64(len(p)), r.n)
		n, err := r.r.Read(p[:toSkip])
		r.n -= int64(n)
		if err != nil {
			return 0, err
		}
	}
	return r.r.Read(p)
}

// nfsFileMeta stores per-file NFS metadata that must survive server restarts.
// Persisted as JSON under metaSidecarKey(key) in the export's backend bucket.
// Phase 0a: routes through extractBucketAndKey wrapper (single bucket).
// Phase 3: per-export routing.
type nfsFileMeta struct {
	Mode  uint32 `json:"mode"`
	Mtime int64  `json:"mtime_ns"` // UnixNano; 0 means "use object LastModified"
}

func metaSidecarKey(key string) string {
	return "__meta/" + key
}

func fileMetaCacheKey(bucket, key string) string {
	return bucket + "\x00" + key
}

func (d *Dispatcher) loadFileMeta(bucket, key string) nfsFileMeta {
	cacheKey := fileMetaCacheKey(bucket, key)
	if d.state != nil {
		if m, ok := d.state.fileMeta.Load(cacheKey); ok {
			return m
		}
	}
	rc, _, err := d.backend.GetObject(context.Background(), bucket, metaSidecarKey(key))
	if err != nil {
		m := nfsFileMeta{Mode: 0644}
		if d.state != nil {
			d.state.fileMeta.Store(cacheKey, m)
		}
		return m
	}
	defer rc.Close()
	data, _ := io.ReadAll(rc)
	var m nfsFileMeta
	if err := json.Unmarshal(data, &m); err != nil || m.Mode == 0 {
		m.Mode = 0644
	}
	if d.state != nil {
		d.state.fileMeta.Store(cacheKey, m)
	}
	return m
}

func (d *Dispatcher) saveFileMeta(bucket, key string, m nfsFileMeta) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = d.backend.PutObject(context.Background(), bucket, metaSidecarKey(key), bytes.NewReader(data), "application/json")
	if err == nil && d.state != nil {
		d.state.fileMeta.Store(fileMetaCacheKey(bucket, key), m)
	}
	return err
}

// --- Helper functions ---

// encodeSetAttrResult returns the attrsset bitmap so the NFS client knows
// which attributes were actually changed and can invalidate its cache.
func encodeSetAttrResult(bm0, bm1 uint32) []byte {
	w := getXDRWriter()
	w.WriteUint32(2)
	w.WriteUint32(bm0)
	w.WriteUint32(bm1)
	return xdrWriterBytes(w)
}

func boolToUint32(v bool) uint32 {
	if v {
		return 1
	}
	return 0
}

func init() {
	// Suppress unused warning for fmt
	_ = fmt.Sprintf
}
