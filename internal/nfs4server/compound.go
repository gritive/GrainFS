package nfs4server

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/pool"
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
	if d.server != nil && d.server.mountSAStore != nil {
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
		if d.server.mountSAStore != nil && childKey == "" {
			d.state.BindFHWithSAID(fh, childBucket, fhSAIDPending, gen)
		} else {
			// T12 propagation fix: a fresh subdir fh must inherit the parent's
			// saID binding so the per-op anon flip gate (anonRejected) can
			// distinguish mount-SA-bound subdir sessions from anon ones.
			// BindFHGeneration preserves an existing saID, but a freshly-
			// created fh has saID="" which would mis-classify as anon. Pull
			// parent's saID explicitly when it is set.
			parentBind, ok := d.state.FHBinding(d.currentFH)
			if ok && parentBind.saID != "" && parentBind.saID != fhSAIDPending {
				d.state.BindFHWithSAID(fh, childBucket, parentBind.saID, gen)
			} else {
				d.state.BindFHGeneration(fh, childBucket, gen)
			}
		}
	}
	d.currentFH = fh
	d.currentPath = childPath
	return OpResult{OpCode: OpLookup, Status: NFS4_OK}
}

// fhSAIDPending is the sentinel stored in stateBinding.saID for a bucket-level
// fh that is awaiting the next LOOKUP to determine mount-SA vs anon binding.
const fhSAIDPending = "(pending)"

// anonRejected reports whether the given fh's anon binding is no longer
// allowed: the binding has saID="" (anon confirmed) AND iam.anon-enabled is
// false. Returns false when the gate is not wired (cfg==nil), when the fh
// has no binding (pseudo-root / unbound), when the binding is mount-SA
// confirmed (saID!="" and not pending), or when iam.anon-enabled=true.
//
// NFS§B T12: per-op guard for Phase 0 → Phase 2 transitions. Mirrors the
// S3 path (§9 T73): active anon-bound sessions must be rejected on the
// next op after the first SA create flips iam.anon-enabled=false.
//
// Hot path: one map RLock + one cfg RLock per op when the gate is wired.
// Zero allocation. The branch is dead when cfg==nil (the production wiring
// passes config.Store; tests inject a stub).
func (d *Dispatcher) anonRejected(fh FileHandle) bool {
	if d.server == nil || d.server.cfg == nil {
		return false
	}
	binding, ok := d.state.FHBinding(fh)
	if !ok {
		return false
	}
	if binding.saID != "" {
		// "(pending)" or "<mount-sa-name>" — not an anon binding.
		return false
	}
	anon, ok := d.server.cfg.GetBool("iam.anon-enabled")
	if !ok {
		// Key not registered: be conservative and do not block.
		return false
	}
	return !anon
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
		// binding (anon "" vs mount-SA "<name>") for the per-op anon flip gate.
		parentBind, ok := d.state.FHBinding(d.currentFH)
		if ok && parentBind.saID != "" && parentBind.saID != fhSAIDPending {
			d.state.BindFHWithSAID(fh, bucket, parentBind.saID, gen)
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
	bucket, _ := extractBucketAndKey(p)
	return d.server.isExportReadOnly(bucket)
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
		// binding (anon "" vs mount-SA "<name>") for the per-op anon flip gate.
		parentBind, ok := d.state.FHBinding(d.currentFH)
		if ok && parentBind.saID != "" && parentBind.saID != fhSAIDPending {
			d.state.BindFHWithSAID(fh, bucket, parentBind.saID, gen)
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
	// Reject if either is anon-bound and the flip happened.
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

func init() {
	// Suppress unused warning for fmt
	_ = fmt.Sprintf
}
