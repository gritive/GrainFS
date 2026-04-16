package nfs4server

import (
	"github.com/gritive/GrainFS/internal/storage"
)

// NFSv4 status codes (RFC 7530)
const (
	NFS4_OK          = 0
	NFS4ERR_NOENT    = 2
	NFS4ERR_IO       = 5
	NFS4ERR_PERM     = 1
	NFS4ERR_STALE    = 70
	NFS4ERR_BADHANDLE = 10001
	NFS4ERR_RESOURCE  = 10018
	NFS4ERR_NOTDIR   = 20
	NFS4ERR_INVAL    = 22
)

// NFSv4 operation codes (RFC 7530 Section 16)
const (
	OpAccess             = 3
	OpClose              = 4
	OpGetAttr            = 9
	OpGetFH              = 10
	OpLookup             = 15
	OpOpen               = 18
	OpOpenConfirm        = 20
	OpPutFH              = 22
	OpPutRootFH          = 24
	OpRead               = 25
	OpReadDir            = 26
	OpSetAttr            = 34
	OpSetClientID        = 35
	OpSetClientIDConfirm = 36
	OpWrite              = 38

	// Max operations per COMPOUND request
	maxCompoundOps = 64
)

// Op is a single NFSv4 operation within a COMPOUND request.
type Op struct {
	OpCode int
	Data   []byte // XDR-encoded arguments (op-specific)
}

// OpResult is the result of a single NFSv4 operation.
type OpResult struct {
	OpCode int
	Status int
	Data   []byte // XDR-encoded results (op-specific)
}

// CompoundRequest is an NFSv4 COMPOUND request.
type CompoundRequest struct {
	Tag      string
	MinorVer uint32
	Ops      []Op
}

// CompoundResponse is an NFSv4 COMPOUND response.
type CompoundResponse struct {
	Status  int
	Tag     string
	Results []OpResult
}

// Dispatcher processes NFSv4 COMPOUND requests.
type Dispatcher struct {
	backend   storage.Backend
	currentFH []byte // current filehandle for this compound
}

// NewDispatcher creates a COMPOUND dispatcher.
func NewDispatcher(backend storage.Backend) *Dispatcher {
	return &Dispatcher{backend: backend}
}

// Dispatch processes a COMPOUND request and returns the response.
// Per RFC 7530: stop processing at the first failed op.
func (d *Dispatcher) Dispatch(req *CompoundRequest) *CompoundResponse {
	resp := &CompoundResponse{
		Tag:    req.Tag,
		Status: NFS4_OK,
	}

	if len(req.Ops) > maxCompoundOps {
		resp.Status = NFS4ERR_RESOURCE
		return resp
	}

	for _, op := range req.Ops {
		result := d.dispatchOp(op)
		resp.Results = append(resp.Results, result)

		if result.Status != NFS4_OK {
			resp.Status = result.Status
			break // stop at first error per RFC 7530
		}
	}

	return resp
}

func (d *Dispatcher) dispatchOp(op Op) OpResult {
	switch op.OpCode {
	case OpPutRootFH:
		return d.opPutRootFH()
	case OpPutFH:
		return d.opPutFH(op.Data)
	case OpGetFH:
		return d.opGetFH()
	case OpLookup:
		return d.opLookup(op.Data)
	case OpGetAttr:
		return d.opGetAttr(op.Data)
	case OpReadDir:
		return d.opReadDir(op.Data)
	case OpRead:
		return d.opRead(op.Data)
	case OpWrite:
		return d.opWrite(op.Data)
	case OpOpen:
		return d.opOpen(op.Data)
	case OpClose:
		return d.opClose(op.Data)
	case OpSetClientID:
		return d.opSetClientID(op.Data)
	case OpSetClientIDConfirm:
		return d.opSetClientIDConfirm(op.Data)
	default:
		return OpResult{OpCode: op.OpCode, Status: NFS4ERR_INVAL}
	}
}

// --- Op implementations (stubs for now, wired to VFS in Task #13) ---

func (d *Dispatcher) opPutRootFH() OpResult {
	d.currentFH = []byte("root")
	return OpResult{OpCode: OpPutRootFH, Status: NFS4_OK}
}

func (d *Dispatcher) opPutFH(data []byte) OpResult {
	d.currentFH = data
	return OpResult{OpCode: OpPutFH, Status: NFS4_OK}
}

func (d *Dispatcher) opGetFH() OpResult {
	if d.currentFH == nil {
		return OpResult{OpCode: OpGetFH, Status: NFS4ERR_BADHANDLE}
	}
	return OpResult{OpCode: OpGetFH, Status: NFS4_OK, Data: d.currentFH}
}

func (d *Dispatcher) opLookup(_ []byte) OpResult {
	return OpResult{OpCode: OpLookup, Status: NFS4_OK}
}

func (d *Dispatcher) opGetAttr(_ []byte) OpResult {
	return OpResult{OpCode: OpGetAttr, Status: NFS4_OK}
}

func (d *Dispatcher) opReadDir(_ []byte) OpResult {
	return OpResult{OpCode: OpReadDir, Status: NFS4_OK}
}

func (d *Dispatcher) opRead(_ []byte) OpResult {
	return OpResult{OpCode: OpRead, Status: NFS4_OK}
}

func (d *Dispatcher) opWrite(_ []byte) OpResult {
	return OpResult{OpCode: OpWrite, Status: NFS4_OK}
}

func (d *Dispatcher) opOpen(_ []byte) OpResult {
	return OpResult{OpCode: OpOpen, Status: NFS4_OK}
}

func (d *Dispatcher) opClose(_ []byte) OpResult {
	return OpResult{OpCode: OpClose, Status: NFS4_OK}
}

func (d *Dispatcher) opSetClientID(_ []byte) OpResult {
	return OpResult{OpCode: OpSetClientID, Status: NFS4_OK}
}

func (d *Dispatcher) opSetClientIDConfirm(_ []byte) OpResult {
	return OpResult{OpCode: OpSetClientIDConfirm, Status: NFS4_OK}
}
