package nfs4server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"path"
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

var opReadBufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

var compoundReqPool = sync.Pool{New: func() any {
	return &CompoundRequest{Ops: make([]Op, 0, maxCompoundOps)}
}}

var compoundRespPool = sync.Pool{New: func() any {
	return &CompoundResponse{Results: make([]OpResult, 0, maxCompoundOps)}
}}

var dispatcherPool = sync.Pool{New: func() any { return &Dispatcher{} }}

func getDispatcher(backend storage.Backend, state *StateManager) *Dispatcher {
	d := dispatcherPool.Get().(*Dispatcher)
	d.backend = backend
	d.state = state
	d.currentFH = FileHandle{}
	d.currentPath = ""
	return d
}

func putDispatcher(d *Dispatcher) {
	d.backend = nil
	d.state = nil
	dispatcherPool.Put(d)
}

// NFSv4 status codes (RFC 7530)
const (
	NFS4_OK             = 0
	NFS4ERR_PERM        = 1
	NFS4ERR_NOENT       = 2
	NFS4ERR_IO          = 5
	NFS4ERR_NOTDIR      = 20
	NFS4ERR_INVAL       = 22
	NFS4ERR_FBIG        = 27
	NFS4ERR_NOSPC       = 28
	NFS4ERR_ROFS        = 30
	NFS4ERR_STALE       = 70
	NFS4ERR_BADHANDLE   = 10001
	NFS4ERR_BAD_STATEID = 10025
	NFS4ERR_RESOURCE    = 10018
	NFS4ERR_SERVERFAULT = 10006
)

// NFSv4 operation codes
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

	maxCompoundOps = 64
)

// NFS file types
const (
	NF4REG = 1
	NF4DIR = 2
)

// The VFS bucket used for NFSv4 storage.
const nfs4Bucket = "__grainfs_nfs4"

type Op struct {
	OpCode  int
	Data    []byte
	poolKey int // 0=no pool, 8=opArgPool8, 16=opArgPool16
}

type OpResult struct {
	OpCode int
	Status int
	Data   []byte
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
	backend     storage.Backend
	state       *StateManager
	currentFH   FileHandle
	currentPath string
}

func NewDispatcher(backend storage.Backend) *Dispatcher {
	return &Dispatcher{backend: backend}
}

func (d *Dispatcher) Dispatch(req *CompoundRequest, resp *CompoundResponse) {
	resp.Tag = req.Tag
	resp.Status = NFS4_OK

	if len(req.Ops) > maxCompoundOps {
		resp.Status = NFS4ERR_RESOURCE
		return
	}

	for _, op := range req.Ops {
		result := d.dispatchOp(op)
		switch op.poolKey {
		case 8:
			putOpArg8(op.Data)
		case 16:
			putOpArg16(op.Data)
		}
		resp.Results = append(resp.Results, result)
		if result.Status != NFS4_OK {
			resp.Status = result.Status
			break
		}
	}
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
	case OpSetAttr:
		return OpResult{OpCode: OpSetAttr, Status: NFS4_OK, Data: encodeSetAttrResult()}
	case OpRenew:
		return OpResult{OpCode: OpRenew, Status: NFS4_OK}
	default:
		return OpResult{OpCode: op.OpCode, Status: NFS4ERR_INVAL}
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
	name := string(data)
	childPath := path.Join(d.currentPath, name)

	// Check if the child exists as an object
	fh := d.state.GetOrCreateFH(childPath)
	d.currentFH = fh
	d.currentPath = childPath

	return OpResult{OpCode: OpLookup, Status: NFS4_OK}
}

func (d *Dispatcher) opGetAttr(data []byte) OpResult {
	w := getXDRWriter()

	// Return minimal attributes based on the bitmap request
	isRoot := d.currentPath == "/"

	if d.backend != nil && !isRoot {
		key := d.currentPath
		if len(key) > 0 && key[0] == '/' {
			key = key[1:]
		}
		obj, err := d.backend.HeadObject(nfs4Bucket, key)
		if err != nil {
			// Might be a directory
			w.WriteUint32(2)      // bitmap length = 2 words
			w.WriteUint32(0)      // word 0 attrs
			w.WriteUint32(0)      // word 1 attrs
			w.WriteUint32(12)     // attrvals length = 12 bytes (type:4 + size:8)
			w.WriteUint32(NF4DIR) // type
			w.WriteUint64(4096)   // size
			return OpResult{OpCode: OpGetAttr, Status: NFS4_OK, Data: xdrWriterBytes(w)}
		}

		w.WriteUint32(2)
		w.WriteUint32(0)
		w.WriteUint32(0)
		w.WriteUint32(12)               // attrvals length = 12 bytes
		w.WriteUint32(NF4REG)           // type
		w.WriteUint64(uint64(obj.Size)) // size
		return OpResult{OpCode: OpGetAttr, Status: NFS4_OK, Data: xdrWriterBytes(w)}
	}

	// Root directory attributes
	w.WriteUint32(2)      // bitmap length = 2 words
	w.WriteUint32(0)      // word 0
	w.WriteUint32(0)      // word 1
	w.WriteUint32(12)     // attrvals length = 12 bytes (type:4 + size:8)
	w.WriteUint32(NF4DIR) // type
	w.WriteUint64(4096)   // size
	return OpResult{OpCode: OpGetAttr, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

func (d *Dispatcher) opAccess(data []byte) OpResult {
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
	w := getXDRWriter()

	// cookieverf (8 bytes)
	w.WriteUint64(0)

	if d.backend != nil {
		prefix := d.currentPath
		if prefix == "/" {
			prefix = ""
		} else if len(prefix) > 0 && prefix[0] == '/' {
			prefix = prefix[1:]
		}
		if prefix != "" && prefix[len(prefix)-1] != '/' {
			prefix += "/"
		}

		objects, _ := d.backend.ListObjects(nfs4Bucket, prefix, 1000)
		for i, obj := range objects {
			w.WriteUint32(1)             // value_follows = TRUE
			w.WriteUint64(uint64(i + 1)) // cookie
			name := obj.Key
			if prefix != "" {
				name = name[len(prefix):]
			}
			// Skip sub-directory entries (contain '/')
			if idx := bytes.IndexByte([]byte(name), '/'); idx >= 0 {
				continue
			}
			w.WriteString(name)
			// attrs (minimal — empty bitmap + empty vals)
			w.WriteUint32(0) // bitmap len = 0
			w.WriteOpaque(nil)
		}
	}

	w.WriteUint32(0) // value_follows = FALSE (end of entries)
	w.WriteUint32(1) // eof = TRUE

	return OpResult{OpCode: OpReadDir, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

func (d *Dispatcher) opRead(data []byte) OpResult {
	if d.backend == nil || len(data) < 28 {
		return OpResult{OpCode: OpRead, Status: NFS4ERR_SERVERFAULT}
	}

	// Skip stateid (16), read offset (8) + count (4)
	offset := binary.BigEndian.Uint64(data[16:24])
	count := binary.BigEndian.Uint32(data[24:28])

	key := d.currentPath
	if len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	// 파일 크기 가져오기
	obj, err := d.backend.HeadObject(nfs4Bucket, key)
	if err != nil {
		return OpResult{OpCode: OpRead, Status: NFS4ERR_NOENT}
	}
	fileSize := obj.Size

	// 오프셋 이후의 남은 바이트 계산
	remainingSize := fileSize
	if offset < uint64(fileSize) {
		remainingSize = fileSize - int64(offset)
	} else {
		// EOF 너머 읽기 - 빈 데이터와 eof=true 반환
		w := getXDRWriter()
		w.WriteUint32(1) // eof = TRUE
		w.WriteOpaque(nil)
		return OpResult{OpCode: OpRead, Status: NFS4_OK, Data: xdrWriterBytes(w)}
	}

	// count가 지정된 경우 제한
	if uint64(remainingSize) > uint64(count) {
		remainingSize = int64(count)
	}

	// 스트리밍 읽기를 위해 객체 열기
	rc, _, err := d.backend.GetObject(nfs4Bucket, key)
	if err != nil {
		return OpResult{OpCode: OpRead, Status: NFS4ERR_NOENT}
	}
	defer rc.Close()

	// 필요한 경우 오프셋으로 이동
	if offset > 0 {
		if seeker, ok := rc.(io.Seeker); ok {
			_, err = seeker.Seek(int64(offset), io.SeekStart)
			if err != nil {
				rc.Close() // 명시적으로 닫아 resource leak 방지
				return OpResult{OpCode: OpRead, Status: NFS4ERR_IO}
			}
		} else {
			// Seek를 지원하지 않는 Reader - 데이터를 건너뛰기
			discard := make([]byte, 4096)
			remainingToSkip := int64(offset)
			for remainingToSkip > 0 {
				toSkip := remainingToSkip
				if toSkip > int64(len(discard)) {
					toSkip = int64(len(discard))
				}
				n, err := rc.Read(discard[:toSkip])
				if err != nil {
					rc.Close() // 명시적으로 닫아 resource leak 방지
					return OpResult{OpCode: OpRead, Status: NFS4ERR_IO}
				}
				remainingToSkip -= int64(n)
			}
		}
	}

	// 버퍼에 데이터 읽기 (pool 재사용으로 alloc 감소)
	buffer := opReadBufPool.Get().(*bytes.Buffer)
	buffer.Reset()
	defer func() { buffer.Reset(); opReadBufPool.Put(buffer) }()
	_, err = bufferedCopy(buffer, rc, remainingSize)
	if err != nil {
		return OpResult{OpCode: OpRead, Status: NFS4ERR_IO}
	}

	readData := buffer.Bytes()

	// count 제한 적용 (이미 Seek로 offset은 적용됨)
	if uint32(len(readData)) > count {
		readData = readData[:count]
	}

	eof := (offset + uint64(len(readData))) >= uint64(fileSize)

	w := getXDRWriter()
	w.WriteUint32(boolToUint32(eof))
	w.WriteOpaque(readData)

	return OpResult{OpCode: OpRead, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

func (d *Dispatcher) opWrite(data []byte) OpResult {
	if d.backend == nil || len(data) < 28 {
		return OpResult{OpCode: OpWrite, Status: NFS4ERR_SERVERFAULT}
	}

	// Skip stateid (16) + offset (8) + stable (4)
	// Then read the data opaque
	r := NewXDRReader(data[16:])
	r.ReadUint64() // offset
	r.ReadUint32() // stable
	writeData, err := r.ReadOpaque()
	if err != nil {
		return OpResult{OpCode: OpWrite, Status: NFS4ERR_IO}
	}

	// 쓰기 크기 검증 (NFSv4 사양은 단일 WRITE를 1MB로 제한)
	const maxWriteSize = 1 * 1024 * 1024
	if len(writeData) > maxWriteSize {
		return OpResult{OpCode: OpWrite, Status: NFS4ERR_FBIG}
	}

	key := d.currentPath
	if len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	// 추가 버퍼링 없이 직접 쓰기 (writeData는 이미 메모리에 있음)
	// 대용량 쓰기의 경우 불필요한 복사를 피하므로 효율적
	_, err = d.backend.PutObject(nfs4Bucket, key, bytes.NewReader(writeData), "application/octet-stream")
	if err != nil {
		return OpResult{OpCode: OpWrite, Status: NFS4ERR_IO}
	}

	w := getXDRWriter()
	w.WriteUint32(uint32(len(writeData))) // count
	w.WriteUint32(2)                      // committed = FILE_SYNC
	w.WriteUint64(0)                      // writeverf (8 bytes)
	return OpResult{OpCode: OpWrite, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

func (d *Dispatcher) opOpen(data []byte) OpResult {
	if len(data) < 12 {
		return OpResult{OpCode: OpOpen, Status: NFS4ERR_INVAL}
	}

	r := NewXDRReader(data)
	shareAccess, _ := r.ReadUint32()
	openType, _ := r.ReadUint32()
	fileName, _ := r.ReadString()

	_ = shareAccess

	childPath := path.Join(d.currentPath, fileName)

	// If CREATE, create the file
	if openType == 1 && d.backend != nil {
		key := childPath
		if len(key) > 0 && key[0] == '/' {
			key = key[1:]
		}
		_, err := d.backend.HeadObject(nfs4Bucket, key)
		if err != nil {
			d.backend.PutObject(nfs4Bucket, key, bytes.NewReader(nil), "application/octet-stream")
		}
	}

	fh := d.state.GetOrCreateFH(childPath)
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

// --- Helper functions ---

func encodeSetAttrResult() []byte {
	w := getXDRWriter()
	w.WriteUint32(0) // bitmap len = 0 (no attrs set)
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
