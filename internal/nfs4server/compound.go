package nfs4server

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

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
	d.savedFH = FileHandle{}
	d.savedPath = ""
	d.minorVer = 0
	return d
}

func putDispatcher(d *Dispatcher) {
	d.backend = nil
	d.state = nil
	dispatcherPool.Put(d)
}

// NFSv4 status codes (RFC 7530)
const (
	NFS4_OK                     = 0
	NFS4ERR_PERM                = 1
	NFS4ERR_NOENT               = 2
	NFS4ERR_IO                  = 5
	NFS4ERR_NOTDIR              = 20
	NFS4ERR_INVAL               = 22
	NFS4ERR_FBIG                = 27
	NFS4ERR_NOSPC               = 28
	NFS4ERR_ROFS                = 30
	NFS4ERR_STALE               = 70
	NFS4ERR_BADHANDLE           = 10001
	NFS4ERR_BAD_STATEID         = 10025
	NFS4ERR_RESOURCE            = 10018
	NFS4ERR_SERVERFAULT         = 10006
	NFS4ERR_NOTSUPP             = 10004
	NFS4ERR_RESTOREFH           = 10030
	NFS4ERR_NOFILEHANDLE        = 10020
	NFS4ERR_BADSESSION          = 10052
	NFS4ERR_BADSLOT             = 10060
	NFS4ERR_SEQ_MISORDERED      = 10063
	NFS4ERR_MINOR_VERS_MISMATCH = 10021
	NFS4ERR_OP_ILLEGAL          = 10044
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
	savedFH     FileHandle
	savedPath   string
	minorVer    uint32
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
		}
		resp.Results = append(resp.Results, result)
		if result.Status != NFS4_OK {
			resp.Status = result.Status
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
	if name == "" || name == "." || name == ".." {
		return OpResult{OpCode: OpLookup, Status: NFS4ERR_INVAL}
	}
	childPath := path.Join(d.currentPath, name)

	// Check existence: backend file, tracked directory, or root
	exists := d.state.IsDir(childPath)
	if !exists && d.backend != nil {
		key := childPath
		if len(key) > 0 && key[0] == '/' {
			key = key[1:]
		}
		_, err := d.backend.HeadObject(nfs4Bucket, key)
		exists = err == nil
	}
	log.Debug().Str("name", name).Str("child", childPath).Bool("exists", exists).Msg("nfs4: LOOKUP")
	if !exists {
		return OpResult{OpCode: OpLookup, Status: NFS4ERR_NOENT}
	}

	fh := d.state.GetOrCreateFH(childPath)
	d.currentFH = fh
	d.currentPath = childPath
	return OpResult{OpCode: OpLookup, Status: NFS4_OK}
}

func (d *Dispatcher) opCreate(data []byte) OpResult {
	// data pre-processed by readOpArgs: objType(uint32) + objname(string)
	if len(data) < 8 {
		return OpResult{OpCode: OpCreate, Status: NFS4ERR_INVAL}
	}
	r := NewXDRReader(data)
	objType, _ := r.ReadUint32()
	objName, _ := r.ReadString()
	if objName == "" || objName == "." || objName == ".." {
		return OpResult{OpCode: OpCreate, Status: NFS4ERR_INVAL}
	}

	newPath := path.Join(d.currentPath, objName)

	if objType == NF4DIR {
		d.state.MarkDir(newPath)
	}
	fh := d.state.GetOrCreateFH(newPath)
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
	// Parse client's requested bitmap (GETATTR4args.attr_request = bitmap4<>)
	var reqBit [2]uint32
	if len(data) >= 4 {
		r := NewXDRReader(data)
		bitmapLen, _ := r.ReadUint32()
		if bitmapLen >= 1 {
			reqBit[0], _ = r.ReadUint32()
		}
		if bitmapLen >= 2 {
			reqBit[1], _ = r.ReadUint32()
		}
	}

	hasBit := func(bit uint) bool {
		if bit < 32 {
			return reqBit[0]&(1<<bit) != 0
		}
		return reqBit[1]&(1<<(bit-32)) != 0
	}

	// Resolve metadata for the current path
	isDir := d.state.IsDir(d.currentPath)
	var fileSize uint64
	var lastModUnix int64
	var sidecarMode uint32
	fileid := pathToFileID(d.currentPath)

	if !isDir && d.backend != nil {
		key := d.currentPath
		if len(key) > 0 && key[0] == '/' {
			key = key[1:]
		}
		obj, err := d.backend.HeadObject(nfs4Bucket, key)
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
			meta := d.loadFileMeta(key)
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
		lastModUnix = d.state.DirMtime(d.currentPath) / 1e9
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
	var respBit [2]uint32

	setBit := func(bit uint) {
		if bit < 32 {
			respBit[0] |= 1 << bit
		} else {
			respBit[1] |= 1 << (bit - 32)
		}
	}

	// Bit 0: SUPPORTED_ATTRS — bitmap4<> listing all attrs we support
	if hasBit(0) {
		setBit(0)
		supW0 := uint32(1<<0 | 1<<1 | 1<<2 | 1<<3 | 1<<4 | 1<<5 | 1<<6 |
			1<<7 | 1<<8 | 1<<9 | 1<<10 | 1<<13 | 1<<14 | 1<<20 | 1<<27 | 1<<30 | 1<<31)
		supW1 := uint32(1<<1 | 1<<3 | 1<<4 | 1<<5 | 1<<13 | 1<<15 | 1<<16 | 1<<20 | 1<<21 | 1<<22 | 1<<23)
		attrW.WriteUint32(2)
		attrW.WriteUint32(supW0)
		attrW.WriteUint32(supW1)
	}
	// Bit 1: TYPE — ftype4
	if hasBit(1) {
		setBit(1)
		attrW.WriteUint32(fileType)
	}
	// Bit 2: FH_EXPIRE_TYPE — FH4_PERSISTENT = 0
	if hasBit(2) {
		setBit(2)
		attrW.WriteUint32(0)
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
		attrW.WriteUint32(1)
	}
	// Bit 6: SYMLINK_SUPPORT — bool
	if hasBit(6) {
		setBit(6)
		attrW.WriteUint32(1)
	}
	// Bit 7: NAMED_ATTR — bool
	if hasBit(7) {
		setBit(7)
		attrW.WriteUint32(0)
	}
	// Bit 8: FSID — {major:uint64, minor:uint64}
	if hasBit(8) {
		setBit(8)
		attrW.WriteUint64(0)
		attrW.WriteUint64(1)
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
	// Bit 14: CANSETTIME — bool
	if hasBit(14) {
		setBit(14)
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
		attrW.WriteUint64(fileid)
	}

	attrBytes := xdrWriterBytes(attrW)

	// GETATTR4resok: bitmap4 attrmask + attrlist4 attr_vals
	w := getXDRWriter()
	w.WriteUint32(2)
	w.WriteUint32(respBit[0])
	w.WriteUint32(respBit[1])
	w.WriteOpaque(attrBytes)
	return OpResult{OpCode: OpGetAttr, Status: NFS4_OK, Data: xdrWriterBytes(w)}
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

func (d *Dispatcher) opReadDir(_ []byte) OpResult {
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
				toSkip := min(remainingToSkip, int64(len(discard)))
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

	r := NewXDRReader(data[16:]) // skip stateid (16 bytes)
	offset, _ := r.ReadUint64()
	r.ReadUint32() // stable
	writeData, err := r.ReadOpaque()
	if err != nil {
		return OpResult{OpCode: OpWrite, Status: NFS4ERR_IO}
	}

	const maxWriteSize = 1 * 1024 * 1024
	if len(writeData) > maxWriteSize {
		return OpResult{OpCode: OpWrite, Status: NFS4ERR_FBIG}
	}
	log.Debug().Str("path", d.currentPath).Uint64("offset", offset).Int("len", len(writeData)).Msg("nfs4: WRITE")

	key := d.currentPath
	if len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	if offset == 0 {
		_, err = d.backend.PutObject(nfs4Bucket, key, bytes.NewReader(writeData), "application/octet-stream")
	} else {
		// Serialise concurrent RMW on the same path via per-path channel semaphore.
		release := d.state.LockPath(key)
		defer release()

		var existing []byte
		if rc, _, rerr := d.backend.GetObject(nfs4Bucket, key); rerr == nil {
			existing, err = io.ReadAll(rc)
			rc.Close()
			if err != nil {
				return OpResult{OpCode: OpWrite, Status: NFS4ERR_IO}
			}
		}
		end := offset + uint64(len(writeData))
		if uint64(len(existing)) < end {
			existing = append(existing, make([]byte, end-uint64(len(existing)))...)
		}
		copy(existing[offset:], writeData)
		_, err = d.backend.PutObject(nfs4Bucket, key, bytes.NewReader(existing), "application/octet-stream")
	}
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
	if len(data) < 8 {
		return OpResult{OpCode: OpOpen, Status: NFS4ERR_INVAL}
	}

	// data is pre-processed by readOpArgs(xdr.go): shareAccess(4) + openType(4) + fileName(string)
	r := NewXDRReader(data)
	shareAccess, _ := r.ReadUint32()
	openType, _ := r.ReadUint32() // 0=NOCREATE, 1=CREATE
	fileName, _ := r.ReadString()

	childPath := path.Join(d.currentPath, fileName)
	log.Debug().Str("file", fileName).Str("child", childPath).Uint32("access", shareAccess).Uint32("type", openType).Msg("nfs4: OPEN")

	// If CREATE, ensure the object exists
	if openType == 1 && d.backend != nil {
		key := childPath
		if len(key) > 0 && key[0] == '/' {
			key = key[1:]
		}
		_, headErr := d.backend.HeadObject(nfs4Bucket, key)
		if headErr != nil {
			if _, putErr := d.backend.PutObject(nfs4Bucket, key, bytes.NewReader(nil), "application/octet-stream"); putErr != nil {
				return OpResult{OpCode: OpOpen, Status: NFS4ERR_IO}
			}
			log.Debug().Str("key", key).Msg("nfs4: OPEN CREATE created file")
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
	if d.currentPath == "" {
		return OpResult{OpCode: OpSetAttr, Status: NFS4ERR_NOFILEHANDLE}
	}
	// data layout: stateid(16) + bm0(4) + bm1(4) + attrVals(opaque)
	if len(data) < 24 {
		return OpResult{OpCode: OpSetAttr, Status: NFS4ERR_INVAL}
	}
	r := NewXDRReader(data[16:]) // skip stateid
	bm0, _ := r.ReadUint32()
	bm1, _ := r.ReadUint32()
	attrVals, _ := r.ReadOpaque()

	key := d.currentPath
	if len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	ar := NewXDRReader(attrVals)

	// FATTR4_SIZE (word0 bit 4)
	if bm0&(1<<4) != 0 {
		size, _ := ar.ReadUint64()
		release := d.state.LockPath(key)
		defer release()
		if tr, ok := d.backend.(storage.Truncatable); ok {
			if err := tr.Truncate(nfs4Bucket, key, int64(size)); err != nil {
				return OpResult{OpCode: OpSetAttr, Status: NFS4ERR_IO}
			}
		} else {
			var existing []byte
			if rc, _, err := d.backend.GetObject(nfs4Bucket, key); err == nil {
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
			if _, err := d.backend.PutObject(nfs4Bucket, key, bytes.NewReader(existing), "application/octet-stream"); err != nil {
				return OpResult{OpCode: OpSetAttr, Status: NFS4ERR_IO}
			}
		}
	}

	meta := d.loadFileMeta(key)
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
		if err := d.saveFileMeta(key, meta); err != nil {
			return OpResult{OpCode: OpSetAttr, Status: NFS4ERR_IO}
		}
	}

	return OpResult{OpCode: OpSetAttr, Status: NFS4_OK, Data: encodeSetAttrResult(bm0, bm1)}
}

func (d *Dispatcher) opCommit() OpResult {
	if d.currentPath == "" {
		return OpResult{OpCode: OpCommit, Status: NFS4ERR_NOFILEHANDLE}
	}
	key := d.currentPath
	if len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}
	if s, ok := d.backend.(storage.Syncable); ok {
		if err := s.Sync(nfs4Bucket, key); err != nil {
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
	name := string(data)
	if name == "" {
		return OpResult{OpCode: OpRemove, Status: NFS4ERR_INVAL}
	}
	targetPath := path.Join(d.currentPath, name)
	key := targetPath
	if len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}

	if _, err := d.backend.HeadObject(nfs4Bucket, key); err != nil {
		// May be a directory tracked in state only.
		if d.state.IsDir(targetPath) {
			d.state.RemoveDir(targetPath)
			d.state.InvalidateFH(targetPath)
			now := uint64(time.Now().UnixNano())
			w := getXDRWriter()
			w.WriteUint32(1)
			w.WriteUint64(now)
			w.WriteUint64(now)
			return OpResult{OpCode: OpRemove, Status: NFS4_OK, Data: xdrWriterBytes(w)}
		}
		return OpResult{OpCode: OpRemove, Status: NFS4ERR_NOENT}
	}
	if err := d.backend.DeleteObject(nfs4Bucket, key); err != nil {
		return OpResult{OpCode: OpRemove, Status: NFS4ERR_IO}
	}
	d.state.InvalidateFH(targetPath)

	w := getXDRWriter()
	// change_info4: atomic + before + after
	w.WriteUint32(1)
	w.WriteUint64(uint64(time.Now().UnixNano()))
	w.WriteUint64(uint64(time.Now().UnixNano()))
	return OpResult{OpCode: OpRemove, Status: NFS4_OK, Data: xdrWriterBytes(w)}
}

func (d *Dispatcher) opRename(data []byte) OpResult {
	r := NewXDRReader(data)
	oldName, err := r.ReadString()
	if err != nil {
		return OpResult{OpCode: OpRename, Status: NFS4ERR_INVAL}
	}
	newName, err := r.ReadString()
	if err != nil {
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
	oldKey := oldPath
	if len(oldKey) > 0 && oldKey[0] == '/' {
		oldKey = oldKey[1:]
	}
	newKey := newPath
	if len(newKey) > 0 && newKey[0] == '/' {
		newKey = newKey[1:]
	}

	// Object store has no rename — read → write new → delete old.
	rc, _, err := d.backend.GetObject(nfs4Bucket, oldKey)
	if err != nil {
		return OpResult{OpCode: OpRename, Status: NFS4ERR_NOENT}
	}
	if _, err := d.backend.PutObject(nfs4Bucket, newKey, rc, "application/octet-stream"); err != nil {
		rc.Close()
		return OpResult{OpCode: OpRename, Status: NFS4ERR_IO}
	}
	rc.Close()
	d.backend.DeleteObject(nfs4Bucket, oldKey) //nolint:errcheck

	d.state.InvalidateFH(oldPath)
	d.state.GetOrCreateFH(newPath)

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

	w := getXDRWriter()
	w.WriteUint64(res.ClientID)   // eir_clientid
	w.WriteUint32(res.SequenceID) // eir_sequenceid
	w.WriteUint32(0)              // eir_flags
	w.WriteUint32(0)              // eir_state_protect.spr_how = SP4_NONE
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
	r := NewXDRReader(data)
	var sidBytes [16]byte
	io.ReadFull(&r.r, sidBytes[:]) //nolint:errcheck
	var sid SessionID
	copy(sid[:], sidBytes[:])

	seqID, _ := r.ReadUint32()
	slotID, _ := r.ReadUint32()
	highSlot, _ := r.ReadUint32()
	cacheThis, _ := r.ReadUint32()

	sess := d.state.GetSession(sid)
	if sess == nil {
		return OpResult{OpCode: OpSequence, Status: NFS4ERR_BADSESSION}
	}

	d.state.sessionMu.Lock()
	if int(slotID) >= len(sess.Slots) {
		d.state.sessionMu.Unlock()
		return OpResult{OpCode: OpSequence, Status: NFS4ERR_BADSLOT}
	}
	slot := &sess.Slots[slotID]

	// Replay detection: same seqID → return cached response if available.
	if slot.SeqID == seqID && (slot.HasCache || slot.SeqID > 0) {
		if slot.HasCache {
			cached := slot.Response
			d.state.sessionMu.Unlock()
			return OpResult{OpCode: OpSequence, Status: NFS4_OK, Data: cached}
		}
		// cacheThis was false on first call; replay without cached bytes is still OK.
		d.state.sessionMu.Unlock()
		// fall through to re-build response below
	} else if slot.SeqID > 0 && seqID != slot.SeqID+1 {
		// Stale or future seqID (not the expected next or a replay).
		d.state.sessionMu.Unlock()
		return OpResult{OpCode: OpSequence, Status: NFS4ERR_SEQ_MISORDERED}
	} else {
		d.state.sessionMu.Unlock()
	}

	w := getXDRWriter()
	w.buf.Write(sid[:])     // sr_sessionid: fixed 16 bytes (no length prefix)
	w.WriteUint32(seqID)    // sr_sequenceid
	w.WriteUint32(slotID)   // sr_slotid
	w.WriteUint32(highSlot) // sr_highest_slotid
	w.WriteUint32(highSlot) // sr_target_highest_slotid
	w.WriteUint32(0)        // sr_status_flags
	resp := xdrWriterBytes(w)

	d.state.sessionMu.Lock()
	slot.SeqID = seqID
	if cacheThis != 0 {
		slot.Response = resp
		slot.HasCache = true
	} else {
		slot.HasCache = false
	}
	d.state.sessionMu.Unlock()

	return OpResult{OpCode: OpSequence, Status: NFS4_OK, Data: resp}
}

// nfsFileMeta stores per-file NFS metadata that must survive server restarts.
// Persisted as JSON in the nfs4Bucket under key "__meta/<path>".
type nfsFileMeta struct {
	Mode  uint32 `json:"mode"`
	Mtime int64  `json:"mtime_ns"` // UnixNano; 0 means "use object LastModified"
}

func metaSidecarKey(key string) string {
	return "__meta/" + key
}

func (d *Dispatcher) loadFileMeta(key string) nfsFileMeta {
	rc, _, err := d.backend.GetObject(nfs4Bucket, metaSidecarKey(key))
	if err != nil {
		return nfsFileMeta{Mode: 0644}
	}
	defer rc.Close()
	data, _ := io.ReadAll(rc)
	var m nfsFileMeta
	if err := json.Unmarshal(data, &m); err != nil || m.Mode == 0 {
		m.Mode = 0644
	}
	return m
}

func (d *Dispatcher) saveFileMeta(key string, m nfsFileMeta) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = d.backend.PutObject(nfs4Bucket, metaSidecarKey(key), bytes.NewReader(data), "application/json")
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
