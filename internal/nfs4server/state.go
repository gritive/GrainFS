package nfs4server

import (
	"crypto/rand"
	"encoding/hex"
	pathPkg "path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gritive/GrainFS/internal/pool"
)

// FileHandle is an opaque 128-bit identifier for a file/directory.
// Uses UUID generation (not path-based) to prevent stale handle attacks.
type FileHandle [16]byte

// String returns hex representation.
func (fh FileHandle) String() string {
	return hex.EncodeToString(fh[:])
}

// ClientState tracks NFSv4 per-client state (SETCLIENTID/SETCLIENTID_CONFIRM).
type ClientState struct {
	ClientID  uint64
	Confirmed atomic.Bool // set true by SETCLIENTID_CONFIRM / EXCHANGE_ID
	Verifier  [8]byte
}

// SessionID is the 16-byte NFSv4.1 session identifier.
type SessionID [16]byte

// ChannelAttrs holds negotiated channel parameters.
type ChannelAttrs struct {
	HeaderPadSize         uint32
	MaxRequestSize        uint32
	MaxResponseSize       uint32
	MaxResponseSizeCached uint32
	MaxOperations         uint32
	MaxRequests           uint32
}

// Session tracks NFSv4.1 session state.
type Session struct {
	SessionID   SessionID
	ClientID    uint64
	Sequence    uint32
	ForeChannel ChannelAttrs
	// per-slot replay cache (indexed by slotid)
	Slots []SlotEntry
}

// ExchangeIDResult is the result of an EXCHANGE_ID operation.
type ExchangeIDResult struct {
	ClientID   uint64
	SequenceID uint32
}

// StateManager tracks NFSv4 state: filehandles, clients, and open files.
//
// Filehandle maps live under a single RWMutex. The previous CoW design
// allocated O(N) per insert (full map clone), which made small-file
// create-storm workloads (e.g., container build extracting tens of thousands
// of paths) burn GBs in `maps.Copy`. Per-op RLock cost is negligible vs
// network/syscall latency on the NFS path.
type StateManager struct {
	fhMu     sync.RWMutex
	fhToPath map[FileHandle]string
	pathToFH map[string]FileHandle

	nextClientID atomic.Uint64
	clients      sync.Map // map[uint64]*ClientState — lock-free reads

	nextStateID atomic.Uint64

	// NFSv4.1 session and EXCHANGE_ID state (lock-free reads).
	sessions sync.Map // map[SessionID]*Session
	exchSeq  sync.Map // map[uint64]*atomic.Uint32 — EXCHANGE_ID sequence per clientID

	// slotMu serialises per-slot replay-cache updates in opSequence.
	// Session lookup (sessions.Load) is lock-free; only slot field writes need this.
	slotMu sync.Mutex

	// dirs tracks paths that exist as directories.
	// Value type is int64 (UnixNano creation timestamp) for CHANGE attribute.
	dirs pool.SyncMap[string, int64]

	// writeGates holds per-path channel semaphores to serialise RMW writes.
	// Value type is chan struct{} (buffered 1).
	writeGates pool.SyncMap[string, chan struct{}]

	// fileMeta caches NFS-specific sidecar metadata by bucket/object key.
	fileMeta pool.SyncMap[string, nfsFileMeta]

	// WriteVerf is the 8-byte write verifier returned in COMMIT responses.
	// Initialized once with crypto/rand per server instance; changes on restart.
	WriteVerf [8]byte
}

// NewStateManager creates a state manager.
func NewStateManager() *StateManager {
	sm := &StateManager{
		fhToPath: make(map[FileHandle]string),
		pathToFH: make(map[string]FileHandle),
	}
	sm.nextClientID.Store(1)
	sm.nextStateID.Store(1)
	if _, err := rand.Read(sm.WriteVerf[:]); err != nil {
		panic(err)
	}

	rootFH := FileHandle{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	sm.fhToPath[rootFH] = "/"
	sm.pathToFH["/"] = rootFH

	sm.dirs.Store("/", time.Now().UnixNano())

	return sm
}

// MarkDir records path as a known directory with current timestamp.
func (sm *StateManager) MarkDir(p string) {
	sm.dirs.Store(p, time.Now().UnixNano())
}

// IsDir reports whether path is a known directory.
func (sm *StateManager) IsDir(p string) bool {
	_, ok := sm.dirs.Load(p)
	return ok
}

// DirMtime returns the creation UnixNano timestamp for a directory,
// or time.Now().UnixNano() if not tracked (e.g. the root).
func (sm *StateManager) DirMtime(p string) int64 {
	if v, ok := sm.dirs.Load(p); ok {
		return v
	}
	return time.Now().UnixNano()
}

// LockPath acquires a per-path channel semaphore and returns a release func.
// Use for serialising concurrent read-modify-write operations on the same path.
func (sm *StateManager) LockPath(p string) func() {
	ch, _ := sm.writeGates.LoadOrStore(p, make(chan struct{}, 1))
	ch <- struct{}{} // blocks until acquired
	return func() { <-ch }
}

// RemoveDir removes path from the directory set.
func (sm *StateManager) RemoveDir(p string) {
	sm.dirs.Delete(p)
}

// GetOrCreateFH returns the filehandle for a path, creating one if needed.
func (sm *StateManager) GetOrCreateFH(path string) FileHandle {
	sm.fhMu.RLock()
	if fh, ok := sm.pathToFH[path]; ok {
		sm.fhMu.RUnlock()
		return fh
	}
	sm.fhMu.RUnlock()

	sm.fhMu.Lock()
	defer sm.fhMu.Unlock()
	if fh, ok := sm.pathToFH[path]; ok { // re-check after upgrading
		return fh
	}
	fh := generateFH()
	sm.fhToPath[fh] = path
	sm.pathToFH[path] = fh
	return fh
}

// ResolveFH returns the path for a filehandle.
func (sm *StateManager) ResolveFH(fh FileHandle) (string, bool) {
	sm.fhMu.RLock()
	path, ok := sm.fhToPath[fh]
	sm.fhMu.RUnlock()
	return path, ok
}

// InvalidateKey clears NFS metadata caches for an out-of-band mutation on
// an object key (e.g. an S3 PUT replicated via Raft from another cluster
// node). The fileMeta cache is dropped so the next stat re-fetches the
// sidecar; the parent directory's cached mtime is refreshed so the next
// READDIR sees a new generation and re-lists. Filehandle mappings are
// intentionally left in place because the authoritative state is the
// backend object — a stale fh will surface as NotFound on the next access
// rather than producing wrong data, and concurrent NFS clients keep their
// open handles valid.
//
// Caller maps the source bucket+key to "/"+key (the NFS-path convention)
// before computing the parent. We only refresh dirs that the StateManager
// already tracks, mirroring the standalone NFS write path which marks
// directories lazily as they are observed.
func (sm *StateManager) InvalidateKey(key string) {
	sm.fileMeta.Delete(key)
	nfsPath := "/" + key
	parent := pathPkg.Dir(nfsPath)
	if _, ok := sm.dirs.Load(parent); ok {
		sm.dirs.Store(parent, time.Now().UnixNano())
	}
}

func (sm *StateManager) InvalidateObject(bucket, key string) {
	sm.fileMeta.Delete(fileMetaCacheKey(bucket, key))
	for _, parent := range []string{pathPkg.Dir("/" + bucket + "/" + key), pathPkg.Dir("/" + key)} {
		if _, ok := sm.dirs.Load(parent); ok {
			sm.dirs.Store(parent, time.Now().UnixNano())
		}
	}
}

// InvalidateFH removes the filehandle mapping (e.g., after delete).
func (sm *StateManager) InvalidateFH(path string) {
	sm.fhMu.Lock()
	defer sm.fhMu.Unlock()
	fh, ok := sm.pathToFH[path]
	if !ok {
		return
	}
	delete(sm.fhToPath, fh)
	delete(sm.pathToFH, path)
}

// RootFH returns the root filehandle.
func (sm *StateManager) RootFH() FileHandle {
	return FileHandle{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
}

// SetClientID registers a new client and returns the client ID.
func (sm *StateManager) SetClientID(verifier [8]byte) uint64 {
	id := sm.nextClientID.Add(1) - 1
	cs := &ClientState{ClientID: id, Verifier: verifier}
	// Confirmed stays false (zero value of atomic.Bool)
	sm.clients.Store(id, cs)
	return id
}

// ConfirmClientID confirms a client ID.
func (sm *StateManager) ConfirmClientID(clientID uint64) bool {
	v, ok := sm.clients.Load(clientID)
	if !ok {
		return false
	}
	v.(*ClientState).Confirmed.Store(true)
	return true
}

func generateFH() FileHandle {
	var fh FileHandle
	if _, err := rand.Read(fh[:]); err != nil {
		panic(err)
	}
	return fh
}

// ExchangeID registers a NFSv4.1 client and returns (clientID, sequenceID).
// Idempotent: same verifier → same clientID.
func (sm *StateManager) ExchangeID(verifier [8]byte, clientOwnerID []byte) ExchangeIDResult {
	id := sm.nextClientID.Add(1) - 1
	cs := &ClientState{ClientID: id, Verifier: verifier}
	cs.Confirmed.Store(true)
	sm.clients.Store(id, cs)

	counter, _ := sm.exchSeq.LoadOrStore(id, new(atomic.Uint32))
	seq := counter.(*atomic.Uint32).Add(1) - 1

	return ExchangeIDResult{ClientID: id, SequenceID: seq}
}

// CreateSession creates a NFSv4.1 session for the given clientID.
func (sm *StateManager) CreateSession(clientID uint64, fore ChannelAttrs) (SessionID, uint32) {
	var sid SessionID
	if _, err := rand.Read(sid[:]); err != nil {
		panic(err)
	}

	maxSlots := fore.MaxRequests
	if maxSlots == 0 || maxSlots > 16 {
		maxSlots = 16
	}

	sess := &Session{
		SessionID:   sid,
		ClientID:    clientID,
		ForeChannel: fore,
		Slots:       make([]SlotEntry, maxSlots),
	}

	sm.sessions.Store(sid, sess)

	return sid, 0
}

// GetSession returns the session for a session ID, or nil if not found.
func (sm *StateManager) GetSession(sid SessionID) *Session {
	v, ok := sm.sessions.Load(sid)
	if !ok {
		return nil
	}
	return v.(*Session)
}

// DestroySession removes a session.
func (sm *StateManager) DestroySession(sid SessionID) bool {
	_, ok := sm.sessions.LoadAndDelete(sid)
	return ok
}

// ClientExists reports whether clientID is registered and confirmed.
func (sm *StateManager) ClientExists(clientID uint64) bool {
	v, ok := sm.clients.Load(clientID)
	return ok && v.(*ClientState).Confirmed.Load()
}

// DestroyClientID removes all sessions for clientID and the client record.
func (sm *StateManager) DestroyClientID(clientID uint64) {
	sm.sessions.Range(func(k, v any) bool {
		if v.(*Session).ClientID == clientID {
			sm.sessions.Delete(k)
		}
		return true
	})
	sm.clients.Delete(clientID)
}

// FreeStateID is a no-op stub; GrainFS does not track fine-grained stateids.
func (sm *StateManager) FreeStateID(_ uint64) {}

// TestStateIDs returns NFS4_OK for each stateid (no expiry tracking).
func (sm *StateManager) TestStateIDs(count int) []uint32 {
	statuses := make([]uint32, count)
	for i := range statuses {
		statuses[i] = NFS4_OK
	}
	return statuses
}
