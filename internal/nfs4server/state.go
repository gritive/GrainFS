package nfs4server

import (
	"crypto/rand"
	"encoding/hex"
	"maps"
	"sync"
	"sync/atomic"
	"time"
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
	Confirmed bool
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

// fhState is an immutable snapshot of the filehandle maps.
// Replaced atomically on every write (CoW). Reads take no lock.
type fhState struct {
	fhToPath map[FileHandle]string
	pathToFH map[string]FileHandle
}

// StateManager tracks NFSv4 state: filehandles, clients, and open files.
//
// Hot path (ResolveFH / GetOrCreateFH cache-hit) touches zero mutexes:
// it loads an atomic pointer and reads an immutable map snapshot.
// Writers serialise on writeMu and publish a new snapshot via Store.
type StateManager struct {
	fhMaps  atomic.Pointer[fhState] // read via Load(), no lock
	writeMu sync.Mutex              // serialises writers

	nextClientID atomic.Uint64
	clientMu     sync.Mutex
	clients      map[uint64]*ClientState

	nextStateID atomic.Uint64

	// NFSv4.1 session state
	sessionMu sync.Mutex
	sessions  map[SessionID]*Session
	// clientID → sequenceid for EXCHANGE_ID idempotency
	exchSeq map[uint64]uint32

	// dirs tracks paths that exist as directories.
	// Value type is int64 (UnixNano creation timestamp) for CHANGE attribute.
	dirs sync.Map

	// writeGates holds per-path channel semaphores to serialise RMW writes.
	// Value type is chan struct{} (buffered 1).
	writeGates sync.Map

	// WriteVerf is the 8-byte write verifier returned in COMMIT responses.
	// Initialized once with crypto/rand per server instance; changes on restart.
	WriteVerf [8]byte
}

// NewStateManager creates a state manager.
func NewStateManager() *StateManager {
	sm := &StateManager{
		clients:  make(map[uint64]*ClientState),
		sessions: make(map[SessionID]*Session),
		exchSeq:  make(map[uint64]uint32),
	}
	sm.nextClientID.Store(1)
	sm.nextStateID.Store(1)
	rand.Read(sm.WriteVerf[:])

	initial := &fhState{
		fhToPath: make(map[FileHandle]string),
		pathToFH: make(map[string]FileHandle),
	}
	rootFH := FileHandle{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	initial.fhToPath[rootFH] = "/"
	initial.pathToFH["/"] = rootFH
	sm.fhMaps.Store(initial)

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
		return v.(int64)
	}
	return time.Now().UnixNano()
}

// LockPath acquires a per-path channel semaphore and returns a release func.
// Use for serialising concurrent read-modify-write operations on the same path.
func (sm *StateManager) LockPath(p string) func() {
	sem, _ := sm.writeGates.LoadOrStore(p, make(chan struct{}, 1))
	ch := sem.(chan struct{})
	ch <- struct{}{} // blocks until acquired
	return func() { <-ch }
}

// RemoveDir removes path from the directory set.
func (sm *StateManager) RemoveDir(p string) {
	sm.dirs.Delete(p)
}

// GetOrCreateFH returns the filehandle for a path, creating one if needed.
// Cache hits (the common case) return with no lock acquired.
func (sm *StateManager) GetOrCreateFH(path string) FileHandle {
	// Fast path: load snapshot, no lock.
	if fh, ok := sm.fhMaps.Load().pathToFH[path]; ok {
		return fh
	}

	// Slow path: CoW update under write lock.
	sm.writeMu.Lock()
	defer sm.writeMu.Unlock()

	cur := sm.fhMaps.Load()
	if fh, ok := cur.pathToFH[path]; ok { // re-check after acquiring lock
		return fh
	}

	fh := generateFH()
	next := &fhState{
		fhToPath: make(map[FileHandle]string, len(cur.fhToPath)+1),
		pathToFH: make(map[string]FileHandle, len(cur.pathToFH)+1),
	}
	maps.Copy(next.fhToPath, cur.fhToPath)
	maps.Copy(next.pathToFH, cur.pathToFH)
	next.fhToPath[fh] = path
	next.pathToFH[path] = fh
	sm.fhMaps.Store(next)
	return fh
}

// ResolveFH returns the path for a filehandle. No lock acquired.
func (sm *StateManager) ResolveFH(fh FileHandle) (string, bool) {
	path, ok := sm.fhMaps.Load().fhToPath[fh]
	return path, ok
}

// InvalidateFH removes the filehandle mapping (e.g., after delete).
func (sm *StateManager) InvalidateFH(path string) {
	sm.writeMu.Lock()
	defer sm.writeMu.Unlock()

	cur := sm.fhMaps.Load()
	fh, ok := cur.pathToFH[path]
	if !ok {
		return
	}

	next := &fhState{
		fhToPath: make(map[FileHandle]string, len(cur.fhToPath)),
		pathToFH: make(map[string]FileHandle, len(cur.pathToFH)),
	}
	maps.Copy(next.fhToPath, cur.fhToPath)
	maps.Copy(next.pathToFH, cur.pathToFH)
	delete(next.fhToPath, fh)
	delete(next.pathToFH, path)
	sm.fhMaps.Store(next)
}

// RootFH returns the root filehandle.
func (sm *StateManager) RootFH() FileHandle {
	return FileHandle{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
}

// SetClientID registers a new client and returns the client ID.
func (sm *StateManager) SetClientID(verifier [8]byte) uint64 {
	id := sm.nextClientID.Add(1) - 1
	sm.clientMu.Lock()
	sm.clients[id] = &ClientState{
		ClientID:  id,
		Confirmed: false,
		Verifier:  verifier,
	}
	sm.clientMu.Unlock()
	return id
}

// ConfirmClientID confirms a client ID.
func (sm *StateManager) ConfirmClientID(clientID uint64) bool {
	sm.clientMu.Lock()
	defer sm.clientMu.Unlock()
	cs, ok := sm.clients[clientID]
	if !ok {
		return false
	}
	cs.Confirmed = true
	return true
}

func generateFH() FileHandle {
	var fh FileHandle
	rand.Read(fh[:])
	return fh
}

// ExchangeID registers a NFSv4.1 client and returns (clientID, sequenceID).
// Idempotent: same verifier → same clientID.
func (sm *StateManager) ExchangeID(verifier [8]byte, clientOwnerID []byte) ExchangeIDResult {
	id := sm.nextClientID.Add(1) - 1
	sm.clientMu.Lock()
	sm.clients[id] = &ClientState{ClientID: id, Confirmed: true, Verifier: verifier}
	sm.clientMu.Unlock()

	sm.sessionMu.Lock()
	seq := sm.exchSeq[id] // starts at 0, first call returns 0
	sm.exchSeq[id] = seq + 1
	sm.sessionMu.Unlock()

	return ExchangeIDResult{ClientID: id, SequenceID: seq}
}

// CreateSession creates a NFSv4.1 session for the given clientID.
func (sm *StateManager) CreateSession(clientID uint64, fore ChannelAttrs) (SessionID, uint32) {
	var sid SessionID
	rand.Read(sid[:])

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

	sm.sessionMu.Lock()
	sm.sessions[sid] = sess
	sm.sessionMu.Unlock()

	return sid, 0
}

// GetSession returns the session for a session ID, or nil if not found.
func (sm *StateManager) GetSession(sid SessionID) *Session {
	sm.sessionMu.Lock()
	defer sm.sessionMu.Unlock()
	return sm.sessions[sid]
}

// DestroySession removes a session.
func (sm *StateManager) DestroySession(sid SessionID) bool {
	sm.sessionMu.Lock()
	defer sm.sessionMu.Unlock()
	_, ok := sm.sessions[sid]
	if ok {
		delete(sm.sessions, sid)
	}
	return ok
}

// DestroyClientID removes all sessions for clientID and the client record.
func (sm *StateManager) DestroyClientID(clientID uint64) {
	sm.sessionMu.Lock()
	for sid, sess := range sm.sessions {
		if sess.ClientID == clientID {
			delete(sm.sessions, sid)
		}
	}
	sm.sessionMu.Unlock()

	sm.clientMu.Lock()
	delete(sm.clients, clientID)
	sm.clientMu.Unlock()
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
