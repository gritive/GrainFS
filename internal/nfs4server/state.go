package nfs4server

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
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

// StateManager tracks NFSv4 state: filehandles, clients, and open files.
type StateManager struct {
	mu sync.RWMutex

	// filehandle → path mapping
	fhToPath map[FileHandle]string
	pathToFH map[string]FileHandle

	// client state
	nextClientID atomic.Uint64
	clients      map[uint64]*ClientState

	// open state (stateid tracking)
	nextStateID atomic.Uint64
}

// NewStateManager creates a state manager.
func NewStateManager() *StateManager {
	sm := &StateManager{
		fhToPath: make(map[FileHandle]string),
		pathToFH: make(map[string]FileHandle),
		clients:  make(map[uint64]*ClientState),
	}
	sm.nextClientID.Store(1)
	sm.nextStateID.Store(1)

	// Pre-register root filehandle
	rootFH := FileHandle{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	sm.fhToPath[rootFH] = "/"
	sm.pathToFH["/"] = rootFH

	return sm
}

// GetOrCreateFH returns the filehandle for a path, creating one if needed.
func (sm *StateManager) GetOrCreateFH(path string) FileHandle {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if fh, ok := sm.pathToFH[path]; ok {
		return fh
	}

	fh := generateFH()
	sm.fhToPath[fh] = path
	sm.pathToFH[path] = fh
	return fh
}

// ResolveFH returns the path for a filehandle.
func (sm *StateManager) ResolveFH(fh FileHandle) (string, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	path, ok := sm.fhToPath[fh]
	return path, ok
}

// InvalidateFH removes the filehandle mapping (e.g., after delete).
func (sm *StateManager) InvalidateFH(path string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if fh, ok := sm.pathToFH[path]; ok {
		delete(sm.fhToPath, fh)
		delete(sm.pathToFH, path)
	}
}

// RootFH returns the root filehandle.
func (sm *StateManager) RootFH() FileHandle {
	return FileHandle{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
}

// SetClientID registers a new client and returns the client ID.
func (sm *StateManager) SetClientID(verifier [8]byte) uint64 {
	id := sm.nextClientID.Add(1) - 1
	sm.mu.Lock()
	sm.clients[id] = &ClientState{
		ClientID:  id,
		Confirmed: false,
		Verifier:  verifier,
	}
	sm.mu.Unlock()
	return id
}

// ConfirmClientID confirms a client ID.
func (sm *StateManager) ConfirmClientID(clientID uint64) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
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
