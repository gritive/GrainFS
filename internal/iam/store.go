package iam

import (
	"sync"
	"sync/atomic"
	"time"
)

// iamState is the immutable in-memory snapshot of all IAM state. Replaced
// atomically on every mutation; readers only ever see fully-formed state.
type iamState struct {
	sas             map[string]*ServiceAccount // sa_id → SA
	keysByAK        map[string]*AccessKey      // access_key → AccessKey (plaintext SecretKey populated)
	grants          map[string]map[string]Role // sa_id → bucket → role
	wildcards       map[string]Role            // sa_id → role (default SA only)
	bucketUpstreams map[string]*BucketUpstream // bucket → upstream
}

func newEmptyState() *iamState {
	return &iamState{
		sas:             make(map[string]*ServiceAccount),
		keysByAK:        make(map[string]*AccessKey),
		grants:          make(map[string]map[string]Role),
		wildcards:       make(map[string]Role),
		bucketUpstreams: make(map[string]*BucketUpstream),
	}
}

// Store is the single owner of IAM state for one node. All mutations go
// through apply* methods (called only from the meta-FSM apply path);
// reads use atomic.Pointer.Load and are lock-free.
type Store struct {
	state atomic.Pointer[iamState]
	mu    sync.Mutex // serializes apply* (single-applier discipline; mu held only during COW build)
}

// NewStore returns an empty Store ready for FSM apply.
func NewStore() *Store {
	s := &Store{}
	s.state.Store(newEmptyState())
	return s
}

// snapshot returns the current state pointer. Callers must NOT mutate.
func (s *Store) snapshot() *iamState { return s.state.Load() }

// LookupKey returns the AccessKey for the given access_key public id, plus
// ok=false if the key is missing, revoked, or expired.
func (s *Store) LookupKey(ak string) (*AccessKey, bool) {
	st := s.snapshot()
	k, ok := st.keysByAK[ak]
	if !ok || k == nil {
		return nil, false
	}
	if k.Status != KeyStatusActive {
		return nil, false
	}
	if k.ExpiresAt != nil && time.Now().After(*k.ExpiresAt) {
		return nil, false
	}
	return k, true
}

// LookupGrant returns the Role for (sa_id, bucket), falling back to the
// wildcard grant if no explicit grant exists. RoleNone if neither.
func (s *Store) LookupGrant(saID, bucket string) Role {
	st := s.snapshot()
	if perBucket, ok := st.grants[saID]; ok {
		if r, ok := perBucket[bucket]; ok {
			return r
		}
	}
	if r, ok := st.wildcards[saID]; ok {
		return r
	}
	return RoleNone
}

// NumExplicitGrants returns the number of per-bucket (non-wildcard) grants
// currently held by saID. Used by admin guards that need to reason about
// access loss before mutating wildcard grants.
func (s *Store) NumExplicitGrants(saID string) int {
	st := s.snapshot()
	if per, ok := st.grants[saID]; ok {
		return len(per)
	}
	return 0
}

// LookupSA returns the ServiceAccount metadata or (nil, false).
func (s *Store) LookupSA(saID string) (*ServiceAccount, bool) {
	st := s.snapshot()
	sa, ok := st.sas[saID]
	return sa, ok
}

// IsEmpty returns true when no SAs are registered. Used by HandleSACreate
// to decide whether to dispatch IAMInitFirstSA (composite) or the regular
// SACreate+KeyCreate path.
func (s *Store) IsEmpty() bool { return len(s.snapshot().sas) == 0 }

// AuthEnabled is a compatibility shim: v0.0.110.0+ removed the sticky
// `auth_enabled` bit and made authz always-on, but the s3auth.IAMStore
// interface (PR #250) still declares this method. Returning true keeps
// PR #250's RequestAuthorizer.Decide layer evaluation enabled
// unconditionally, which matches the "always-on" semantics.
func (s *Store) AuthEnabled() bool { return true }

// Reset wipes all in-memory state to a fresh empty Store. Called by the
// MetaFSM raft Restore path to ensure snapshot install replaces (not
// merges with) any state accumulated during local apply replay.
func (s *Store) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Store(newEmptyState())
}

// --- apply* methods: called only from FSM apply path ---

func (s *Store) cow() *iamState {
	old := s.snapshot()
	ns := &iamState{
		sas:             copySAMap(old.sas),
		keysByAK:        copyKeyMap(old.keysByAK),
		grants:          copyGrantMap(old.grants),
		wildcards:       copyRoleMap(old.wildcards),
		bucketUpstreams: copyBucketUpstreamMap(old.bucketUpstreams),
	}
	return ns
}

func (s *Store) commit(ns *iamState) { s.state.Store(ns) }

func (s *Store) applySACreate(sa ServiceAccount) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns := s.cow()
	ns.sas[sa.ID] = &sa
	s.commit(ns)
}

func (s *Store) applySADelete(saID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns := s.cow()
	delete(ns.sas, saID)
	for ak, k := range ns.keysByAK {
		if k.SAID == saID {
			delete(ns.keysByAK, ak)
		}
	}
	delete(ns.grants, saID)
	delete(ns.wildcards, saID)
	s.commit(ns)
}

func (s *Store) applyKeyCreate(k AccessKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns := s.cow()
	ns.keysByAK[k.AccessKey] = &k
	s.commit(ns)
}

func (s *Store) applyKeyRevoke(ak string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns := s.cow()
	if k, ok := ns.keysByAK[ak]; ok {
		kc := *k
		kc.Status = KeyStatusRevoked
		ns.keysByAK[ak] = &kc
	}
	s.commit(ns)
}

func (s *Store) applyGrantPut(g Grant) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns := s.cow()
	per, ok := ns.grants[g.SAID]
	if !ok {
		per = make(map[string]Role)
	} else {
		per = copyBucketRoleMap(per)
	}
	per[g.Bucket] = g.Role
	ns.grants[g.SAID] = per
	s.commit(ns)
}

func (s *Store) applyGrantDelete(saID, bucket string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns := s.cow()
	if per, ok := ns.grants[saID]; ok {
		per2 := copyBucketRoleMap(per)
		delete(per2, bucket)
		if len(per2) == 0 {
			delete(ns.grants, saID)
		} else {
			ns.grants[saID] = per2
		}
	}
	s.commit(ns)
}

func (s *Store) applyGrantWildcardPut(g Grant) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns := s.cow()
	ns.wildcards[g.SAID] = g.Role
	s.commit(ns)
}

func (s *Store) applyGrantWildcardDelete(saID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns := s.cow()
	delete(ns.wildcards, saID)
	s.commit(ns)
}

func (s *Store) applyBucketUpstreamPut(u BucketUpstream) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns := s.cow()
	uc := u
	ns.bucketUpstreams[u.Bucket] = &uc
	s.commit(ns)
}

func (s *Store) applyBucketUpstreamDelete(bucket string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns := s.cow()
	delete(ns.bucketUpstreams, bucket)
	s.commit(ns)
}

// LookupBucketUpstream returns the BucketUpstream record for the given bucket,
// or (nil, false) if no upstream is configured. Lock-free read via atomic
// snapshot pointer.
func (s *Store) LookupBucketUpstream(bucket string) (*BucketUpstream, bool) {
	st := s.snapshot()
	u, ok := st.bucketUpstreams[bucket]
	if !ok || u == nil {
		return nil, false
	}
	return u, true
}

// --- map copy helpers (fresh maps so old state remains immutable) ---

func copySAMap(in map[string]*ServiceAccount) map[string]*ServiceAccount {
	out := make(map[string]*ServiceAccount, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyKeyMap(in map[string]*AccessKey) map[string]*AccessKey {
	out := make(map[string]*AccessKey, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyGrantMap(in map[string]map[string]Role) map[string]map[string]Role {
	out := make(map[string]map[string]Role, len(in))
	for k, v := range in {
		out[k] = copyBucketRoleMap(v)
	}
	return out
}

func copyBucketRoleMap(in map[string]Role) map[string]Role {
	out := make(map[string]Role, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyRoleMap(in map[string]Role) map[string]Role {
	out := make(map[string]Role, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyBucketUpstreamMap(in map[string]*BucketUpstream) map[string]*BucketUpstream {
	out := make(map[string]*BucketUpstream, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
