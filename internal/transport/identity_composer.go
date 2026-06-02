package transport

import (
	"crypto/tls"
	"sync"
)

// identityComposer owns the three sources of the transport accept-set and the
// present cert/SPKI, recomputes their UNION, and issues exactly one
// SwapIdentity per change. No single source ever REPLACEs the whole set
// (spec §6 D-rev3 step 3): base PSK, KEK-rotation window, and the peer-registry
// per-node SPKIs compose, they do not clobber.
type identityComposer struct {
	mu          sync.Mutex
	base        [32]byte
	rotation    [][32]byte
	registry    [][32]byte
	presentCert tls.Certificate
	presentSPKI [32]byte
	pinPresent  bool
	dropped     bool
	swap        func(*IdentitySnapshot)
}

func newIdentityComposer(base [32]byte, swap func(*IdentitySnapshot)) *identityComposer {
	return &identityComposer{base: base, presentSPKI: base, swap: swap}
}

func (c *identityComposer) setRegistry(r [][32]byte) {
	c.mu.Lock()
	c.registry = r
	c.recompute()
	c.mu.Unlock()
}
func (c *identityComposer) setPresent(cert tls.Certificate, spki [32]byte) {
	c.mu.Lock()
	if !c.pinPresent {
		c.presentCert, c.presentSPKI = cert, spki
	}
	c.recompute()
	c.mu.Unlock()
}

// setPinPresent pins the presented cert/SPKI so that subsequent applyRotation
// and setPresent calls no longer change presentCert/presentSPKI (applyRotation
// still advances the rotation window + base). Live caller: the present-flip to
// per-node identity, via TCPTransport.FlipPresent — fired by the meta-FSM
// onPresentFlip callback and by post-drop invite-join boot (spec §8 H4').
func (c *identityComposer) setPinPresent(cert tls.Certificate, spki [32]byte) {
	c.mu.Lock()
	c.presentCert, c.presentSPKI = cert, spki
	c.pinPresent = true
	c.recompute()
	c.mu.Unlock()
}

// setDropped marks the cluster key as dropped so recompute excludes ALL
// cluster-key-derived SPKIs (base PSK + rotation window) from the accept-set
// (accept = registry only). Live caller: TCPTransport.SetDropped — fired by
// the meta-FSM onClusterKeyDropped callback (apply + Restore of a dropped
// snapshot) and by post-drop invite-join boot (spec §8 H3/H4').
func (c *identityComposer) setDropped() {
	c.mu.Lock()
	c.dropped = true
	c.recompute()
	c.mu.Unlock()
}

// applyRotation sets the rotation window, present cert/SPKI, and (when newBase
// is non-nil) the base in ONE locked section followed by a SINGLE recompute.
// Combining the mutations under one lock guarantees no intermediate recompute
// drops acceptance of the just-presented cert (spec §6 D-rev3 step 3).
func (c *identityComposer) applyRotation(window [][32]byte, cert tls.Certificate, spki [32]byte, newBase *[32]byte) {
	c.mu.Lock()
	c.rotation = window
	if !c.pinPresent {
		c.presentCert, c.presentSPKI = cert, spki
	}
	if newBase != nil {
		c.base = *newBase
	}
	c.recompute()
	c.mu.Unlock()
}

// recompute builds the accept-set and swaps. Caller holds mu.
//
// Not dropped: base ∪ rotation ∪ registry (dedup).
// Dropped: registry only. Both the base PSK AND the rotation window are
// cluster-key-derived — rotation_worker installs the cluster rotation FSM's
// {OldSPKI, NewSPKI} (from DeriveClusterIdentity(clusterKey)) into c.rotation,
// never per-node SPKIs — so a drop that lands mid-rotation must exclude the
// rotation window too, or it would keep accepting cluster-key certs until the
// next steady recompute clears the window (spec §8 H4'). Excluding the whole
// rotation set when dropped is timing-independent and cannot strand a peer
// (per-node SPKIs live only in c.registry).
func (c *identityComposer) recompute() {
	var seen map[[32]byte]struct{}
	var accept [][32]byte
	var sources [][][32]byte
	if c.dropped {
		seen = map[[32]byte]struct{}{}
		accept = [][32]byte{}
		sources = [][][32]byte{c.registry}
	} else {
		seen = map[[32]byte]struct{}{c.base: {}}
		accept = [][32]byte{c.base}
		sources = [][][32]byte{c.rotation, c.registry}
	}
	for _, set := range sources {
		for _, s := range set {
			if _, ok := seen[s]; ok {
				continue
			}
			seen[s] = struct{}{}
			accept = append(accept, s)
		}
	}
	c.swap(NewIdentitySnapshot(accept, c.presentCert, c.presentSPKI))
}
