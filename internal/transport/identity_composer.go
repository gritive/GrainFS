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
	swap        func(*IdentitySnapshot)
}

func newIdentityComposer(base [32]byte, swap func(*IdentitySnapshot)) *identityComposer {
	return &identityComposer{base: base, presentSPKI: base, swap: swap}
}

func (c *identityComposer) setBase(s [32]byte) { c.mu.Lock(); c.base = s; c.recompute(); c.mu.Unlock() }
func (c *identityComposer) setRotationWindow(w [][32]byte) {
	c.mu.Lock()
	c.rotation = w
	c.recompute()
	c.mu.Unlock()
}
func (c *identityComposer) setRegistry(r [][32]byte) {
	c.mu.Lock()
	c.registry = r
	c.recompute()
	c.mu.Unlock()
}
func (c *identityComposer) setPresent(cert tls.Certificate, spki [32]byte) {
	c.mu.Lock()
	c.presentCert, c.presentSPKI = cert, spki
	c.recompute()
	c.mu.Unlock()
}

// recompute builds base ∪ rotation ∪ registry (dedup) and swaps. Caller holds mu.
func (c *identityComposer) recompute() {
	seen := map[[32]byte]struct{}{c.base: {}}
	accept := [][32]byte{c.base}
	for _, set := range [][][32]byte{c.rotation, c.registry} {
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
