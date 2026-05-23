package cluster

// NewTestDistributedBackend exposes newTestDistributedBackend to external
// test packages (package cluster_test) without making it part of the
// production API. Only compiled when running tests.
func NewTestDistributedBackend(t clusterTestTB) *DistributedBackend {
	return newTestDistributedBackend(t)
}

// Root returns the backend's root directory (test helper).
func (b *DistributedBackend) Root() string { return b.root }

// SelfAddr returns the backend's self address (test helper).
func (b *DistributedBackend) SelfAddr() string { return b.selfAddr }
