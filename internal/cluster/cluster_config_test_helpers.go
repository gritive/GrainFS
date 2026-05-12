package cluster

// ApplyClusterConfigPatchForTest is a test-only helper that encodes p with the
// canonical FlatBuffer encoder and feeds the result through the FSM's normal
// apply path. Production code MUST NOT call this — it bypasses Raft and is
// only useful for unit tests that need to mutate ClusterConfig without a
// running cluster.
func (f *MetaFSM) ApplyClusterConfigPatchForTest(p ClusterConfigPatch) error {
	data, err := EncodeClusterConfigPatchCmd(p)
	if err != nil {
		return err
	}
	return f.applyCmd(data)
}
