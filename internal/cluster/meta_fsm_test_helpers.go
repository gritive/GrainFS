package cluster

// ApplyCmdForTest is a test-only re-export of the private applyCmd method so
// packages outside internal/cluster (e.g. internal/serveruntime) can drive
// FSM-level apply paths in their own tests without copying the apply switch.
//
// Production code must not call this. The "ForTest" suffix and this top-level
// comment exist to keep that contract visible at code-review time.
func (f *MetaFSM) ApplyCmdForTest(cmd []byte) error {
	return f.applyCmd(cmd)
}

// EncodeMetaCmdForTest wraps a typed payload in a MetaCmd FlatBuffers envelope
// from outside the cluster package. Mirrors the private encodeMetaCmd. Test-only.
func EncodeMetaCmdForTest(cmdType MetaCmdType, payload []byte) ([]byte, error) {
	return encodeMetaCmd(cmdType, payload)
}
