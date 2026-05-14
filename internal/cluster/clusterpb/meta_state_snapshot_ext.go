package clusterpb

import flatbuffers "github.com/google/flatbuffers/go"

func (rcv *MetaStateSnapshot) NfsExportsPresent() bool {
	if rcv == nil {
		return false
	}
	return flatbuffers.UOffsetT(rcv._tab.Offset(22)) != 0
}
