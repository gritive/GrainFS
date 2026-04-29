package nfs4server

// SlotEntry holds the replay cache for one SEQUENCE slot.
type SlotEntry struct {
	SeqID        uint32
	Response     []byte // XDR-encoded full COMPOUND response when HasCache=true
	HasCache     bool
	WasCacheThis bool // true if the last new request had cacheThis=1
}
