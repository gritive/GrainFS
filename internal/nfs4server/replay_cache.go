package nfs4server

// SlotEntry holds the replay cache for one SEQUENCE slot.
type SlotEntry struct {
	SeqID    uint32
	Response []byte // XDR-encoded SEQUENCE result when HasCache=true
	HasCache bool
}
