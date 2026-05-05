package resourcewatch

// FD-specific Category constants. Used by the FD provider implementation
// (fd_provider_unix.go) and FD wiring helpers in cmd/grainfs.
const (
	FDCategorySocket              Category = "socket"
	FDCategoryBadger              Category = "badger"
	FDCategoryReceiptOrEventStore Category = "receipt_or_event_store"
	FDCategoryNFSSession          Category = "nfs_session"
	FDCategoryRegularFile         Category = "regular_file"
	FDCategoryUnknown             Category = "unknown"
)

// DBCategory* — BadgerDB vlog watcher (PR2) categories. Used by Registry,
// VlogProvider, and GC ticker to label per-DB vlog metrics and incidents.
const (
	DBCategoryMeta          Category = "meta"
	DBCategorySharedRaftLog Category = "shared-raft-log"
	DBCategoryGroupRaft     Category = "group-raft"
	DBCategoryIncident      Category = "incident"
	DBCategoryReceipts      Category = "receipts"
	DBCategoryDedup         Category = "dedup"
	DBCategoryStorage       Category = "storage"
)
