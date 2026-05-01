package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/raft"
)

const RecoverClusterMarkerPath = "recovery/recovercluster.json"

var (
	ErrRecoverClusterNoSnapshot = errors.New("recovercluster: no source snapshot found")
	ErrRecoverClusterReadOnly   = errors.New("recovercluster: recovered cluster is write-disabled until verify --mark-writable")
)

type RecoverClusterOptions struct {
	SourceData        string
	TargetData        string
	NewNodeID         string
	NewRaftAddr       string
	BadgerManagedMode bool
	StripJointState   bool
}

type RecoverClusterPlan struct {
	Options              RecoverClusterOptions
	SourceRaftDir        string
	TargetRaftDir        string
	TargetMetaDir        string
	SnapshotIndex        uint64
	SnapshotTerm         uint64
	SnapshotSize         int64
	OriginalServers      []raft.Server
	JointPhase           raft.JointPhase
	JointOldVoters       []string
	JointNewVoters       []string
	JointEnterIndex      uint64
	JointManagedLearners []string
	SourceManagedMode    bool
	ManagedModePresent   bool
	TargetFresh          bool
	BlockedReason        string
}

type RecoverClusterMarker struct {
	Writable          bool          `json:"writable"`
	VerifiedAt        time.Time     `json:"verified_at,omitempty"`
	SourceSnapshot    SnapshotID    `json:"source_snapshot"`
	OriginalServers   []raft.Server `json:"original_servers"`
	RecoveredNodeID   string        `json:"recovered_node_id"`
	RecoveredRaftAddr string        `json:"recovered_raft_addr"`
	CreatedAt         time.Time     `json:"created_at"`
}

type SnapshotID struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Size  int64  `json:"size"`
}

// Recovery flow:
// source raft -> read-only plan -> execute full snapshot restore -> rewrite
// active membership to {new-node-id voter} -> marker writable=false -> serve
// wraps storage.Backend until verify flips the marker.
func BuildRecoverClusterPlan(opts RecoverClusterOptions) (*RecoverClusterPlan, error) {
	if opts.SourceData == "" || opts.TargetData == "" || opts.NewNodeID == "" || opts.NewRaftAddr == "" {
		return nil, fmt.Errorf("source-data, target-data, new-node-id, and new-raft-addr are required")
	}
	sourceRaftDir := filepath.Join(opts.SourceData, "raft")
	targetRaftDir := filepath.Join(opts.TargetData, "raft")
	targetMetaDir := filepath.Join(opts.TargetData, "meta")

	if hasGroups(opts.SourceData) {
		return nil, fmt.Errorf("multi-Raft group recovery is not supported in v1: %s exists", filepath.Join(opts.SourceData, "groups"))
	}

	managed, present, err := raft.InspectManagedModeReadOnly(sourceRaftDir)
	if err != nil {
		return nil, err
	}
	if present && managed != opts.BadgerManagedMode {
		return nil, fmt.Errorf("managed-mode mismatch: source managed=%v, requested target managed=%v", managed, opts.BadgerManagedMode)
	}
	meta, err := raft.InspectSnapshotMetaReadOnly(sourceRaftDir)
	if err != nil {
		return nil, err
	}
	if meta.Index == 0 {
		return nil, ErrRecoverClusterNoSnapshot
	}
	if meta.JointPhase != raft.JointNone && !opts.StripJointState {
		return nil, fmt.Errorf("source snapshot is in joint consensus phase %d; rerun with --strip-joint-state to recover as a clean single-node cluster", meta.JointPhase)
	}
	if err := checkRecoverTargetFresh(opts.TargetData); err != nil {
		return nil, err
	}

	return &RecoverClusterPlan{
		Options:              opts,
		SourceRaftDir:        sourceRaftDir,
		TargetRaftDir:        targetRaftDir,
		TargetMetaDir:        targetMetaDir,
		SnapshotIndex:        meta.Index,
		SnapshotTerm:         meta.Term,
		SnapshotSize:         meta.DataSize,
		OriginalServers:      meta.Servers,
		JointPhase:           meta.JointPhase,
		JointOldVoters:       meta.JointOldVoters,
		JointNewVoters:       meta.JointNewVoters,
		JointEnterIndex:      meta.JointEnterIndex,
		JointManagedLearners: meta.JointManagedLearners,
		SourceManagedMode:    managed,
		ManagedModePresent:   present,
		TargetFresh:          true,
	}, nil
}

func ExecuteRecoverClusterPlan(plan *RecoverClusterPlan) error {
	if plan == nil {
		return fmt.Errorf("recovercluster: nil plan")
	}
	if err := checkRecoverTargetFresh(plan.Options.TargetData); err != nil {
		return err
	}
	snap, err := raft.LoadSnapshotReadOnly(plan.SourceRaftDir)
	if err != nil {
		return err
	}
	if snap.Index == 0 || len(snap.Data) == 0 {
		return ErrRecoverClusterNoSnapshot
	}
	marker := RecoverClusterMarker{
		Writable:          false,
		SourceSnapshot:    SnapshotID{Index: snap.Index, Term: snap.Term, Size: int64(len(snap.Data))},
		OriginalServers:   plan.OriginalServers,
		RecoveredNodeID:   plan.Options.NewNodeID,
		RecoveredRaftAddr: plan.Options.NewRaftAddr,
		CreatedAt:         time.Now().UTC(),
	}
	if err := WriteRecoverClusterMarker(plan.Options.TargetData, marker); err != nil {
		return fmt.Errorf("write recovery marker: %w", err)
	}

	if err := os.MkdirAll(plan.TargetMetaDir, 0o755); err != nil {
		return fmt.Errorf("create target meta dir: %w", err)
	}
	db, err := badger.Open(badger.DefaultOptions(plan.TargetMetaDir).WithLogger(nil))
	if err != nil {
		return fmt.Errorf("open target meta db: %w", err)
	}
	fsm := NewFSM(db)
	if err := fsm.Restore(snap.Data); err != nil {
		_ = db.Close()
		return fmt.Errorf("restore FSM snapshot: %w", err)
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("close target meta db: %w", err)
	}

	var storeOpts []raft.BadgerLogStoreOption
	if plan.Options.BadgerManagedMode {
		storeOpts = append(storeOpts, raft.WithManagedMode())
	}
	store, err := raft.NewBadgerLogStore(plan.TargetRaftDir, storeOpts...)
	if err != nil {
		return fmt.Errorf("open target raft store: %w", err)
	}
	defer store.Close()

	recovered := raft.Snapshot{
		Index:   snap.Index,
		Term:    snap.Term,
		Data:    snap.Data,
		Servers: []raft.Server{{ID: plan.Options.NewNodeID, Suffrage: raft.Voter}},
	}
	if err := store.SaveSnapshot(recovered); err != nil {
		return fmt.Errorf("write recovered snapshot: %w", err)
	}
	if err := store.SaveBootstrapMarker(); err != nil {
		return fmt.Errorf("write bootstrap marker: %w", err)
	}
	return nil
}

func MarkRecoverClusterWritable(dataDir string) error {
	marker, err := LoadRecoverClusterMarker(dataDir)
	if err != nil {
		return err
	}
	if marker == nil {
		return fmt.Errorf("recovery marker not found at %s", dataDir)
	}
	if err := VerifyRecoverClusterTarget(dataDir, *marker); err != nil {
		return err
	}
	marker.Writable = true
	marker.VerifiedAt = time.Now().UTC()
	return WriteRecoverClusterMarker(dataDir, *marker)
}

func VerifyRecoverClusterTarget(dataDir string, marker RecoverClusterMarker) error {
	metaDir := filepath.Join(dataDir, "meta")
	db, err := badger.Open(badger.DefaultOptions(metaDir).WithLogger(nil).WithReadOnly(true))
	if err != nil {
		return fmt.Errorf("verify target meta db: %w", err)
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("verify target meta db close: %w", err)
	}

	snap, err := raft.InspectSnapshotMetaReadOnly(filepath.Join(dataDir, "raft"))
	if err != nil {
		return fmt.Errorf("verify target raft snapshot: %w", err)
	}
	if snap.Index == 0 {
		return ErrRecoverClusterNoSnapshot
	}
	if snap.Index != marker.SourceSnapshot.Index || snap.Term != marker.SourceSnapshot.Term {
		return fmt.Errorf("verify target raft snapshot: marker points to %d/%d, raft has %d/%d",
			marker.SourceSnapshot.Index, marker.SourceSnapshot.Term, snap.Index, snap.Term)
	}
	if len(snap.Servers) != 1 || snap.Servers[0].ID != marker.RecoveredNodeID || snap.Servers[0].Suffrage != raft.Voter {
		return fmt.Errorf("verify target raft snapshot: active membership is not recovered single voter %q", marker.RecoveredNodeID)
	}
	return nil
}

func LoadRecoverClusterMarker(dataDir string) (*RecoverClusterMarker, error) {
	path := filepath.Join(dataDir, RecoverClusterMarkerPath)
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var marker RecoverClusterMarker
	if err := json.Unmarshal(raw, &marker); err != nil {
		return nil, fmt.Errorf("read recovery marker: %w", err)
	}
	return &marker, nil
}

func WriteRecoverClusterMarker(dataDir string, marker RecoverClusterMarker) error {
	path := filepath.Join(dataDir, RecoverClusterMarkerPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(marker, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func checkRecoverTargetFresh(targetData string) error {
	dirty := []string{"meta", "raft", "groups", "data", "blobs", "wal", "snapshots", "recovery"}
	for _, name := range dirty {
		if _, err := os.Stat(filepath.Join(targetData, name)); err == nil {
			return fmt.Errorf("target is not fresh: %s already exists", filepath.Join(targetData, name))
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func hasGroups(dataDir string) bool {
	entries, err := os.ReadDir(filepath.Join(dataDir, "groups"))
	return err == nil && len(entries) > 0
}
