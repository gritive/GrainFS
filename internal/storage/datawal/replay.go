package datawal

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gritive/GrainFS/internal/encrypt"
)

const checkpointName = "checkpoint"

// Materializer applies WAL records during recovery.
//
// Recover saves its checkpoint only after a full successful replay. Until that
// checkpoint is saved, records that were already materialized can be replayed
// after a later failure or process crash. Replacement records can use
// HasReplacement to skip already-present targets; Materialize implementations
// for patch or logical mutation records must be idempotent.
type Materializer interface {
	Materialize(context.Context, Record) error
	HasReplacement(context.Context, Record) (bool, error)
}

// Recover replays records after max(fromSeq, LoadCheckpoint(dir)) through m and
// saves the checkpoint to the last replayed sequence after replay completes.
func Recover(ctx context.Context, dir string, fromSeq uint64, enc *encrypt.Encryptor, m Materializer) error {
	if m == nil {
		return fmt.Errorf("datawal: nil materializer")
	}
	checkpoint, err := LoadCheckpoint(dir)
	if err != nil {
		return err
	}
	startSeq := fromSeq
	if checkpoint > startSeq {
		startSeq = checkpoint
	}

	var lastSeq uint64
	err = Replay(ctx, dir, startSeq, enc, func(rec Record) error {
		lastSeq = rec.Seq
		if isReplacementOp(rec.Op) {
			ok, err := m.HasReplacement(ctx, rec)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		}
		return m.Materialize(ctx, rec)
	})
	if err != nil {
		return err
	}
	if lastSeq == 0 {
		return nil
	}
	return SaveCheckpoint(dir, lastSeq)
}

func LoadCheckpoint(dir string) (uint64, error) {
	data, err := os.ReadFile(checkpointPath(dir))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(data) != 8 {
		return 0, fmt.Errorf("datawal: invalid checkpoint length %d", len(data))
	}
	return binary.BigEndian.Uint64(data), nil
}

func SaveCheckpoint(dir string, seq uint64) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, checkpointName+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	var data [8]byte
	binary.BigEndian.PutUint64(data[:], seq)
	if err := writeAll(tmp, data[:]); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, checkpointPath(dir)); err != nil {
		return err
	}
	return syncDir(dir)
}

func checkpointPath(dir string) string {
	return filepath.Join(dir, checkpointName)
}

func isReplacementOp(op byte) bool {
	return op == OpSegmentPut || op == OpShardPut || op == OpSpoolPut
}
