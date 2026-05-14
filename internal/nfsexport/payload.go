package nfsexport

import (
	"fmt"

	"github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

type Config struct {
	ReadOnly   bool
	FsidMajor  uint64
	FsidMinor  uint64
	Generation uint64
}

func EncodeUpsertPayload(bucket string, cfg Config) ([]byte, error) {
	if bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	b := flatbuffers.NewBuilder(64)
	bucketOff := b.CreateString(bucket)
	clusterpb.NfsExportConfigStart(b)
	clusterpb.NfsExportConfigAddReadOnly(b, cfg.ReadOnly)
	clusterpb.NfsExportConfigAddFsidMajor(b, cfg.FsidMajor)
	clusterpb.NfsExportConfigAddFsidMinor(b, cfg.FsidMinor)
	clusterpb.NfsExportConfigAddGeneration(b, cfg.Generation)
	cfgOff := clusterpb.NfsExportConfigEnd(b)
	clusterpb.NfsExportUpsertCmdStart(b)
	clusterpb.NfsExportUpsertCmdAddBucket(b, bucketOff)
	clusterpb.NfsExportUpsertCmdAddConfig(b, cfgOff)
	b.Finish(clusterpb.NfsExportUpsertCmdEnd(b))
	return append([]byte(nil), b.FinishedBytes()...), nil
}

func DecodeUpsertPayload(buf []byte) (bucket string, cfg Config, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("invalid flatbuffer: %v", r)
		}
	}()
	if len(buf) == 0 {
		return "", Config{}, fmt.Errorf("empty data")
	}
	cmd := clusterpb.GetRootAsNfsExportUpsertCmd(buf, 0)
	bucket = string(cmd.Bucket())
	if bucket == "" {
		return "", Config{}, fmt.Errorf("bucket is required")
	}
	fbCfg := cmd.Config(nil)
	if fbCfg == nil {
		return "", Config{}, fmt.Errorf("config is required")
	}
	return bucket, Config{
		ReadOnly:   fbCfg.ReadOnly(),
		FsidMajor:  fbCfg.FsidMajor(),
		FsidMinor:  fbCfg.FsidMinor(),
		Generation: fbCfg.Generation(),
	}, nil
}

func EncodeDeletePayload(bucket string) ([]byte, error) {
	if bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	b := flatbuffers.NewBuilder(64)
	bucketOff := b.CreateString(bucket)
	clusterpb.NfsExportDeleteCmdStart(b)
	clusterpb.NfsExportDeleteCmdAddBucket(b, bucketOff)
	b.Finish(clusterpb.NfsExportDeleteCmdEnd(b))
	return append([]byte(nil), b.FinishedBytes()...), nil
}

func DecodeDeletePayload(buf []byte) (bucket string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("invalid flatbuffer: %v", r)
		}
	}()
	if len(buf) == 0 {
		return "", fmt.Errorf("empty data")
	}
	cmd := clusterpb.GetRootAsNfsExportDeleteCmd(buf, 0)
	bucket = string(cmd.Bucket())
	if bucket == "" {
		return "", fmt.Errorf("bucket is required")
	}
	return bucket, nil
}

func EncodeBucketDeleteCascadePayload(bucket string, force bool) ([]byte, error) {
	if bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	b := flatbuffers.NewBuilder(64)
	bucketOff := b.CreateString(bucket)
	clusterpb.NfsExportBucketDeleteCascadeCmdStart(b)
	clusterpb.NfsExportBucketDeleteCascadeCmdAddBucket(b, bucketOff)
	clusterpb.NfsExportBucketDeleteCascadeCmdAddForce(b, force)
	b.Finish(clusterpb.NfsExportBucketDeleteCascadeCmdEnd(b))
	return append([]byte(nil), b.FinishedBytes()...), nil
}

func DecodeBucketDeleteCascadePayload(buf []byte) (bucket string, force bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("invalid flatbuffer: %v", r)
		}
	}()
	if len(buf) == 0 {
		return "", false, fmt.Errorf("empty data")
	}
	cmd := clusterpb.GetRootAsNfsExportBucketDeleteCascadeCmd(buf, 0)
	bucket = string(cmd.Bucket())
	if bucket == "" {
		return "", false, fmt.Errorf("bucket is required")
	}
	return bucket, cmd.Force(), nil
}
