package volume

import (
	"fmt"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/volume/volumepb"
)

var volumeBuilderPool = sync.Pool{
	New: func() any { return flatbuffers.NewBuilder(256) },
}

func marshalVolume(vol *Volume) ([]byte, error) {
	b := volumeBuilderPool.Get().(*flatbuffers.Builder)
	nameOff := b.CreateString(vol.Name)
	volumepb.VolumeStart(b)
	volumepb.VolumeAddName(b, nameOff)
	volumepb.VolumeAddSize(b, vol.Size)
	volumepb.VolumeAddBlockSize(b, int32(vol.BlockSize))
	volumepb.VolumeAddAllocatedBlocks(b, vol.AllocatedBlocks)
	volumepb.VolumeAddSnapshotCount(b, vol.SnapshotCount)
	root := volumepb.VolumeEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	b.Reset()
	volumeBuilderPool.Put(b)
	return out, nil
}

func unmarshalVolume(data []byte) (vol *Volume, err error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("unmarshal volume: empty data")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unmarshal volume: invalid flatbuffer: %v", r)
		}
	}()
	t := volumepb.GetRootAsVolume(data, 0)
	return &Volume{
		Name:            string(t.Name()),
		Size:            t.Size(),
		BlockSize:       int(t.BlockSize()),
		AllocatedBlocks: t.AllocatedBlocks(), // -1 if field absent (old volumes)
		SnapshotCount:   t.SnapshotCount(),   // 0 if field absent (old volumes)
	}, nil
}
