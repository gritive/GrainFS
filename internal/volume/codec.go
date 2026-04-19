package volume

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/volume/volumepb"
)

func marshalVolume(vol *Volume) ([]byte, error) {
	b := flatbuffers.NewBuilder(64)
	nameOff := b.CreateString(vol.Name)
	volumepb.VolumeStart(b)
	volumepb.VolumeAddName(b, nameOff)
	volumepb.VolumeAddSize(b, vol.Size)
	volumepb.VolumeAddBlockSize(b, int32(vol.BlockSize))
	root := volumepb.VolumeEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
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
		Name:      string(t.Name()),
		Size:      t.Size(),
		BlockSize: int(t.BlockSize()),
	}, nil
}
