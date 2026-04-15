package volume

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/volume/volumepb"
	"google.golang.org/protobuf/proto"
)

// marshalVolume serializes a Volume to protobuf binary format.
func marshalVolume(vol *Volume) ([]byte, error) {
	pb := &volumepb.Volume{
		Name:      vol.Name,
		Size:      vol.Size,
		BlockSize: int32(vol.BlockSize),
	}
	data, err := proto.Marshal(pb)
	if err != nil {
		return nil, fmt.Errorf("marshal volume proto: %w", err)
	}
	return data, nil
}

// unmarshalVolume deserializes protobuf binary data into a Volume.
func unmarshalVolume(data []byte) (*Volume, error) {
	pb := &volumepb.Volume{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return nil, fmt.Errorf("unmarshal volume proto: %w", err)
	}
	return &Volume{
		Name:      pb.Name,
		Size:      pb.Size,
		BlockSize: int(pb.BlockSize),
	}, nil
}
