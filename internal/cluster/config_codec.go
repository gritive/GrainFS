package cluster

import (
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// encodeMetaConfigPutCmd serializes a ConfigPut payload (inner data bytes of
// a MetaCmd envelope — wrap with encodeMetaCmd to get the full envelope).
func encodeMetaConfigPutCmd(key, value string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	keyOff := b.CreateString(key)
	valOff := b.CreateString(value)
	clusterpb.MetaConfigPutCmdStart(b)
	clusterpb.MetaConfigPutCmdAddKey(b, keyOff)
	clusterpb.MetaConfigPutCmdAddValue(b, valOff)
	return fbFinish(b, clusterpb.MetaConfigPutCmdEnd(b)), nil
}

// decodeMetaConfigPutCmd parses the inner ConfigPut payload bytes.
func decodeMetaConfigPutCmd(data []byte) (key, value string, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaConfigPutCmd {
		return clusterpb.GetRootAsMetaConfigPutCmd(d, 0)
	})
	if err != nil {
		return "", "", fmt.Errorf("config_codec: ConfigPut: %w", err)
	}
	return string(t.Key()), string(t.Value()), nil
}

// encodeMetaConfigDeleteCmd serializes a ConfigDelete payload.
func encodeMetaConfigDeleteCmd(key string) ([]byte, error) {
	b := clusterBuilderPool.Get()
	keyOff := b.CreateString(key)
	clusterpb.MetaConfigDeleteCmdStart(b)
	clusterpb.MetaConfigDeleteCmdAddKey(b, keyOff)
	return fbFinish(b, clusterpb.MetaConfigDeleteCmdEnd(b)), nil
}

// decodeMetaConfigDeleteCmd parses the inner ConfigDelete payload bytes.
func decodeMetaConfigDeleteCmd(data []byte) (key string, err error) {
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaConfigDeleteCmd {
		return clusterpb.GetRootAsMetaConfigDeleteCmd(d, 0)
	})
	if err != nil {
		return "", fmt.Errorf("config_codec: ConfigDelete: %w", err)
	}
	return string(t.Key()), nil
}
