package serveruntime

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/config"
)

func TestBootSrvOptsAndReceipt_NilCfgStore(t *testing.T) {
	stores, err := WireIAMPolicyStores(context.Background(), nil, 0)
	require.NoError(t, err)

	state := &bootState{
		cfg:             Config{DataDir: t.TempDir()},
		iamPolicyStores: stores,
		cfgStore:        nil, // deliberately missing — must trigger boot-fail
	}
	err = bootSrvOptsAndReceipt(context.Background(), state)
	require.Error(t, err)
	require.ErrorContains(t, err, "cfgStore")
}

func TestBootSrvOptsAndReceipt_NilIAMPolicyStores(t *testing.T) {
	cfgStore := config.NewStore()
	state := &bootState{
		cfg:             Config{DataDir: t.TempDir()},
		iamPolicyStores: nil, // deliberately missing — must trigger boot-fail
		cfgStore:        cfgStore,
	}
	err := bootSrvOptsAndReceipt(context.Background(), state)
	require.Error(t, err)
	require.ErrorContains(t, err, "iamPolicyStores")
}
