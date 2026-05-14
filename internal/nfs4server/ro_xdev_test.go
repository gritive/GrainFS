package nfs4server

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

func newDispatcherWithExports(t *testing.T, rows map[string]exportConfig) *Dispatcher {
	t.Helper()
	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	for bucket := range rows {
		require.NoError(t, backend.CreateBucket(context.Background(), bucket))
	}
	srv := NewServer(backend)
	srv.SetExportsForTest(buildSnap(rows))
	d := getDispatcherWithClient(srv.backend, srv.state, srv, "", nil)
	t.Cleanup(func() {
		putDispatcher(d)
		backend.Close()
	})
	return d
}

func TestReadOnlyExportMutationGuards(t *testing.T) {
	cases := []struct {
		name   string
		setup  func(*Dispatcher)
		invoke func(*Dispatcher) OpResult
	}{
		{
			name: "write",
			setup: func(d *Dispatcher) {
				d.currentPath = "/ro/file.bin"
			},
			invoke: func(d *Dispatcher) OpResult {
				return d.opWrite(buildWriteOp(0, []byte("x")))
			},
		},
		{
			name: "open_create",
			setup: func(d *Dispatcher) {
				d.currentPath = "/ro"
			},
			invoke: func(d *Dispatcher) OpResult {
				return d.opOpen(buildOpenCreateArgs("file.bin"))
			},
		},
		{
			name: "setattr",
			setup: func(d *Dispatcher) {
				d.currentPath = "/ro/file.bin"
			},
			invoke: func(d *Dispatcher) OpResult {
				return d.opSetAttr(buildSetAttrOp(1<<fattr4Size, 0, mustSizeAttr(0)))
			},
		},
		{
			name: "create",
			setup: func(d *Dispatcher) {
				d.currentPath = "/ro"
			},
			invoke: func(d *Dispatcher) OpResult {
				w := &XDRWriter{}
				w.WriteUint32(NF4DIR)
				w.WriteString("dir")
				return d.opCreate(w.Bytes())
			},
		},
		{
			name: "remove",
			setup: func(d *Dispatcher) {
				d.currentPath = "/ro"
			},
			invoke: func(d *Dispatcher) OpResult {
				return d.opRemove([]byte("file.bin"))
			},
		},
		{
			name: "rename",
			setup: func(d *Dispatcher) {
				d.savedPath = "/ro"
				d.currentPath = "/ro"
			},
			invoke: func(d *Dispatcher) OpResult {
				w := &XDRWriter{}
				w.WriteString("old.bin")
				w.WriteString("new.bin")
				return d.opRename(w.Bytes())
			},
		},
		{
			name: "allocate",
			setup: func(d *Dispatcher) {
				d.currentPath = "/ro/file.bin"
			},
			invoke: func(d *Dispatcher) OpResult {
				return d.opAllocate(buildAllocateArgs42(0, 1))
			},
		},
		{
			name: "deallocate",
			setup: func(d *Dispatcher) {
				d.currentPath = "/ro/file.bin"
			},
			invoke: func(d *Dispatcher) OpResult {
				return d.opDeallocate(buildAllocateArgs42(0, 1))
			},
		},
		{
			name: "copy",
			setup: func(d *Dispatcher) {
				d.savedPath = "/ro/src.bin"
				d.currentPath = "/ro/dst.bin"
			},
			invoke: func(d *Dispatcher) OpResult {
				return d.opCopy(buildCopyArgs42(0, 0, 1))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newDispatcherWithExports(t, map[string]exportConfig{
				"ro": {readOnly: true, generation: 1},
			})
			tc.setup(d)
			require.Equal(t, NFS4ERR_ROFS, tc.invoke(d).Status)
		})
	}
}

func TestCrossExportRenameAndCopyReturnXDEV(t *testing.T) {
	d := newDispatcherWithExports(t, map[string]exportConfig{
		"a": {generation: 1},
		"b": {generation: 1},
	})

	d.savedPath = "/a"
	d.currentPath = "/b"
	renameArgs := &XDRWriter{}
	renameArgs.WriteString("old.bin")
	renameArgs.WriteString("new.bin")
	require.Equal(t, NFS4ERR_XDEV, d.opRename(renameArgs.Bytes()).Status)

	d.savedPath = "/a/src.bin"
	d.currentPath = "/b/dst.bin"
	require.Equal(t, NFS4ERR_XDEV, d.opCopy(buildCopyArgs42(0, 0, 1)).Status)
}

func mustSizeAttr(size uint64) []byte {
	w := &XDRWriter{}
	w.WriteUint64(size)
	return w.Bytes()
}
