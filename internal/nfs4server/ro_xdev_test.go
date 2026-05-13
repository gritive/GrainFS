package nfs4server

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

func newDispatcherWithExports(t *testing.T, rows map[string]exportConfig) *Dispatcher {
	t.Helper()
	backend, err := storage.NewLocalBackend(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for bucket := range rows {
		if err := backend.CreateBucket(context.Background(), bucket); err != nil {
			t.Fatal(err)
		}
	}
	srv := NewServer(backend)
	srv.SetExportsForTest(buildSnap(rows))
	d := getDispatcher(srv.backend, srv.state, srv)
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
			if got := tc.invoke(d).Status; got != NFS4ERR_ROFS {
				t.Fatalf("status = %d, want NFS4ERR_ROFS", got)
			}
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
	if got := d.opRename(renameArgs.Bytes()).Status; got != NFS4ERR_XDEV {
		t.Fatalf("rename status = %d, want NFS4ERR_XDEV", got)
	}

	d.savedPath = "/a/src.bin"
	d.currentPath = "/b/dst.bin"
	if got := d.opCopy(buildCopyArgs42(0, 0, 1)).Status; got != NFS4ERR_XDEV {
		t.Fatalf("copy status = %d, want NFS4ERR_XDEV", got)
	}
}

func mustSizeAttr(size uint64) []byte {
	w := &XDRWriter{}
	w.WriteUint64(size)
	return w.Bytes()
}
