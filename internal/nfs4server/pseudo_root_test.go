package nfs4server

import "testing"

func TestPseudoRootGetAttrUsesReservedFSIDAndVolatileFH(t *testing.T) {
	d := newDispatcherWithExports(t, map[string]exportConfig{
		"bucket": {fsidMajor: 1, fsidMinor: 2, generation: 1},
	})
	d.currentPath = "/"

	got := decodeAttrsForPseudoRootTest(t, d.encodeAttrs("/", [2]uint32{
		1<<1 | 1<<2 | 1<<8,
		1<<(33-32) | 1<<(55-32),
	}))
	if got.typ != NF4DIR {
		t.Fatalf("type = %d, want NF4DIR", got.typ)
	}
	if got.fhExpire != 1 {
		t.Fatalf("fh_expire_type = %d, want FH4_VOLATILE_ANY", got.fhExpire)
	}
	if got.fsidMajor != 1 || got.fsidMinor != 0 {
		t.Fatalf("fsid = (%d,%d), want (1,0)", got.fsidMajor, got.fsidMinor)
	}
	if got.mode != 0755 {
		t.Fatalf("mode = %#o, want 0755", got.mode)
	}
	if got.mountedOnFileID != pathToFileID("/") {
		t.Fatalf("mounted_on_fileid = %d, want root fileid", got.mountedOnFileID)
	}
}

func TestExportRootGetAttrUsesExportFSIDAndRootMountedOnFileID(t *testing.T) {
	d := newDispatcherWithExports(t, map[string]exportConfig{
		"bucket": {fsidMajor: 1, fsidMinor: 9, generation: 1},
	})
	d.currentPath = "/bucket"
	d.state.MarkDir("/bucket")

	got := decodeAttrsForPseudoRootTest(t, d.encodeAttrs("/bucket", [2]uint32{
		1<<1 | 1<<2 | 1<<8,
		1<<(33-32) | 1<<(55-32),
	}))
	if got.fsidMajor != 1 || got.fsidMinor != 9 {
		t.Fatalf("fsid = (%d,%d), want (1,9)", got.fsidMajor, got.fsidMinor)
	}
	if got.mountedOnFileID != pathToFileID("/") {
		t.Fatalf("mounted_on_fileid = %d, want root fileid", got.mountedOnFileID)
	}
}

type pseudoRootAttr struct {
	typ             uint32
	fhExpire        uint32
	fsidMajor       uint64
	fsidMinor       uint64
	mode            uint32
	mountedOnFileID uint64
}

func decodeAttrsForPseudoRootTest(t *testing.T, data []byte) pseudoRootAttr {
	t.Helper()
	r := NewXDRReader(data)
	bitmapLen, err := r.ReadUint32()
	if err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < bitmapLen; i++ {
		if _, err := r.ReadUint32(); err != nil {
			t.Fatal(err)
		}
	}
	attrBytes, err := r.ReadOpaque()
	if err != nil {
		t.Fatal(err)
	}
	ar := NewXDRReader(attrBytes)
	var got pseudoRootAttr
	if got.typ, err = ar.ReadUint32(); err != nil {
		t.Fatal(err)
	}
	if got.fhExpire, err = ar.ReadUint32(); err != nil {
		t.Fatal(err)
	}
	if got.fsidMajor, err = ar.ReadUint64(); err != nil {
		t.Fatal(err)
	}
	if got.fsidMinor, err = ar.ReadUint64(); err != nil {
		t.Fatal(err)
	}
	if got.mode, err = ar.ReadUint32(); err != nil {
		t.Fatal(err)
	}
	if got.mountedOnFileID, err = ar.ReadUint64(); err != nil {
		t.Fatal(err)
	}
	return got
}
