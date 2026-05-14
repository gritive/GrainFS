package nfs4server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPseudoRootGetAttrUsesReservedFSIDAndVolatileFH(t *testing.T) {
	d := newDispatcherWithExports(t, map[string]exportConfig{
		"bucket": {fsidMajor: 1, fsidMinor: 2, generation: 1},
	})
	d.currentPath = "/"

	got := decodeAttrsForPseudoRootTest(t, d.encodeAttrs("/", attrBitmap{
		1<<1 | 1<<2 | 1<<8,
		1<<(33-32) | 1<<(55-32),
	}))
	require.Equal(t, uint32(NF4DIR), got.typ)
	require.Equal(t, uint32(1), got.fhExpire)
	require.Equal(t, uint64(1), got.fsidMajor)
	require.Equal(t, uint64(0), got.fsidMinor)
	require.Equal(t, uint32(0755), got.mode)
	require.Equal(t, pathToFileID("/"), got.mountedOnFileID)
}

func TestExportRootGetAttrUsesExportFSIDAndRootMountedOnFileID(t *testing.T) {
	d := newDispatcherWithExports(t, map[string]exportConfig{
		"bucket": {fsidMajor: 1, fsidMinor: 9, generation: 1},
	})
	d.currentPath = "/bucket"
	d.state.MarkDir("/bucket")

	got := decodeAttrsForPseudoRootTest(t, d.encodeAttrs("/bucket", attrBitmap{
		1<<1 | 1<<2 | 1<<8,
		1<<(33-32) | 1<<(55-32),
	}))
	require.Equal(t, uint64(1), got.fsidMajor)
	require.Equal(t, uint64(9), got.fsidMinor)
	require.Equal(t, pathToFileID("/"), got.mountedOnFileID)
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
	require.NoError(t, err)
	for i := uint32(0); i < bitmapLen; i++ {
		_, err := r.ReadUint32()
		require.NoError(t, err)
	}
	attrBytes, err := r.ReadOpaque()
	require.NoError(t, err)
	ar := NewXDRReader(attrBytes)
	var got pseudoRootAttr
	got.typ, err = ar.ReadUint32()
	require.NoError(t, err)
	got.fhExpire, err = ar.ReadUint32()
	require.NoError(t, err)
	got.fsidMajor, err = ar.ReadUint64()
	require.NoError(t, err)
	got.fsidMinor, err = ar.ReadUint64()
	require.NoError(t, err)
	got.mode, err = ar.ReadUint32()
	require.NoError(t, err)
	got.mountedOnFileID, err = ar.ReadUint64()
	require.NoError(t, err)
	return got
}

func TestSupportedAttrsAdvertisesCansettimeAndSuppattrExclcreat(t *testing.T) {
	d := newDispatcherWithExports(t, map[string]exportConfig{
		"bucket": {fsidMajor: 1, fsidMinor: 2, generation: 1},
	})

	attrBytes := decodeAttrListForTest(t, d.encodeAttrs("/", attrBitmap{1 << 0}))
	ar := NewXDRReader(attrBytes)
	bitmapLen, err := ar.ReadUint32()
	require.NoError(t, err)
	require.Equal(t, uint32(3), bitmapLen)
	word0, err := ar.ReadUint32()
	require.NoError(t, err)
	_, err = ar.ReadUint32()
	require.NoError(t, err)
	word2, err := ar.ReadUint32()
	require.NoError(t, err)

	require.Zero(t, word0&(1<<14), "bit 14 is archive, not cansettime")
	require.NotZero(t, word0&(1<<15), "bit 15 cansettime should be advertised")
	require.NotZero(t, word2&(1<<(75-64)), "bit 75 suppattr_exclcreat should be advertised")
}

func TestLinkAndSymlinkSupportReflectUnsupportedOps(t *testing.T) {
	d := newDispatcherWithExports(t, map[string]exportConfig{
		"bucket": {fsidMajor: 1, fsidMinor: 2, generation: 1},
	})

	attrBytes := decodeAttrListForTest(t, d.encodeAttrs("/", attrBitmap{1<<5 | 1<<6}))
	ar := NewXDRReader(attrBytes)
	linkSupport, err := ar.ReadUint32()
	require.NoError(t, err)
	symlinkSupport, err := ar.ReadUint32()
	require.NoError(t, err)
	require.Equal(t, uint32(0), linkSupport)
	require.Equal(t, uint32(0), symlinkSupport)
}

func TestGetAttrCanReturnThirdBitmapWord(t *testing.T) {
	d := newDispatcherWithExports(t, map[string]exportConfig{
		"bucket": {fsidMajor: 1, fsidMinor: 2, generation: 1},
	})

	data := d.encodeAttrs("/", attrBitmap{0, 0, 1 << (75 - 64)})
	r := NewXDRReader(data)
	bitmapLen, err := r.ReadUint32()
	require.NoError(t, err)
	require.Equal(t, uint32(3), bitmapLen)
	_, err = r.ReadUint32()
	require.NoError(t, err)
	_, err = r.ReadUint32()
	require.NoError(t, err)
	word2, err := r.ReadUint32()
	require.NoError(t, err)
	require.NotZero(t, word2&(1<<(75-64)))

	attrBytes, err := r.ReadOpaque()
	require.NoError(t, err)
	ar := NewXDRReader(attrBytes)
	suppAttrLen, err := ar.ReadUint32()
	require.NoError(t, err)
	require.Equal(t, uint32(0), suppAttrLen)
}

func decodeAttrListForTest(t *testing.T, data []byte) []byte {
	t.Helper()
	r := NewXDRReader(data)
	bitmapLen, err := r.ReadUint32()
	require.NoError(t, err)
	for i := uint32(0); i < bitmapLen; i++ {
		_, err := r.ReadUint32()
		require.NoError(t, err)
	}
	attrBytes, err := r.ReadOpaque()
	require.NoError(t, err)
	return attrBytes
}
