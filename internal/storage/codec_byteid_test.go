package storage

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// byteIdentityObjectFixtures returns Object fixtures whose marshaled bytes are
// frozen as golden constants captured from clean HEAD (before the codec
// vector-build dedup). Each helper extraction is pure loop-extraction, so the
// wire output must stay byte-identical. The fixtures intentionally cover the
// edges that would surface a vector-build drift: nil vs empty tag slices, an
// object with no optional fields, and multi-tag / multi-part / multi-segment
// shapes.
func byteIdentityObjectFixtures() map[string]Object {
	return map[string]Object{
		"nil_tags": {
			Key: "k", ETag: "e", Size: 5, LastModified: 1700000000,
		},
		"empty_tags": {
			Key: "k", ETag: "e", Size: 5, LastModified: 1700000000,
			Tags: []Tag{},
		},
		"multi_tags": {
			Key: "obj", ETag: "etag", ContentType: "text/plain",
			Size: 42, LastModified: 1700000001, ACL: 3,
			Tags: []Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "alice"}, {Key: "z", Value: ""}},
		},
		"tags_parts_segments": {
			Key: "obj2", ETag: "etag2", Size: 99, LastModified: 1700000002,
			IsAppendable: true,
			Segments: []SegmentRef{
				{BlobID: "b0", Size: 10, Checksum: []byte{1, 2, 3}, PlacementGroupID: "g0", ShardSize: 4},
				{BlobID: "b1", Size: 20},
			},
			Parts: []MultipartPartEntry{
				{PartNumber: 1, Size: 100, ETag: "pe1"},
				{PartNumber: 2, Size: 0, ETag: "pe2"},
			},
			Tags: []Tag{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}},
		},
	}
}

func byteIdentityMultipartFixtures() map[string]*multipartMeta {
	return map[string]*multipartMeta{
		"nil_tags": {
			UploadID: "u", Bucket: "bk", Key: "k", ContentType: "ct", CreatedAt: 1700000000,
		},
		"empty_tags": {
			UploadID: "u", Bucket: "bk", Key: "k", ContentType: "ct", CreatedAt: 1700000000,
			Tags: []Tag{},
		},
		"multi_tags": {
			UploadID: "u2", Bucket: "bk2", Key: "k2", ContentType: "application/octet-stream",
			CreatedAt: 1700000003,
			Tags:      []Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "bob"}},
		},
	}
}

// goldenObjects / goldenMultipart are the hex-encoded marshal outputs captured
// from clean HEAD. Regenerate ONLY on an intentional wire change by running the
// _Capture test below and pasting its output.
var goldenObjects = map[string]string{
	"nil_tags":            "1400000000000e0024002000140010000c0004000e00000000f1536500000000180000001c0000000500000000000000000000001400000001000000650000000000000000000000010000006b000000",
	"empty_tags":          "1400000000000e0024002000140010000c0004000e00000000f1536500000000180000001c0000000500000000000000000000001400000001000000650000000000000000000000010000006b000000",
	"multi_tags":          "2400000000001e00280024001c00180014000c000b0000000000000000000000000004001e000000240000000000000301f15365000000008c000000940000002a000000000000009800000003000000540000002400000004000000c0ffffff080000000c0000000000000000000000010000007a000000dcffffff080000001000000005000000616c696365000000050000006f776e657200000008000c00080004000800000008000000100000000400000070726f640000000003000000656e76000400000065746167000000000a000000746578742f706c61696e0000030000006f626a00",
	"tags_parts_segments": "2400000000001e0030002c00240020001c00140000000000000010000f000800000004001e0000002c0000007400000000000001c000000002f1536500000000280100003001000063000000000000002c010000020000002c00000004000000e4ffffff080000000c0000000100000032000000010000006200000008000c000800040008000000080000000c0000000100000031000000010000006100000002000000140000003800000000000a0014001000080004000a00000010000000640000000000000001000000030000007065310000000a000c000800000004000a0000000800000002000000030000007065320002000000180000005000000000000e001c00180010000c00080004000e0000000400000014000000180000000a00000000000000140000000200000067300000030000000102030002000000623000000800140010000400080000001400000000000000000000000400000002000000623100000500000065746167320000000000000000000000040000006f626a3200000000",
}

var goldenMultipart = map[string]string{
	"nil_tags":   "1400000000000e001c001800140010000c0004000e00000000f15365000000001000000014000000180000001c0000000200000063740000010000006b00000002000000626b00000100000075000000",
	"empty_tags": "1400000000000e001c001800140010000c0004000e00000000f15365000000001000000014000000180000001c0000000200000063740000010000006b00000002000000626b00000100000075000000",
	"multi_tags": "18000000000000001000240020001c001800140008000400100000002000000003f15365000000000000000064000000800000008400000088000000020000003000000004000000e0ffffff080000000c00000003000000626f6200050000006f776e657200000008000c00080004000800000008000000100000000400000070726f640000000003000000656e7600180000006170706c69636174696f6e2f6f637465742d73747265616d00000000020000006b32000003000000626b32000200000075320000",
}

// To refresh the golden maps above after an INTENTIONAL wire change, log the
// current marshal output from within one of the assertion tests below (e.g.
// t.Logf("%q: %s", name, hex.EncodeToString(buf))) and paste the values.

// TestByteIdentity_Object asserts marshalObject output is byte-identical to the
// frozen golden captured from clean HEAD. This guards the encode-side tags
// vector-build dedup (round-trip tests prove only semantic equality, not byte
// identity).
func TestByteIdentity_Object(t *testing.T) {
	for name, obj := range byteIdentityObjectFixtures() {
		want, ok := goldenObjects[name]
		require.True(t, ok, "missing golden for %q", name)
		o := obj
		buf, err := marshalObject(&o)
		require.NoError(t, err)
		require.Equal(t, want, hex.EncodeToString(buf), "Object %q wire bytes drifted", name)
	}
}

func TestByteIdentity_MultipartMeta(t *testing.T) {
	for name, m := range byteIdentityMultipartFixtures() {
		want, ok := goldenMultipart[name]
		require.True(t, ok, "missing golden for %q", name)
		buf, err := marshalMultipartMeta(m)
		require.NoError(t, err)
		require.Equal(t, want, hex.EncodeToString(buf), "MultipartMeta %q wire bytes drifted", name)
	}
}
