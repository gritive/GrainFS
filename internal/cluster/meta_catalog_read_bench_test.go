package cluster

import (
	"strconv"
	"testing"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
)

// makeBigMetadata returns a synthetic Iceberg-shaped JSON blob of approximately
// `size` bytes. We pad with deterministic content so allocations are stable
// across runs (no random source).
func makeBigMetadata(size int) []byte {
	const head = `{"format-version":2,"location":"s3://bucket/warehouse/db/tbl","schemas":[],"properties":{`
	const tail = `}}`
	out := make([]byte, 0, size)
	out = append(out, head...)
	i := 0
	for len(out) < size-len(tail) {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, '"', 'k')
		out = strconv.AppendInt(out, int64(i), 10)
		out = append(out, '"', ':', '"', 'v')
		out = strconv.AppendInt(out, int64(i), 10)
		out = append(out, '"')
		i++
	}
	out = append(out, tail...)
	return out
}

func benchRequest_LoadNamespace() metaCatalogReadRequest {
	return metaCatalogReadRequest{Op: "load-namespace", Namespace: []string{"analytics", "ingest"}}
}

func benchRequest_LoadTable() metaCatalogReadRequest {
	return metaCatalogReadRequest{
		Op:         "load-table",
		Identifier: icebergcatalog.Identifier{Namespace: []string{"analytics", "ingest"}, Name: "events"},
	}
}

func benchRequest_ListTables1K() metaCatalogReadRequest {
	return metaCatalogReadRequest{Op: "list-tables", Namespace: []string{"analytics"}}
}

func benchReply_LoadNamespace() *metaLoadTableReply {
	props := make(map[string]string, 32)
	for i := 0; i < 32; i++ {
		props["k"+strconv.Itoa(i)] = "v" + strconv.Itoa(i)
	}
	return &metaLoadTableReply{Properties: props}
}

func benchReply_LoadTable_64KB() *metaLoadTableReply {
	return &metaLoadTableReply{
		Table: &icebergcatalog.Table{
			Identifier:       icebergcatalog.Identifier{Namespace: []string{"analytics", "ingest"}, Name: "events"},
			MetadataLocation: "s3://bucket/warehouse/db/tbl/metadata/v1.metadata.json",
			Metadata:         makeBigMetadata(64 * 1024),
			Properties:       map[string]string{"owner": "ingest-team"},
		},
	}
}

func benchReply_ListTables1K() *metaLoadTableReply {
	tables := make([]icebergcatalog.Identifier, 1000)
	for i := range tables {
		tables[i] = icebergcatalog.Identifier{
			Namespace: []string{"analytics"},
			Name:      "t" + strconv.Itoa(i),
		}
	}
	return &metaLoadTableReply{Tables: tables}
}

func BenchmarkMetaCatalogReadRequest_RoundTrip(b *testing.B) {
	cases := []struct {
		name string
		req  metaCatalogReadRequest
	}{
		{"load-namespace", benchRequest_LoadNamespace()},
		{"load-table", benchRequest_LoadTable()},
		{"list-tables-1k", benchRequest_ListTables1K()},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				data, err := encodeMetaCatalogReadRequest(tc.req)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := decodeMetaCatalogReadRequest(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMetaCatalogReadReply_RoundTrip(b *testing.B) {
	cases := []struct {
		name  string
		reply *metaLoadTableReply
	}{
		{"load-namespace", benchReply_LoadNamespace()},
		{"load-table-64KB", benchReply_LoadTable_64KB()},
		{"list-tables-1k", benchReply_ListTables1K()},
	}
	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				data := encodeMetaLoadTableReply(tc.reply, nil)
				_, err := decodeMetaLoadTableReply(data)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
