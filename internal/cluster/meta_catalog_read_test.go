package cluster

import (
	"bytes"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
)

func TestMetaCatalogReadRequest_RoundTrip(t *testing.T) {
	cases := []struct {
		name string
		req  metaCatalogReadRequest
	}{
		{"load-namespace", metaCatalogReadRequest{Op: "load-namespace", Namespace: []string{"analytics", "ingest"}}},
		{"list-namespaces", metaCatalogReadRequest{Op: "list-namespaces"}},
		{"load-table", metaCatalogReadRequest{
			Op:         "load-table",
			Identifier: icebergcatalog.Identifier{Namespace: []string{"a", "b"}, Name: "tbl"},
		}},
		{"list-tables", metaCatalogReadRequest{Op: "list-tables", Namespace: []string{"db"}}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			payload, err := encodeMetaCatalogReadRequest(tc.req)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			out, err := decodeMetaCatalogReadRequest(payload)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if out.Op != tc.req.Op {
				t.Errorf("op: got %q want %q", out.Op, tc.req.Op)
			}
			if !reflect.DeepEqual(out.Namespace, tc.req.Namespace) {
				if !(len(out.Namespace) == 0 && len(tc.req.Namespace) == 0) {
					t.Errorf("namespace: got %v want %v", out.Namespace, tc.req.Namespace)
				}
			}
			if !reflect.DeepEqual(out.Identifier, tc.req.Identifier) {
				if !(out.Identifier.Name == "" && tc.req.Identifier.Name == "" &&
					len(out.Identifier.Namespace) == 0 && len(tc.req.Identifier.Namespace) == 0) {
					t.Errorf("identifier: got %+v want %+v", out.Identifier, tc.req.Identifier)
				}
			}
		})
	}
}

func TestMetaCatalogReadReply_RoundTrip_LoadNamespace(t *testing.T) {
	props := map[string]string{"owner": "team-a", "region": "us-east"}
	reply := &metaLoadTableReply{Properties: props}
	data := encodeMetaLoadTableReply(reply, nil)
	out, err := decodeMetaLoadTableReply(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(out.Properties, props) {
		t.Errorf("properties: got %v want %v", out.Properties, props)
	}
	if out.Table != nil || len(out.Namespaces) != 0 || len(out.Tables) != 0 {
		t.Errorf("expected only Properties populated, got %+v", out)
	}
}

func TestMetaCatalogReadReply_RoundTrip_ListNamespaces(t *testing.T) {
	nss := [][]string{{"a"}, {"a", "b"}, {"a", "b", "c"}}
	reply := &metaLoadTableReply{Namespaces: nss}
	data := encodeMetaLoadTableReply(reply, nil)
	out, err := decodeMetaLoadTableReply(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(out.Namespaces, nss) {
		t.Errorf("namespaces: got %v want %v", out.Namespaces, nss)
	}
}

func TestMetaCatalogReadReply_RoundTrip_LoadTable_64KB_Metadata(t *testing.T) {
	meta := make([]byte, 64*1024)
	for i := range meta {
		meta[i] = byte(i % 251)
	}
	tbl := &icebergcatalog.Table{
		Identifier:       icebergcatalog.Identifier{Namespace: []string{"db"}, Name: "tbl"},
		MetadataLocation: "s3://bucket/v.json",
		Metadata:         meta,
		Properties:       map[string]string{"k": "v"},
	}
	reply := &metaLoadTableReply{Table: tbl}
	data := encodeMetaLoadTableReply(reply, nil)
	out, err := decodeMetaLoadTableReply(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Table == nil {
		t.Fatal("table nil")
	}
	if !bytes.Equal(out.Table.Metadata, meta) {
		t.Errorf("metadata bytes diverged: len got=%d want=%d", len(out.Table.Metadata), len(meta))
	}
	if out.Table.MetadataLocation != tbl.MetadataLocation {
		t.Errorf("location: got %q want %q", out.Table.MetadataLocation, tbl.MetadataLocation)
	}
	if !reflect.DeepEqual(out.Table.Identifier, tbl.Identifier) {
		t.Errorf("identifier: got %+v want %+v", out.Table.Identifier, tbl.Identifier)
	}
	if !reflect.DeepEqual(out.Table.Properties, tbl.Properties) {
		t.Errorf("properties: got %v want %v", out.Table.Properties, tbl.Properties)
	}
}

func TestMetaCatalogReadReply_RoundTrip_ListTables(t *testing.T) {
	tables := make([]icebergcatalog.Identifier, 100)
	for i := range tables {
		tables[i] = icebergcatalog.Identifier{Namespace: []string{"db"}, Name: "t" + strconv.Itoa(i)}
	}
	reply := &metaLoadTableReply{Tables: tables}
	data := encodeMetaLoadTableReply(reply, nil)
	out, err := decodeMetaLoadTableReply(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Tables) != len(tables) {
		t.Fatalf("tables len: got %d want %d", len(out.Tables), len(tables))
	}
	for i := range tables {
		if !reflect.DeepEqual(out.Tables[i], tables[i]) {
			t.Errorf("tables[%d]: got %+v want %+v", i, out.Tables[i], tables[i])
		}
	}
}

func TestMetaCatalogReadReply_AllErrorTypes(t *testing.T) {
	// Real iceberg error symbols (verified in internal/icebergcatalog/store.go).
	cases := []struct {
		name string
		in   error
		want error
	}{
		{"namespace-not-found", icebergcatalog.ErrNamespaceNotFound, icebergcatalog.ErrNamespaceNotFound},
		{"namespace-exists", icebergcatalog.ErrNamespaceExists, icebergcatalog.ErrNamespaceExists},
		{"namespace-not-empty", icebergcatalog.ErrNamespaceNotEmpty, icebergcatalog.ErrNamespaceNotEmpty},
		{"table-not-found", icebergcatalog.ErrTableNotFound, icebergcatalog.ErrTableNotFound},
		{"table-exists", icebergcatalog.ErrTableExists, icebergcatalog.ErrTableExists},
		{"commit-failed", icebergcatalog.ErrCommitFailed, icebergcatalog.ErrCommitFailed},
		// Non-iceberg / unknown errors classify to "service-unavailable" wire
		// type, which decodes back as ErrServiceUnavailable.
		{"synthetic", errors.New("synthetic non-iceberg error"), icebergcatalog.ErrServiceUnavailable},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			data := encodeMetaLoadTableReply(nil, tc.in)
			_, err := decodeMetaLoadTableReply(data)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, tc.want) {
				t.Errorf("error type round-trip: got %v, want errors.Is(_, %v)", err, tc.want)
			}
		})
	}
}

func TestMetaCatalogReadRequest_MalformedFB(t *testing.T) {
	bad := append([]byte(nil), metaCatalogReadRequestMagic...)
	bad = append(bad, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}...)
	_, err := decodeMetaCatalogReadRequest(bad)
	if err == nil {
		t.Fatal("expected malformed-FB error, got nil")
	}
}

func TestMetaCatalogReadReply_MalformedFB(t *testing.T) {
	bad := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	_, err := decodeMetaLoadTableReply(bad)
	if err == nil {
		t.Fatal("expected malformed-FB error, got nil")
	}
}

func TestCatalogReadOp_DriftGuard(t *testing.T) {
	cases := []string{"load-namespace", "list-namespaces", "load-table", "list-tables"}
	for _, op := range cases {
		fb := catalogOpToFB(op)
		if fb == clusterpb.CatalogReadOpUnknown {
			t.Errorf("op %q maps to Unknown enum", op)
		}
		back := catalogOpFromFB(fb)
		if back != op {
			t.Errorf("drift: %q -> FB %v -> %q", op, fb, back)
		}
	}
	if catalogOpFromFB(clusterpb.CatalogReadOpUnknown) != "" {
		t.Error("Unknown FB should decode to empty string")
	}
	if catalogOpToFB("nonsense") != clusterpb.CatalogReadOpUnknown {
		t.Error("nonsense op should map to Unknown")
	}
}
