package server

import (
	"fmt"
	"strconv"
	"strings"
)

// icebergDefaultWarehouse is the canonical warehouse key for single-warehouse
// (legacy §1-§3) deployments. It mirrors cluster.IcebergDefaultWarehouse; the
// constant is copied here to avoid an import cycle (internal/icebergcatalog
// cannot import internal/cluster which already imports icebergcatalog).
const icebergDefaultWarehouse = "default"

// icebergTableBasePath constructs the base S3 URI for an Iceberg table.
//
// The warehouse segment is omitted (backward-compatible §1-§3 layout) when:
//   - warehouse is "" (unset / anonymous SigV4 path)
//   - warehouse is "default" (canonical single-warehouse FSM key)
//   - warehouse is an s3:// URI (legacy Store.Warehouse() returns the full URI,
//     which is already the prefix — inserting it again would double the path)
//
// For any other logical warehouse name (a bearer JWT claim.Warehouse that is
// not "default"), the warehouse segment is included so that two distinct
// warehouses sharing the same (ns, table) name produce distinct object paths:
//
//	s3Prefix + "/" + warehouse + "/" + ns + "/" + table
func icebergTableBasePath(s3Prefix, warehouse, ns, table string) string {
	if warehouse == "" || warehouse == icebergDefaultWarehouse || strings.HasPrefix(warehouse, "s3://") {
		return fmt.Sprintf("%s/%s/%s", s3Prefix, ns, table)
	}
	return fmt.Sprintf("%s/%s/%s/%s", s3Prefix, warehouse, ns, table)
}

func nextIcebergMetadataLocation(current string) string {
	const suffix = ".json"
	if !strings.HasSuffix(current, suffix) {
		return current
	}
	slash := strings.LastIndex(current, "/")
	if slash < 0 {
		return current
	}
	prefix := current[:slash+1]
	name := strings.TrimSuffix(current[slash+1:], suffix)
	n, err := strconv.Atoi(name)
	if err != nil {
		return current
	}
	return fmt.Sprintf("%s%05d%s", prefix, n+1, suffix)
}
