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
//   - warehouse equals s3Prefix exactly (legacy Store.Warehouse() returns the
//     full S3 URI as its own name; in that case the prefix already encodes the
//     warehouse — inserting it again would double the path)
//
// The s3:// exemption is intentionally scoped to warehouse == s3Prefix: in
// MetaCatalog mode the logical warehouse name and the S3 prefix are distinct
// values passed as separate arguments, so a URI-shaped logical name (e.g. a
// crafted bearer claim "s3://attacker/x") must NOT be treated as its own
// prefix and MUST include the warehouse segment (defense in depth for F24).
//
// For any other logical warehouse name (a bearer JWT claim.Warehouse that is
// not "default"), the warehouse segment is included so that two distinct
// warehouses sharing the same (ns, table) name produce distinct object paths:
//
//	s3Prefix + "/" + warehouse + "/" + ns + "/" + table
func icebergTableBasePath(s3Prefix, warehouse, ns, table string) string {
	if warehouse == "" || warehouse == icebergDefaultWarehouse || warehouse == s3Prefix {
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
