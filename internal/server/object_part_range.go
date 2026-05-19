package server

import (
	"strconv"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/storage"
)

// partRange resolves S3 ?partNumber=N against an object's Parts list.
// Returns the byte range covered by part N, that part's ETag, the 1-based
// total parts count, and ok=true. Legacy (non-multipart) objects accept
// n == 1 — the whole blob is treated as a single virtual part — and reject
// every other n. ok=false maps to 416 InvalidPartNumber upstream.
func partRange(obj *storage.Object, n int) (start, end int64, etag string, partsCount int, ok bool) {
	if obj == nil || n < 1 {
		return 0, 0, "", 0, false
	}
	if len(obj.Parts) == 0 {
		if n != 1 {
			return 0, 0, "", 0, false
		}
		if obj.Size == 0 {
			return 0, -1, obj.ETag, 1, true
		}
		return 0, obj.Size - 1, obj.ETag, 1, true
	}
	if n > len(obj.Parts) {
		return 0, 0, "", len(obj.Parts), false
	}
	var offset int64
	for i := 0; i < n-1; i++ {
		offset += obj.Parts[i].Size
	}
	p := obj.Parts[n-1]
	if p.Size <= 0 {
		return 0, 0, "", len(obj.Parts), false
	}
	return offset, offset + p.Size - 1, p.ETag, len(obj.Parts), true
}

// readPartNumber pulls ?partNumber from the request and validates it.
// Returns (0, false) when absent; (-1, true) when validation failed and
// the response has already been written (caller returns); (n, true) on
// success.
func readPartNumber(c *app.RequestContext, rangeHeader string) (int, bool) {
	raw := string(c.QueryArgs().Peek("partNumber"))
	if raw == "" {
		return 0, false
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 || v > 10000 {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument",
			"partNumber must be an integer in [1, 10000]")
		return -1, true
	}
	if rangeHeader != "" {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument",
			"Range and partNumber cannot be specified at the same time")
		return -1, true
	}
	return v, true
}
