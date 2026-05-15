package server

import (
	"fmt"
	"strconv"
	"strings"
)

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
