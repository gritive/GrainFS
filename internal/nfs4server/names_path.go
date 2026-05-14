package nfs4server

import "strings"

// extractBucketAndKey splits a normalized NFS path into (bucket, key).
//
//	""              -> ("", "")
//	"/"             -> ("", "")             pseudo-root
//	"/bucket"       -> ("bucket", "")
//	"/bucket/"      -> ("bucket", "")
//	"/bucket/key"   -> ("bucket", "key")
//	"/bucket/a/b/c" -> ("bucket", "a/b/c")
func extractBucketAndKey(p string) (bucket, key string) {
	if p == "" || p == "/" {
		return "", ""
	}
	p = strings.TrimPrefix(p, "/")
	slash := strings.IndexByte(p, '/')
	if slash < 0 {
		return p, ""
	}
	return p[:slash], p[slash+1:]
}

func objectLockKey(bucket, key string) string {
	return bucket + "\x00" + key
}
