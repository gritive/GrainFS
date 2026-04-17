//go:build !unix

package packblob

import "os"

func acquireBlobDirLock(dir string) (*os.File, error) { return nil, nil }
func releaseBlobDirLock(f *os.File)                   {}
