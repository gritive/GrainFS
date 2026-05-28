package snapshot

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/gritive/GrainFS/internal/encrypt"
)

const snapshotHeaderPeekCap = 4096

func CountSnapshotsSealedUnderKEK(dir string, version uint32) (uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("snapshot: kek-ref scan: read dir: %w", err)
	}
	var count uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "snapshot-") {
			continue
		}
		if !strings.HasSuffix(e.Name(), ".json.zst") && !strings.HasSuffix(e.Name(), ".json.zst.tmp") {
			continue
		}
		head, err := readFilePrefix(filepath.Join(dir, e.Name()), snapshotHeaderPeekCap)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, fmt.Errorf("snapshot: kek-ref scan: read %s: %w", e.Name(), err)
		}
		if !encrypt.IsSnapshotEnvelope(head) {
			continue
		}
		hdr, _, _, err := encrypt.PeekSnapshotEnvelopeHeader(head)
		if err != nil {
			count++ // fail closed
			continue
		}
		if hdr.ActiveKEKVersion() == version {
			count++
		}
	}
	return count, nil
}

func readFilePrefix(path string, n int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, n)
	read, err := io.ReadFull(f, buf)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return buf[:read], nil
	}
	if err != nil {
		return nil, err
	}
	return buf[:read], nil
}
