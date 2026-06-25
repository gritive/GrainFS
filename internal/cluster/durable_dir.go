package cluster

import (
	"fmt"
	"path/filepath"
)

func syncDirChainNoDedup(leaf, stop string, fsyncDir func(string) error) error {
	stop = filepath.Clean(stop)
	leaf = filepath.Clean(leaf)
	if leaf == stop {
		return nil
	}
	if err := fsyncDir(leaf); err != nil {
		return err
	}
	for d := leaf; ; {
		parent := filepath.Dir(d)
		if parent == stop {
			return nil
		}
		if parent == d {
			return fmt.Errorf("syncDirChainNoDedup: stop %q is not an ancestor of %q", stop, leaf)
		}
		if err := fsyncDir(parent); err != nil {
			return err
		}
		d = parent
	}
}
