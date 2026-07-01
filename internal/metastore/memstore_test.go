package metastore_test

import (
	"testing"

	"github.com/gritive/GrainFS/internal/metastore"
	"github.com/gritive/GrainFS/internal/metastore/storetest"
)

func TestMemStoreConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) metastore.Store {
		return metastore.NewMemStore()
	})
}
