package audit

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigureDuckDBConnectionPoolPinsSingleConnection(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	configureDuckDBConnectionPool(db)

	require.Equal(t, 1, db.Stats().MaxOpenConnections)
}

func TestDuckDBSearcherSetupSQLUsesAuditWarehouse(t *testing.T) {
	searcher := NewDuckDBSearcher(DuckDBSearchConfig{
		Endpoint:  "http://127.0.0.1:9000",
		AccessKey: "AK-test",
		SecretKey: "secret",
	})

	require.Contains(t, searcher.setupSQL(), "OAUTH2_SCOPE 'PRINCIPAL_ROLE:"+Warehouse+"'")
	require.Contains(t, searcher.setupSQL(), "ATTACH '"+Warehouse+"' AS grainfs_iceberg")
}
