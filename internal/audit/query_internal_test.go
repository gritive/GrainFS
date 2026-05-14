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
