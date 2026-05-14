package adminapi

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorHelpFieldsMarshalWhenSet(t *testing.T) {
	raw, err := json.Marshal((&Error{Code: "invalid", Message: "bad"}).
		WithParam("bucket").
		WithHelp("create the bucket first").
		WithDocs("https://example.test/docs"))
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Equal(t, "bucket", got["param"])
	require.Equal(t, "create the bucket first", got["help"])
	require.Equal(t, "https://example.test/docs", got["docs_url"])
}

func TestErrorHelpFieldsOmitWhenEmpty(t *testing.T) {
	raw, err := json.Marshal(&Error{Code: "invalid", Message: "bad"})
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(raw, &got))
	require.NotContains(t, got, "param")
	require.NotContains(t, got, "help")
	require.NotContains(t, got, "docs_url")
}
