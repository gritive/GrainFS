package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServiceRequestCount_SumsAllLabelCombos(t *testing.T) {
	before := ServiceRequestCount()

	ServiceRequestsTotal.WithLabelValues("s3", "PutObject", "PUT", "2xx").Inc()
	ServiceRequestsTotal.WithLabelValues("s3", "GetObject", "GET", "2xx").Add(3)
	ServiceRequestsTotal.WithLabelValues("s3", "GetObject", "GET", "5xx").Inc()

	after := ServiceRequestCount()
	assert.Equal(t, 5.0, after-before, "must sum across all label combinations")
}
