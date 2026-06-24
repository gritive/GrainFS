package server

import "testing"

func TestServiceMetricRouteClassifiesS3Operations(t *testing.T) {
	tests := []struct {
		name      string
		method    string
		path      string
		query     string
		service   string
		operation string
	}{
		{name: "list buckets", method: "GET", path: "/", service: "s3", operation: "ListBuckets"},
		{name: "put object", method: "PUT", path: "/bucket/key", service: "s3", operation: "PutObject"},
		{name: "get object", method: "GET", path: "/bucket/key", service: "s3", operation: "GetObject"},
		{name: "list bucket", method: "GET", path: "/bucket", service: "s3", operation: "ListBucket"},
		{name: "delete objects", method: "POST", path: "/bucket", query: "delete=", service: "s3", operation: "DeleteObjects"},
		{name: "create multipart", method: "POST", path: "/bucket/key", query: "uploads=", service: "s3", operation: "CreateMultipartUpload"},
		{name: "upload part", method: "PUT", path: "/bucket/key", query: "uploadId=u&partNumber=1", service: "s3", operation: "UploadPart"},
		{name: "complete multipart", method: "POST", path: "/bucket/key", query: "uploadId=u", service: "s3", operation: "CompleteMultipartUpload"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyServiceMetricRoute(tt.method, tt.path, tt.query)
			if got.service != tt.service || got.operation != tt.operation {
				t.Fatalf("route = %#v, want service=%q operation=%q", got, tt.service, tt.operation)
			}
		})
	}
}

func TestServiceMetricRouteClassifiesNonS3Services(t *testing.T) {
	tests := []struct {
		method    string
		path      string
		service   string
		operation string
	}{
		{method: "GET", path: "/metrics", service: "metrics", operation: "Scrape"},
		{method: "GET", path: "/api/cluster/status", service: "cluster", operation: "Status"},
		{method: "GET", path: "/api", service: "admin", operation: "API"},
		{method: "GET", path: "/api/incidents", service: "admin", operation: "Incidents"},
		{method: "GET", path: "/admin", service: "admin", operation: "API"},
		{method: "GET", path: "/ui/", service: "dashboard", operation: "UI"},
	}
	for _, tt := range tests {
		t.Run(tt.service+"_"+tt.operation, func(t *testing.T) {
			got := classifyServiceMetricRoute(tt.method, tt.path, "")
			if got.service != tt.service || got.operation != tt.operation {
				t.Fatalf("%s %s route = %#v, want service=%q operation=%q", tt.method, tt.path, got, tt.service, tt.operation)
			}
		})
	}
}

func TestServiceMetricStatusClass(t *testing.T) {
	if got := serviceMetricStatusClass(204); got != "2xx" {
		t.Fatalf("status class = %q, want 2xx", got)
	}
	if got := serviceMetricStatusClass(503); got != "5xx" {
		t.Fatalf("status class = %q, want 5xx", got)
	}
}
