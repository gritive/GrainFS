package server

import (
	"strconv"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/gritive/GrainFS/internal/metrics"
)

type serviceMetricRoute struct {
	service   string
	operation string
}

func recordServiceMetrics(c *app.RequestContext, method string, status int, durationSeconds float64) {
	route := classifyServiceMetricRoute(method, string(c.URI().Path()), string(c.URI().QueryString()))
	statusClass := serviceMetricStatusClass(status)

	metrics.ServiceRequestsTotal.WithLabelValues(route.service, route.operation, method, statusClass).Inc()
	metrics.ServiceRequestDuration.WithLabelValues(route.service, route.operation, method, statusClass).Observe(durationSeconds)
	metrics.ServiceRequestBytesTotal.WithLabelValues(route.service, route.operation, method, statusClass).Add(serviceMetricRequestBytes(c))
	metrics.ServiceResponseBytesTotal.WithLabelValues(route.service, route.operation, method, statusClass).Add(serviceMetricResponseBytes(c))
}

func classifyServiceMetricRoute(method, path, rawQuery string) serviceMetricRoute {
	if path == routePathMetrics {
		return serviceMetricRoute{service: "metrics", operation: "Scrape"}
	}
	if strings.HasPrefix(path, "/api/cluster/") {
		return serviceMetricRoute{service: "cluster", operation: classifyAPIOperation(path)}
	}
	if path == strings.TrimSuffix(routePrefixAPI, "/") || path == strings.TrimSuffix(routePrefixAdmin, "/") ||
		strings.HasPrefix(path, routePrefixAPI) || strings.HasPrefix(path, routePrefixAdmin) {
		return serviceMetricRoute{service: "admin", operation: classifyAPIOperation(path)}
	}
	if path == routePathUI || strings.HasPrefix(path, routePrefixUI) {
		return serviceMetricRoute{service: "dashboard", operation: "UI"}
	}
	if isS3MetricPath(path) {
		return serviceMetricRoute{service: "s3", operation: classifyS3ServiceOperation(method, path, rawQuery)}
	}
	return serviceMetricRoute{service: "other", operation: method}
}

func isS3MetricPath(path string) bool {
	if path == "/" {
		return true
	}
	if !strings.HasPrefix(path, "/") {
		return false
	}
	first := strings.TrimPrefix(path, "/")
	if first == "" {
		return true
	}
	first, _, _ = strings.Cut(first, "/")
	switch first {
	case "api", "admin", "metrics", "ui":
		return false
	default:
		return true
	}
}

func classifyS3ServiceOperation(method, path, rawQuery string) string {
	hasKey := strings.Count(strings.Trim(path, "/"), "/") > 0
	hasUploadID := queryHas(rawQuery, "uploadId")
	switch {
	case method == "GET" && path == "/":
		return "ListBuckets"
	case method == "POST" && !hasKey && queryHas(rawQuery, "delete"):
		return "DeleteObjects"
	case method == "POST" && hasKey && queryHas(rawQuery, "uploads"):
		return "CreateMultipartUpload"
	case method == "PUT" && hasKey && hasUploadID && queryHas(rawQuery, "partNumber"):
		return "UploadPart"
	case method == "POST" && hasKey && hasUploadID:
		return "CompleteMultipartUpload"
	case method == "DELETE" && hasKey && hasUploadID:
		return "AbortMultipartUpload"
	case method == "GET" && hasKey && hasUploadID:
		return "ListParts"
	case method == "GET" && hasKey:
		return "GetObject"
	case method == "HEAD" && hasKey:
		return "HeadObject"
	case method == "PUT" && hasKey:
		return "PutObject"
	case method == "DELETE" && hasKey:
		return "DeleteObject"
	case method == "GET":
		return "ListBucket"
	case method == "HEAD":
		return "HeadBucket"
	case method == "PUT":
		return "CreateBucket"
	case method == "DELETE":
		return "DeleteBucket"
	default:
		return method
	}
}

func classifyAPIOperation(path string) string {
	switch {
	case path == routePathClusterStatus:
		return "Status"
	case path == routePathClusterPlacement:
		return "Placement"
	case path == routePathClusterBalancer:
		return "BalancerStatus"
	case path == routePathIncidents || strings.HasPrefix(path, routePrefixIncidents):
		return "Incidents"
	case path == routePathEvents || path == routePathEventLog:
		return "Events"
	default:
		return "API"
	}
}

func queryHas(rawQuery, key string) bool {
	for _, part := range strings.Split(rawQuery, "&") {
		name, _, _ := strings.Cut(part, "=")
		if name == key {
			return true
		}
	}
	return false
}

func serviceMetricStatusClass(status int) string {
	if status < 100 {
		return "unknown"
	}
	return strconv.Itoa(status/100) + "xx"
}

func serviceMetricRequestBytes(c *app.RequestContext) float64 {
	if n := c.Request.Header.ContentLength(); n > 0 {
		return float64(n)
	}
	return float64(len(c.Request.BodyBytes()))
}

func serviceMetricResponseBytes(c *app.RequestContext) float64 {
	if n := c.Response.Header.ContentLength(); n > 0 {
		return float64(n)
	}
	return float64(len(c.Response.BodyBytes()))
}
