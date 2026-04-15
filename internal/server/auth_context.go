package server

import "context"

type contextKey string

const accessKeyContextKey contextKey = "accessKey"

// WithAccessKey returns a context with the given access key attached.
func WithAccessKey(ctx context.Context, accessKey string) context.Context {
	return context.WithValue(ctx, accessKeyContextKey, accessKey)
}

// AccessKeyFromContext returns the access key from the context, or empty string.
func AccessKeyFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(accessKeyContextKey).(string); ok {
		return v
	}
	return ""
}
