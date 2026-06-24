// internal/audit/schema.go
package audit

// AnonSAID is the canonical sentinel for anonymous (unauthenticated) requests.
// Using a non-empty sentinel lets log consumers distinguish anonymous traffic
// from genuine empty-attribution bugs via sa_id = '(anonymous)'.
const AnonSAID = "(anonymous)"
