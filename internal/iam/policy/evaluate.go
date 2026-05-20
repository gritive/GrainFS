package policy

type Decision int

const (
	DecisionDeny Decision = iota
	DecisionAllow
)

func (d Decision) String() string {
	if d == DecisionAllow {
		return "Allow"
	}
	return "Deny"
}

type EvalResult struct {
	Decision      Decision
	MatchedPolicy string
	MatchedSid    string
	Reason        string
	// ConditionContext echoes the IAM condition keys observed at evaluation
	// time (e.g. aws:SourceIp, s3:prefix, the action/resource ARN). Populated
	// from EvalInput.Ctx; empty when no fields were set. Surfaced into
	// audit.s3.condition_context_json so operators can correlate a decision
	// with the exact request facts that drove it. T51' B2 review.
	ConditionContext map[string]string
}

// ConditionContextFromRequest snapshots the request-context fields available
// at evaluation time into a stable map. Empty values are skipped so the
// resulting JSON has no zero-valued keys. Keys use the standard AWS/S3
// condition-key vocabulary so consumers can filter without translation.
// Exported so short-circuit paths in s3auth.Authorizer (which don't run
// Evaluate) can attach the same context shape to their EvalResults.
func ConditionContextFromRequest(c RequestContext) map[string]string {
	var out map[string]string
	put := func(k, v string) {
		if v == "" {
			return
		}
		if out == nil {
			out = make(map[string]string, 4)
		}
		out[k] = v
	}
	put("aws:Action", c.Action)
	put("aws:Resource", c.Resource)
	put("aws:SourceIp", c.SourceIP)
	put("s3:prefix", c.Prefix)
	return out
}

type EvalInput struct {
	PrincipalPolicies []*Document
	ResourcePolicy    *Document
	// PrincipalPolicyNames runs parallel to PrincipalPolicies and carries the
	// catalog name of each document so Evaluate can surface MatchedPolicy. May
	// be nil or shorter than PrincipalPolicies; missing entries yield "".
	PrincipalPolicyNames []string
	// ResourcePolicyBucket names the bucket the ResourcePolicy is attached to;
	// used to label MatchedPolicy ("bucket:<name>") for bucket-policy matches.
	// May be empty.
	ResourcePolicyBucket string
	Principal            string
	Ctx                  RequestContext
	AllowAnonBucket      bool
}

// principalPolicyName returns the catalog name for the i-th principal policy
// in the slice, or "" when names are unavailable.
func principalPolicyName(names []string, i int) string {
	if i < 0 || i >= len(names) {
		return ""
	}
	return names[i]
}

// bucketPolicyMatchID renders the MatchedPolicy label for a resource-policy
// match: "bucket:<name>" when bucket is known, "bucket" otherwise.
func bucketPolicyMatchID(bucket string) string {
	if bucket == "" {
		return "bucket"
	}
	return "bucket:" + bucket
}

// Evaluate implements: explicit Deny > explicit Allow > implicit Deny.
// Union of principal-attached and resource-attached policies. Every return
// path attaches a ConditionContext snapshot of the request facts that drove
// the decision; the snapshot is the same for allow / deny / implicit-deny so
// audit consumers see "what was the request" regardless of outcome.
func Evaluate(in EvalInput) EvalResult {
	cc := ConditionContextFromRequest(in.Ctx)
	allowMatch := ""
	allowSid := ""
	allowPolicy := ""
	for i, doc := range in.PrincipalPolicies {
		if doc == nil {
			continue
		}
		for _, st := range doc.Statement {
			if st.Effect != EffectDeny {
				continue
			}
			if st.matches(in.Ctx) {
				return EvalResult{
					Decision:         DecisionDeny,
					MatchedPolicy:    principalPolicyName(in.PrincipalPolicyNames, i),
					MatchedSid:       st.Sid,
					Reason:           "explicit Deny on principal policy",
					ConditionContext: cc,
				}
			}
		}
	}
	if in.ResourcePolicy != nil {
		for _, st := range in.ResourcePolicy.Statement {
			if st.Effect != EffectDeny {
				continue
			}
			if !principalMatches(st.Principal, in.Principal, in.AllowAnonBucket) {
				continue
			}
			if st.matches(in.Ctx) {
				return EvalResult{
					Decision:         DecisionDeny,
					MatchedPolicy:    bucketPolicyMatchID(in.ResourcePolicyBucket),
					MatchedSid:       st.Sid,
					Reason:           "explicit Deny on bucket policy",
					ConditionContext: cc,
				}
			}
		}
	}
	for i, doc := range in.PrincipalPolicies {
		if doc == nil {
			continue
		}
		for _, st := range doc.Statement {
			if st.Effect != EffectAllow {
				continue
			}
			if st.matches(in.Ctx) {
				allowSid = st.Sid
				allowMatch = "principal policy"
				allowPolicy = principalPolicyName(in.PrincipalPolicyNames, i)
			}
		}
	}
	if in.ResourcePolicy != nil {
		for _, st := range in.ResourcePolicy.Statement {
			if st.Effect != EffectAllow {
				continue
			}
			if !principalMatches(st.Principal, in.Principal, in.AllowAnonBucket) {
				continue
			}
			if st.matches(in.Ctx) {
				allowSid = st.Sid
				allowMatch = "bucket policy"
				allowPolicy = bucketPolicyMatchID(in.ResourcePolicyBucket)
			}
		}
	}
	if allowMatch != "" {
		return EvalResult{
			Decision:         DecisionAllow,
			MatchedPolicy:    allowPolicy,
			MatchedSid:       allowSid,
			Reason:           "explicit Allow on " + allowMatch,
			ConditionContext: cc,
		}
	}
	return EvalResult{
		Decision:         DecisionDeny,
		Reason:           "implicit Deny (no statement matched)",
		ConditionContext: cc,
	}
}

func principalMatches(p *StringOrMap, principal string, allowAnon bool) bool {
	if p == nil {
		return false
	}
	if p.Star {
		return allowAnon
	}
	for _, vs := range p.Named {
		for _, v := range vs {
			if v == "*" {
				// Named-form wildcard (e.g. Principal:{"AWS":["*"]}) must obey the same
				// AllowAnonBucket gate as the top-level Star form. Without this, an
				// operator could write a bucket policy with {"Principal":{"AWS":["*"]}}
				// and bypass both iam.anon-enabled and iam.allow-anonymous-bucket-policy.
				if allowAnon {
					return true
				}
				continue
			}
			if v == principal {
				return true
			}
		}
	}
	return false
}
