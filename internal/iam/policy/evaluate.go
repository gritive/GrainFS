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
// Union of principal-attached and resource-attached policies.
func Evaluate(in EvalInput) EvalResult {
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
					Decision:      DecisionDeny,
					MatchedPolicy: principalPolicyName(in.PrincipalPolicyNames, i),
					MatchedSid:    st.Sid,
					Reason:        "explicit Deny on principal policy",
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
					Decision:      DecisionDeny,
					MatchedPolicy: bucketPolicyMatchID(in.ResourcePolicyBucket),
					MatchedSid:    st.Sid,
					Reason:        "explicit Deny on bucket policy",
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
			Decision:      DecisionAllow,
			MatchedPolicy: allowPolicy,
			MatchedSid:    allowSid,
			Reason:        "explicit Allow on " + allowMatch,
		}
	}
	return EvalResult{Decision: DecisionDeny, Reason: "implicit Deny (no statement matched)"}
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
