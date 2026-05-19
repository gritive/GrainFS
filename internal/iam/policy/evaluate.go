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
	Principal         string
	Ctx               RequestContext
	AllowAnonBucket   bool
}

// Evaluate implements: explicit Deny > explicit Allow > implicit Deny.
// Union of principal-attached and resource-attached policies.
func Evaluate(in EvalInput) EvalResult {
	allowMatch := ""
	allowSid := ""
	for _, doc := range in.PrincipalPolicies {
		if doc == nil {
			continue
		}
		for _, st := range doc.Statement {
			if st.Effect != EffectDeny {
				continue
			}
			if st.matches(in.Ctx) {
				return EvalResult{Decision: DecisionDeny, MatchedSid: st.Sid, Reason: "explicit Deny on principal policy"}
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
				return EvalResult{Decision: DecisionDeny, MatchedSid: st.Sid, Reason: "explicit Deny on bucket policy"}
			}
		}
	}
	for _, doc := range in.PrincipalPolicies {
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
			}
		}
	}
	if allowMatch != "" {
		return EvalResult{Decision: DecisionAllow, MatchedSid: allowSid, Reason: "explicit Allow on " + allowMatch}
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
