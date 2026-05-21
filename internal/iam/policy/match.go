package policy

import (
	"net"
	"strings"
)

// PrincipalType selects which attach pool the Resolver consults when
// materializing principal-attached policies. Default zero-value is
// PrincipalTypeS3 so existing S3 callers don't need to set the field.
//
// FU#5 (F-§B-resolver-mountsa): mount-SA names live in a separate attach
// pool (policyattach.MountSAPolicies) than S3 SAs. Without this enum the
// resolver always reads SAPolicies and a mount-SA principal returns an
// empty policy set even when 9PAttachOnly / NFSMountOnly is attached.
type PrincipalType uint8

const (
	// PrincipalTypeS3 selects the S3 service-account pool (SAPolicies +
	// SAGroups → GroupPolicies). Zero value, default for any caller that
	// does not set the field.
	PrincipalTypeS3 PrincipalType = 0
	// PrincipalTypeMount selects the NFS/9P mount service-account pool
	// (MountSAPolicies only — mount-SA does not expand through groups,
	// matching the cross-namespace attach-time reject semantics from
	// NFS§A T4).
	PrincipalTypeMount PrincipalType = 1
)

type RequestContext struct {
	Action   string // "s3:GetObject"
	Resource string // "arn:aws:s3:::analytics/logs/foo"
	SourceIP string
	Prefix   string // set only on list-like requests; empty otherwise
	// PrincipalType selects which attach pool the Resolver reads for the
	// principal. Defaults to PrincipalTypeS3 — NFS/9P callers set this to
	// PrincipalTypeMount when invoking Authorize with a mount-SA name.
	PrincipalType PrincipalType
}

func matchAction(pattern, req string) bool {
	if pattern == "*" || pattern == req {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return false
	}
	return wildcardMatch(pattern, req)
}

// wildcardMatch implements the standard `*`-glob with iterative two-pointer backtrack.
// `*` matches any sequence including empty. No `?` support.
func wildcardMatch(pattern, s string) bool {
	pi, si, starP, starS := 0, 0, -1, 0
	for si < len(s) {
		if pi < len(pattern) && pattern[pi] == s[si] {
			pi++
			si++
		} else if pi < len(pattern) && pattern[pi] == '*' {
			starP = pi
			starS = si
			pi++
		} else if starP != -1 {
			pi = starP + 1
			starS++
			si = starS
		} else {
			return false
		}
	}
	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}
	return pi == len(pattern)
}

func matchResource(pattern, req string) bool {
	if pattern == "*" {
		return true
	}
	return wildcardMatch(pattern, req)
}

func matchCondition(cond map[string]map[string]StringOrSlice, ctx RequestContext) bool {
	for op, kv := range cond {
		for key, values := range kv {
			if !evalCond(op, key, values, ctx) {
				return false
			}
		}
	}
	return true
}

func evalCond(op, key string, values StringOrSlice, ctx RequestContext) bool {
	switch key {
	case "aws:SourceIp":
		if ctx.SourceIP == "" {
			return false
		}
		ip := net.ParseIP(ctx.SourceIP)
		if ip == nil {
			return false
		}
		matched := false
		for _, v := range values {
			_, cidr, err := net.ParseCIDR(v)
			if err != nil {
				continue
			}
			if cidr.Contains(ip) {
				matched = true
				break
			}
		}
		if op == "NotIpAddress" {
			return !matched
		}
		return matched
	case "s3:prefix":
		// F#13: absent key on non-list → statement does not match
		if ctx.Prefix == "" && ctx.Action != "s3:ListBucket" {
			return false
		}
		for _, v := range values {
			if op == "StringEquals" && ctx.Prefix == v {
				return true
			}
			if op == "StringLike" && wildcardMatch(v, ctx.Prefix) {
				return true
			}
		}
		return false
	}
	return false
}

// matches returns true iff Action AND Resource AND (Condition if present) all match.
func (s *Statement) matches(ctx RequestContext) bool {
	any := false
	for _, a := range s.Action {
		if matchAction(a, ctx.Action) {
			any = true
			break
		}
	}
	if !any {
		return false
	}
	any = false
	for _, r := range s.Resource {
		if matchResource(r, ctx.Resource) {
			any = true
			break
		}
	}
	if !any {
		return false
	}
	if len(s.Condition) > 0 && !matchCondition(s.Condition, ctx) {
		return false
	}
	return true
}
