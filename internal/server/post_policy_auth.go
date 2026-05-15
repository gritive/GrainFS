package server

import (
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/s3auth"
)

func (s *Server) validateFormUploadPolicy(c *app.RequestContext, values map[string][]string, bucket, key string) bool {
	policyB64 := firstFormValue(values, "policy")
	sig := firstFormValue(values, "X-Amz-Signature")
	if policyB64 == "" || sig == "" {
		writeXMLError(c, consts.StatusForbidden, "AccessDenied", "missing POST policy signature")
		return false
	}

	if err := s3auth.ValidatePostPolicyExpiration(policyB64); err != nil {
		writeXMLError(c, consts.StatusForbidden, "AccessDenied", err.Error())
		return false
	}

	formFields := map[string]string{
		"bucket": bucket,
		"key":    key,
	}
	for k, vs := range values {
		if len(vs) > 0 {
			formFields[k] = vs[0]
		}
	}
	if err := s3auth.ValidatePostPolicyConditions(policyB64, formFields); err != nil {
		writeXMLError(c, consts.StatusForbidden, "AccessDenied", err.Error())
		return false
	}

	parts := strings.SplitN(firstFormValue(values, "X-Amz-Credential"), "/", 5)
	if len(parts) != 5 {
		writeXMLError(c, consts.StatusForbidden, "AccessDenied", "invalid credential")
		return false
	}
	secretKey := s.postPolicySigningSecret(parts[0])
	if secretKey == "" {
		writeXMLError(c, consts.StatusForbidden, "AccessDenied", "invalid access key")
		return false
	}
	if err := s3auth.VerifyPostPolicy(policyB64, sig, secretKey, parts[1], parts[2], parts[3]); err != nil {
		writeXMLError(c, consts.StatusForbidden, "SignatureDoesNotMatch", err.Error())
		return false
	}
	return true
}

func firstFormValue(values map[string][]string, key string) string {
	if vs := values[key]; len(vs) > 0 {
		return vs[0]
	}
	return ""
}
