package server

import "github.com/cloudwego/hertz/pkg/app"

func (s *Server) validateFormUploadPolicyIfConfigured(c *app.RequestContext, formValues map[string][]string, bucket, key string) bool {
	if s.verifier == nil {
		return true
	}
	return s.validateFormUploadPolicy(c, formValues, bucket, key)
}
