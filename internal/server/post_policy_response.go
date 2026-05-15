package server

import (
	"fmt"
	"net/url"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

func writeFormUploadSuccess(c *app.RequestContext, values map[string][]string, bucket, key, etag string) {
	if redirectURL := values["success_action_redirect"]; len(redirectURL) > 0 && redirectURL[0] != "" {
		u, err := url.Parse(redirectURL[0])
		if err == nil {
			q := u.Query()
			q.Set("bucket", bucket)
			q.Set("key", key)
			q.Set("etag", etag)
			u.RawQuery = q.Encode()
			c.Redirect(consts.StatusSeeOther, []byte(u.String()))
			return
		}
	}

	statusStr := "204"
	if ss := values["success_action_status"]; len(ss) > 0 {
		statusStr = ss[0]
	}

	switch statusStr {
	case "200":
		c.Status(consts.StatusOK)
	case "201":
		result := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<PostResponse>
  <Location>%s/%s/%s</Location>
  <Bucket>%s</Bucket>
  <Key>%s</Key>
  <ETag>"%s"</ETag>
</PostResponse>`, "", bucket, key, bucket, key, etag)
		c.Data(consts.StatusCreated, "application/xml", []byte(result))
	default:
		c.Status(consts.StatusNoContent)
	}
}
