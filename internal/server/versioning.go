package server

import (
	"context"
	"encoding/xml"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/storage"
)

// versioningConfiguration is the S3 XML body for GET/PUT ?versioning.
type versioningConfiguration struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Status  string   `xml:"Status"`
}

// BucketVersioner is implemented by backends that support bucket versioning.
type BucketVersioner interface {
	SetBucketVersioning(bucket, state string) error
	GetBucketVersioning(bucket string) (string, error)
}

func findBucketVersioner(b storage.Backend) (BucketVersioner, bool) {
	for b != nil {
		if v, ok := b.(BucketVersioner); ok {
			return v, true
		}
		u, ok := b.(unwrapper)
		if !ok {
			break
		}
		b = u.Unwrap()
	}
	return nil, false
}

// putBucketVersioning handles PUT /<bucket>?versioning.
func (s *Server) putBucketVersioning(c *app.RequestContext, bucket string) {
	v, ok := findBucketVersioner(s.backend)
	if !ok {
		writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", "versioning not supported by this backend")
		return
	}

	var vc versioningConfiguration
	if err := xml.Unmarshal(c.Request.Body(), &vc); err != nil {
		writeXMLError(c, consts.StatusBadRequest, "MalformedXML", "invalid versioning configuration XML")
		return
	}
	if vc.Status != "Enabled" && vc.Status != "Suspended" && vc.Status != "Unversioned" {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "versioning status must be Enabled or Suspended")
		return
	}

	if err := v.SetBucketVersioning(bucket, vc.Status); err != nil {
		mapError(c, err)
		return
	}
	c.Status(consts.StatusOK)
}

// getBucketVersioning handles GET /<bucket>?versioning.
func (s *Server) getBucketVersioning(_ context.Context, c *app.RequestContext, bucket string) {
	v, ok := findBucketVersioner(s.backend)
	if !ok {
		writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", "versioning not supported by this backend")
		return
	}

	state, err := v.GetBucketVersioning(bucket)
	if err != nil {
		mapError(c, err)
		return
	}

	c.Header("Content-Type", "application/xml")
	c.Status(consts.StatusOK)
	enc := xml.NewEncoder(c.Response.BodyWriter())
	enc.Encode(versioningConfiguration{ //nolint:errcheck
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		Status: state,
	})
}
