package server

import (
	"context"
	"encoding/xml"
	"fmt"
	"time"

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

// ObjectVersionLister is implemented by backends that support listing object versions.
type ObjectVersionLister interface {
	ListObjectVersions(bucket, prefix string, maxKeys int) ([]*storage.ObjectVersion, error)
}

func findObjectVersionLister(b storage.Backend) (ObjectVersionLister, bool) {
	for b != nil {
		if v, ok := b.(ObjectVersionLister); ok {
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

// listVersionsResult is the S3 XML response for GET /<bucket>?versions.
type listVersionsResult struct {
	XMLName       xml.Name              `xml:"ListVersionsResult"`
	Xmlns         string                `xml:"xmlns,attr,omitempty"`
	Name          string                `xml:"Name"`
	Prefix        string                `xml:"Prefix"`
	MaxKeys       int                   `xml:"MaxKeys"`
	IsTruncated   bool                  `xml:"IsTruncated"`
	Versions      []versionEntry        `xml:"Version"`
	DeleteMarkers []deleteMarkerEntry   `xml:"DeleteMarker"`
}

type versionEntry struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

type deleteMarkerEntry struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
}

// listObjectVersions handles GET /<bucket>?versions.
func (s *Server) listObjectVersions(_ context.Context, c *app.RequestContext, bucket string) {
	lister, ok := findObjectVersionLister(s.backend)
	if !ok {
		writeXMLError(c, consts.StatusNotImplemented, "NotImplemented", "versioning not supported by this backend")
		return
	}

	prefix := string(c.QueryArgs().Peek("prefix"))
	maxKeys := 1000

	vs, err := lister.ListObjectVersions(bucket, prefix, maxKeys)
	if err != nil {
		mapError(c, err)
		return
	}

	result := listVersionsResult{
		Xmlns:   "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:    bucket,
		Prefix:  prefix,
		MaxKeys: maxKeys,
	}
	for _, v := range vs {
		ts := time.Unix(v.LastModified, 0).UTC().Format(time.RFC3339)
		if v.IsDeleteMarker {
			result.DeleteMarkers = append(result.DeleteMarkers, deleteMarkerEntry{
				Key:          v.Key,
				VersionID:    v.VersionID,
				IsLatest:     v.IsLatest,
				LastModified: ts,
			})
		} else {
			result.Versions = append(result.Versions, versionEntry{
				Key:          v.Key,
				VersionID:    v.VersionID,
				IsLatest:     v.IsLatest,
				LastModified: ts,
				ETag:         fmt.Sprintf("\"%s\"", v.ETag),
				Size:         v.Size,
			})
		}
	}

	data, _ := xml.Marshal(result)
	c.Data(consts.StatusOK, "application/xml", data)
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
