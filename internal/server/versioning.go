package server

import (
	"context"
	"encoding/xml"
	"fmt"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// versioningConfiguration is the S3 XML body for GET/PUT ?versioning.
type versioningConfiguration struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Status  string   `xml:"Status"`
}

// putBucketVersioning handles PUT /<bucket>?versioning.
func (s *Server) putBucketVersioning(c *app.RequestContext, bucket string) {
	var vc versioningConfiguration
	if err := xml.Unmarshal(c.Request.Body(), &vc); err != nil {
		writeXMLError(c, consts.StatusBadRequest, "MalformedXML", "invalid versioning configuration XML")
		return
	}
	if vc.Status != "Enabled" && vc.Status != "Suspended" {
		writeXMLError(c, consts.StatusBadRequest, "InvalidArgument", "versioning status must be Enabled or Suspended")
		return
	}

	if err := s.ops.SetBucketVersioning(bucket, vc.Status); err != nil {
		mapError(c, err)
		return
	}
	c.Status(consts.StatusOK)
}

// listVersionsResult is the S3 XML response for GET /<bucket>?versions.
type listVersionsResult struct {
	XMLName       xml.Name            `xml:"ListVersionsResult"`
	Xmlns         string              `xml:"xmlns,attr,omitempty"`
	Name          string              `xml:"Name"`
	Prefix        string              `xml:"Prefix"`
	MaxKeys       int                 `xml:"MaxKeys"`
	IsTruncated   bool                `xml:"IsTruncated"`
	Versions      []versionEntry      `xml:"Version"`
	DeleteMarkers []deleteMarkerEntry `xml:"DeleteMarker"`
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
	prefix := string(c.QueryArgs().Peek("prefix"))
	maxKeys := 1000

	vs, err := s.ops.ListObjectVersions(bucket, prefix, maxKeys)
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

	// TODO: S3 spec requires Owner and StorageClass in Versions/DeleteMarkers entries.
	// Populating Owner needs proper IAM/ACL integration; StorageClass is not yet modeled.
	data, _ := xml.Marshal(result)
	out := append([]byte(xml.Header), data...)
	c.Data(consts.StatusOK, "application/xml", out)
}

// getBucketVersioning handles GET /<bucket>?versioning.
func (s *Server) getBucketVersioning(_ context.Context, c *app.RequestContext, bucket string) {
	state, err := s.ops.GetBucketVersioning(bucket)
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
