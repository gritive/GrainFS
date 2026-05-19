package server

import "encoding/xml"

type listBucketsResult struct {
	XMLName xml.Name       `xml:"ListAllMyBucketsResult"`
	Xmlns   string         `xml:"xmlns,attr"`
	Buckets []bucketResult `xml:"Buckets>Bucket"`
}

type bucketResult struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

// listObjectsResultV1 mirrors S3 ListObjects (V1) response shape. Marker is
// always emitted — S3 returns `<Marker/>` even when the request omitted it,
// and some clients (mc, certain SDKs) parse for its presence rather than
// content. NextMarker only appears when IsTruncated is true.
type listObjectsResultV1 struct {
	XMLName     xml.Name       `xml:"ListBucketResult"`
	Xmlns       string         `xml:"xmlns,attr"`
	Name        string         `xml:"Name"`
	Prefix      string         `xml:"Prefix"`
	Marker      string         `xml:"Marker"`
	NextMarker  string         `xml:"NextMarker,omitempty"`
	MaxKeys     int            `xml:"MaxKeys"`
	IsTruncated bool           `xml:"IsTruncated"`
	Contents    []objectResult `xml:"Contents"`
}

// listObjectsResultV2 mirrors S3 ListObjectsV2 (?list-type=2). KeyCount is
// emitted unconditionally per S3 spec — minio-go's V2 parser reads it even
// on empty pages. ContinuationToken / StartAfter are optional and echo the
// request; NextContinuationToken only appears when IsTruncated is true.
type listObjectsResultV2 struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Xmlns                 string         `xml:"xmlns,attr"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	KeyCount              int            `xml:"KeyCount"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	IsTruncated           bool           `xml:"IsTruncated"`
	Contents              []objectResult `xml:"Contents"`
}

type objectResult struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

type bucketLocationResult struct {
	XMLName            xml.Name `xml:"LocationConstraint"`
	Xmlns              string   `xml:"xmlns,attr"`
	LocationConstraint string   `xml:",chardata"`
}
