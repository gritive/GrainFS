package server

import "encoding/xml"

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

type completeMultipartUploadRequest struct {
	XMLName xml.Name      `xml:"CompleteMultipartUpload"`
	Parts   []xmlPartInfo `xml:"Part"`
}

type xmlPartInfo struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeMultipartUploadResult struct {
	XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Bucket  string   `xml:"Bucket"`
	Key     string   `xml:"Key"`
	ETag    string   `xml:"ETag"`
}

type listMultipartUploadsResult struct {
	XMLName     xml.Name             `xml:"ListMultipartUploadsResult"`
	Xmlns       string               `xml:"xmlns,attr"`
	Bucket      string               `xml:"Bucket"`
	Prefix      string               `xml:"Prefix,omitempty"`
	MaxUploads  int                  `xml:"MaxUploads"`
	IsTruncated bool                 `xml:"IsTruncated"`
	Uploads     []multipartUploadXML `xml:"Upload"`
}

type multipartUploadXML struct {
	Key       string `xml:"Key"`
	UploadId  string `xml:"UploadId"`
	Initiated string `xml:"Initiated"`
}

type listPartsResult struct {
	XMLName     xml.Name      `xml:"ListPartsResult"`
	Xmlns       string        `xml:"xmlns,attr"`
	Bucket      string        `xml:"Bucket"`
	Key         string        `xml:"Key"`
	UploadId    string        `xml:"UploadId"`
	MaxParts    int           `xml:"MaxParts"`
	IsTruncated bool          `xml:"IsTruncated"`
	Parts       []partInfoXML `xml:"Part"`
}

type partInfoXML struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
	Size       int64  `xml:"Size"`
}
