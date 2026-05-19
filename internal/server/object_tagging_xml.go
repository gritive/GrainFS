package server

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"net/url"

	"github.com/gritive/GrainFS/internal/storage"
)

const taggingNamespace = "http://s3.amazonaws.com/doc/2006-03-01/"

type wireTag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type wireTagging struct {
	XMLName xml.Name `xml:"Tagging"`
	XMLNS   string   `xml:"xmlns,attr"`
	TagSet  struct {
		Tags []wireTag `xml:"Tag"`
	} `xml:"TagSet"`
}

func MarshalTaggingXML(tags []storage.Tag) ([]byte, error) {
	w := wireTagging{XMLNS: taggingNamespace}
	for _, t := range tags {
		w.TagSet.Tags = append(w.TagSet.Tags, wireTag{Key: t.Key, Value: t.Value})
	}
	var buf bytes.Buffer
	buf.WriteString(xml.Header)
	enc := xml.NewEncoder(&buf)
	if err := enc.Encode(w); err != nil {
		return nil, err
	}
	if err := enc.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func ParseTaggingXML(body []byte) ([]storage.Tag, error) {
	var w wireTagging
	if err := xml.Unmarshal(body, &w); err != nil {
		return nil, err
	}
	if w.XMLName.Local != "Tagging" {
		return nil, errors.New("tagging: wrong root element")
	}
	tags := make([]storage.Tag, 0, len(w.TagSet.Tags))
	for _, t := range w.TagSet.Tags {
		tags = append(tags, storage.Tag{Key: t.Key, Value: t.Value})
	}
	return tags, nil
}

func ParseTaggingHeader(h string) ([]storage.Tag, error) {
	if h == "" {
		return nil, nil
	}
	values, err := url.ParseQuery(h)
	if err != nil {
		return nil, err
	}
	tags := make([]storage.Tag, 0, len(values))
	for k, vs := range values {
		if k == "" {
			continue
		}
		if len(vs) > 1 {
			return nil, fmt.Errorf("duplicate tag key in header: %q", k)
		}
		tags = append(tags, storage.Tag{Key: k, Value: vs[0]})
	}
	return tags, nil
}
