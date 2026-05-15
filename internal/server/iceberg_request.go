package server

import (
	"encoding/json"
)

type icebergCreateNamespaceRequest struct {
	Namespace  []string          `json:"namespace"`
	Properties map[string]string `json:"properties"`
}

type icebergCreateTableRequest struct {
	Name       string            `json:"name"`
	Schema     json.RawMessage   `json:"schema"`
	Properties map[string]string `json:"properties"`
}

func parseIcebergCreateNamespaceRequest(body []byte) (icebergCreateNamespaceRequest, error) {
	var req icebergCreateNamespaceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return icebergCreateNamespaceRequest{}, err
	}
	return req, nil
}

func parseIcebergCreateTableRequest(body []byte) (icebergCreateTableRequest, error) {
	var req icebergCreateTableRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return icebergCreateTableRequest{}, err
	}
	return req, nil
}
