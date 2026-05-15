package admin

import "encoding/json"

func decodeOptionalJSON(body []byte, dst any) error {
	if len(body) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return NewInvalid("invalid JSON body: " + err.Error())
	}
	return nil
}
