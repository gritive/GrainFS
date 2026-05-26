package execution

import "github.com/google/uuid"

type OperationKind string

const OperationScrub OperationKind = "scrub"

type Operation struct {
	ID    string
	Kind  OperationKind
	Scrub ScrubOperation
}

type ScrubOperation struct {
	Bucket    string
	KeyPrefix string
	DryRun    bool
}

func NewRequestID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func (o Operation) Validate() error {
	if o.Kind != OperationScrub {
		return NewError(CodeUnsupported, ErrExecutionUnsupported)
	}
	return o.Scrub.Validate()
}

func (o ScrubOperation) Validate() error {
	if o.Bucket == "" {
		return NewError(CodeInvalid, ErrInvalidOperation)
	}
	return nil
}
