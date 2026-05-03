package server

import (
	"errors"
	"sync/atomic"
)

var ErrMutationDisabled = errors.New("mutations are disabled by startup recovery mode")

type MutationGate struct {
	err atomic.Value
}

type MutationGateResponse struct {
	Status    int    `json:"-"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Operation string `json:"operation"`
}

func NewMutationGate(err error) *MutationGate {
	g := &MutationGate{}
	if err != nil {
		g.err.Store(err)
	}
	return g
}

func (g *MutationGate) Check(operation string) error {
	if g == nil {
		return nil
	}
	v := g.err.Load()
	if v == nil {
		return nil
	}
	if err, ok := v.(error); ok && err != nil {
		return err
	}
	return ErrMutationDisabled
}

func (g *MutationGate) SetBlocked(err error) {
	if err == nil {
		err = ErrMutationDisabled
	}
	g.err.Store(err)
}

func (g *MutationGate) BlockResponse(operation string) (MutationGateResponse, bool) {
	err := g.Check(operation)
	if err == nil {
		return MutationGateResponse{}, false
	}
	return MutationGateResponse{
		Status:    503,
		Code:      "RecoveryReadOnly",
		Message:   err.Error(),
		Operation: operation,
	}, true
}
