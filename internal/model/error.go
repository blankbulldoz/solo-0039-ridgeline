package model

import (
	"errors"
	"fmt"
)

type Error struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Detail)
}

func Bad(s string) error {
	return &Error{Code: "invalid", Detail: s}
}

func Missing(s string) error {
	return &Error{Code: "missing", Detail: s}
}

func Conflict(s string) error {
	return &Error{Code: "conflict", Detail: s}
}

func Capacity(s string) error {
	return &Error{Code: "capacity", Detail: s}
}

// Describe normalizes any failure into the stable code/detail form so a
// failed refresh attempt can be stored and inspected later.
func Describe(err error) *Error {
	var domain *Error
	if errors.As(err, &domain) {
		return domain
	}
	return &Error{Code: "internal", Detail: err.Error()}
}
