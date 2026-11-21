package model

import "fmt"

type Error struct {
	Code      string             `json:"code"`
	Detail    string             `json:"detail"`
	Conflicts []ConflictEvidence `json:"conflicts,omitempty"`
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

// CorrectionConflict reports that at least one observation in a correction no
// longer matches the version its author observed. The whole request is
// rejected without publishing; evidence carries the current states so the
// caller can re-observe and decide again.
func CorrectionConflict(detail string, evidence []ConflictEvidence) error {
	return &Error{Code: "conflict", Detail: detail, Conflicts: evidence}
}

func Capacity(s string) error {
	return &Error{Code: "capacity", Detail: s}
}
