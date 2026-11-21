package model

// RuleHistory is the ordered revision list of one rule name, including
// revisions that are no longer current.
type RuleHistory struct {
	Name      string `json:"name"`
	Current   int    `json:"current"`
	Deleted   bool   `json:"deleted"`
	DeletedAt uint64 `json:"deleted_sequence,omitempty"`
	Revisions []Rule `json:"revisions"`
}

// RuleRevisionView explains the definition a stored run executed.
type RuleRevisionView struct {
	Name     string `json:"name"`
	Revision int    `json:"revision"`
	Rule     Rule   `json:"rule"`
	Current  bool   `json:"current"`
	Deleted  bool   `json:"deleted"`
}
