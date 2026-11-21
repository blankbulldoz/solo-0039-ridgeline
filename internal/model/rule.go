package model

type SourceSpec struct {
	Matchers []Matcher `json:"matchers"`
	Function string    `json:"function"`
	Step     int64     `json:"step"`
	Origin   int64     `json:"origin"`
	GroupBy  []string  `json:"group_by"`
}

func (s SourceSpec) Query(start, end int64) Query {
	return Query{
		Start: start, End: end, Matchers: s.Matchers,
		Step: s.Step, Origin: s.Origin, GroupBy: s.GroupBy, Function: s.Function,
	}
}

// Rule is one immutable revision of a named aggregate or derived metric.
// Source rules select raw measurements; derived rules combine the cells of
// other rules. Revisions are never rewritten, so every stored run can name the
// exact definition it executed.
type Rule struct {
	Name     string      `json:"name"`
	Revision int         `json:"revision"`
	Kind     string      `json:"kind"`
	Source   *SourceSpec `json:"source,omitempty"`
	Inputs   []string    `json:"inputs,omitempty"`
	Combine  string      `json:"combine,omitempty"`
	GroupBy  []string    `json:"group_by,omitempty"`
	Sequence uint64      `json:"sequence"`
}

// RuleInput is the caller-supplied definition of a new rule revision.
type RuleInput struct {
	Kind    string      `json:"kind"`
	Source  *SourceSpec `json:"source,omitempty"`
	Inputs  []string    `json:"inputs,omitempty"`
	Combine string      `json:"combine,omitempty"`
	GroupBy []string    `json:"group_by,omitempty"`
}

type RuleGeometry struct {
	Step   int64 `json:"step"`
	Origin int64 `json:"origin"`
}

type RuleView struct {
	Rule
	Geometry   RuleGeometry `json:"geometry"`
	Dependents []string     `json:"dependents"`
}

type RuleDeletion struct {
	Name       string   `json:"name"`
	Revision   int      `json:"revision"`
	Sequence   uint64   `json:"sequence"`
	Dependents []string `json:"dependents"`
	Deleted    bool     `json:"deleted"`
}

type RuleNode struct {
	Rule
	Geometry   RuleGeometry `json:"geometry"`
	Cells      []Cell       `json:"cells"`
	Stats      ScanStats    `json:"stats"`
	Decoded    int          `json:"decoded"`
	References int          `json:"references"`
	Shared     bool         `json:"shared"`
}

type RuleRunRequest struct {
	Start     int64 `json:"start"`
	End       int64 `json:"end"`
	MaxPoints int   `json:"max_points"`
	Record    bool  `json:"record"`
}

// RuleRun records the complete dependency chain of one execution. Nodes are in
// dependency order and carry the revision, the immutable definition and the
// output cells of every rule that participated.
type RuleRun struct {
	ID             string     `json:"run"`
	Root           string     `json:"root"`
	Revision       int        `json:"revision"`
	Start          int64      `json:"start"`
	End            int64      `json:"end"`
	ViewSequence   uint64     `json:"view_sequence"`
	DataSequence   uint64     `json:"data_sequence"`
	RecordSequence uint64     `json:"record_sequence"`
	Budget         int        `json:"budget"`
	Decoded        int        `json:"decoded"`
	Order          []string   `json:"order"`
	Result         []Cell     `json:"result"`
	Nodes          []RuleNode `json:"nodes"`
}

type RevisionState struct {
	Name     string `json:"name"`
	Executed int    `json:"executed"`
	Current  int    `json:"current"`
	Changed  bool   `json:"changed"`
}

type RuleRunView struct {
	RuleRun
	DataStale       bool               `json:"data_stale"`
	DefinitionStale bool               `json:"definition_stale"`
	Revisions       []RevisionState    `json:"revisions"`
	Definitions     []RuleRevisionView `json:"definitions"`
}

type RuleRunSummary struct {
	Run            string `json:"run"`
	Root           string `json:"root"`
	Revision       int    `json:"revision"`
	Start          int64  `json:"start"`
	End            int64  `json:"end"`
	ViewSequence   uint64 `json:"view_sequence"`
	DataSequence   uint64 `json:"data_sequence"`
	RecordSequence uint64 `json:"record_sequence"`
	Decoded        int    `json:"decoded"`
	Rules          int    `json:"rules"`
}
