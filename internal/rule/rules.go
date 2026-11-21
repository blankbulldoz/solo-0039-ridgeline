// Package rule owns recording-rule definition semantics: what a valid rule is,
// how revisions form a dependency graph and how derived cells are combined.
// Storage and transaction ownership stay in the engine.
package rule

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"

	"ridgeline/internal/model"
	selectseries "ridgeline/internal/select"
	"ridgeline/internal/window"
)

const (
	KindSource  = "source"
	KindDerived = "derived"

	MaxInputs = 8
	MaxDepth  = 32
	MaxRules  = 256
)

var combines = map[string]bool{
	"sum": true, "min": true, "max": true, "avg": true, "count": true,
}

// ValidateInput checks one rule definition in isolation. References to other
// rules are resolved by the caller, which owns the stored graph.
func ValidateInput(in model.RuleInput) error {
	switch in.Kind {
	case KindSource:
		return validateSource(in)
	case KindDerived:
		return validateDerived(in)
	default:
		return model.Bad("rule kind must be source or derived")
	}
}

func validateSource(in model.RuleInput) error {
	if in.Source == nil {
		return model.Bad("source rule requires a source definition")
	}
	if len(in.Inputs) > 0 || in.Combine != "" || len(in.GroupBy) > 0 {
		return model.Bad("source rule cannot declare inputs, combine or group_by")
	}
	if err := window.Validate(in.Source.Query(0, 1)); err != nil {
		return err
	}
	if _, err := selectseries.Compile(in.Source.Matchers); err != nil {
		return err
	}
	return nil
}

func validateDerived(in model.RuleInput) error {
	if in.Source != nil {
		return model.Bad("derived rule cannot declare a source definition")
	}
	if len(in.Inputs) == 0 || len(in.Inputs) > MaxInputs {
		return model.Bad("derived rule requires 1..8 inputs")
	}
	seen := map[string]bool{}
	for _, name := range in.Inputs {
		if !model.ValidName(name) {
			return model.Bad("invalid input rule name")
		}
		if seen[name] {
			return model.Bad("derived rule repeats an input rule")
		}
		seen[name] = true
	}
	if !combines[in.Combine] {
		return model.Bad("unknown combine operation")
	}
	for _, label := range in.GroupBy {
		if !model.ValidName(label) {
			return model.Bad("invalid grouping label")
		}
	}
	return nil
}

// GeometryOf returns the window geometry a stored rule reduces on. Source
// rules declare it; derived rules inherit it from their inputs.
func GeometryOf(name string, graph map[string]model.Rule) (model.RuleGeometry, error) {
	return geometry(name, graph, map[string]bool{})
}

func geometry(name string, graph map[string]model.Rule, visiting map[string]bool) (model.RuleGeometry, error) {
	if visiting[name] {
		return model.RuleGeometry{}, model.Conflict("rule dependency cycle at " + name)
	}
	r, ok := graph[name]
	if !ok {
		return model.RuleGeometry{}, model.Missing("rule " + name + " does not exist")
	}
	if r.Kind == KindSource && r.Source != nil {
		return model.RuleGeometry{Step: r.Source.Step, Origin: r.Source.Origin}, nil
	}
	if len(r.Inputs) == 0 {
		return model.RuleGeometry{}, model.Conflict("rule " + name + " has no inputs")
	}
	visiting[name] = true
	defer delete(visiting, name)
	var first model.RuleGeometry
	for i, input := range r.Inputs {
		g, err := geometry(input, graph, visiting)
		if err != nil {
			return model.RuleGeometry{}, err
		}
		if i == 0 {
			first = g
		} else if g != first {
			return model.RuleGeometry{}, model.Conflict(
				fmt.Sprintf("rule %q inputs have different window geometry", name))
		}
	}
	return first, nil
}

// Graph builds the dependency edges of the current rule revisions.
func Graph(rules map[string]model.Rule) map[string][]string {
	graph := make(map[string][]string, len(rules))
	for name, r := range rules {
		graph[name] = append([]string(nil), r.Inputs...)
	}
	return graph
}

// Check walks the graph from start. It returns a cycle path when the
// dependencies are not acyclic, and otherwise the longest chain length.
func Check(graph map[string][]string, start string) ([]string, int) {
	state := map[string]int{} // 1 visiting, 2 done
	depth := map[string]int{} // longest chain below and including the node
	path := make([]string, 0)
	var cycle []string
	var walk func(string) int
	walk = func(name string) int {
		if state[name] == 1 {
			for i, p := range path {
				if p == name {
					cycle = append(append([]string(nil), path[i:]...), name)
					break
				}
			}
			return 0
		}
		if state[name] == 2 {
			return depth[name]
		}
		state[name] = 1
		path = append(path, name)
		longest := 1
		for _, next := range graph[name] {
			if d := walk(next) + 1; d > longest {
				longest = d
			}
		}
		path = path[:len(path)-1]
		state[name] = 2
		depth[name] = longest
		return longest
	}
	return cycle, walk(start)
}

// Combine reduces the already-computed cells of the input rules into this
// derived rule's cells. Cells match on window start and on the labels kept by
// the derived rule's group_by (all input labels when group_by is empty).
func Combine(def model.Rule, geom model.RuleGeometry, inputs []model.RuleNode) ([]model.Cell, error) {
	type accumulator struct {
		cell  model.Cell
		sum   float64
		count int
		min   float64
		max   float64
		has   bool
	}
	acc := map[string]*accumulator{}
	for _, input := range inputs {
		if input.Geometry != geom {
			return nil, model.Conflict(
				fmt.Sprintf("rule %q input %q has different window geometry", def.Name, input.Name))
		}
		for _, cell := range input.Cells {
			labels := cell.Labels
			if len(def.GroupBy) > 0 {
				_, labels = window.Group(cell.Labels, def.GroupBy)
			}
			key := cellKey(labels, cell.Start)
			a := acc[key]
			if a == nil {
				a = &accumulator{cell: model.Cell{
					Labels: labels, Start: cell.Start, End: cell.Start + geom.Step,
				}}
				acc[key] = a
			}
			a.count++
			a.cell.Count += cell.Count
			a.sum += cell.Value
			if !a.has || cell.Value < a.min {
				a.min = cell.Value
			}
			if !a.has || cell.Value > a.max {
				a.max = cell.Value
			}
			a.has = true
		}
	}
	out := make([]model.Cell, 0, len(acc))
	for _, a := range acc {
		switch def.Combine {
		case "sum":
			a.cell.Value = a.sum
		case "min":
			a.cell.Value = a.min
		case "max":
			a.cell.Value = a.max
		case "avg":
			a.cell.Value = a.sum / float64(a.count)
		case "count":
			a.cell.Value = float64(a.count)
		}
		if math.IsInf(a.cell.Value, 0) || math.IsNaN(a.cell.Value) {
			return nil, model.Capacity("derived rule combination overflow")
		}
		out = append(out, a.cell)
	}
	window.SortCells(out)
	return out, nil
}

func cellKey(labels model.Labels, start int64) string {
	data, _ := json.Marshal(labels)
	return string(data) + "/" + strconv.FormatInt(start, 10)
}

// SortedNames returns the current rule names in a stable order.
func SortedNames(rules map[string]model.Rule) []string {
	names := make([]string, 0, len(rules))
	for name := range rules {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
