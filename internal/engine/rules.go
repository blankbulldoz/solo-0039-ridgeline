package engine

import (
	"encoding/json"
	"fmt"

	"ridgeline/internal/model"
	"ridgeline/internal/rule"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

// Rule buckets are created lazily so databases written before rules existed
// remain readable.
func rulesBucket(b *bolt.Bucket) (*bolt.Bucket, error) {
	return b.CreateBucketIfNotExists(volume.Rules)
}

func revisionsBucket(b *bolt.Bucket) (*bolt.Bucket, error) {
	return b.CreateBucketIfNotExists(volume.RuleRevisions)
}

func runsBucket(b *bolt.Bucket) (*bolt.Bucket, error) {
	return b.CreateBucketIfNotExists(volume.RuleRuns)
}

func revisionKey(name string, revision int) []byte {
	return []byte(fmt.Sprintf("%s\x00%06d", name, revision))
}

func revisionPrefix(name string) []byte {
	return []byte(name + "\x00")
}

func readCurrentRules(b *bolt.Bucket) (map[string]model.Rule, error) {
	out := map[string]model.Rule{}
	bucket := b.Bucket(volume.Rules)
	if bucket == nil {
		return out, nil
	}
	err := bucket.ForEach(func(_, value []byte) error {
		var r model.Rule
		if err := json.Unmarshal(value, &r); err != nil {
			return err
		}
		out[r.Name] = r
		return nil
	})
	return out, err
}

func readRevisions(b *bolt.Bucket, name string) ([]model.Rule, error) {
	out := make([]model.Rule, 0)
	bucket := b.Bucket(volume.RuleRevisions)
	if bucket == nil {
		return out, nil
	}
	prefix := revisionPrefix(name)
	cursor := bucket.Cursor()
	for k, v := cursor.Seek(prefix); k != nil && hasPrefix(k, prefix); k, v = cursor.Next() {
		var r model.Rule
		if err := json.Unmarshal(v, &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

func hasPrefix(key, prefix []byte) bool {
	if len(key) < len(prefix) {
		return false
	}
	for i := range prefix {
		if key[i] != prefix[i] {
			return false
		}
	}
	return true
}

func dependentsOf(rules map[string]model.Rule, name string) []string {
	out := make([]string, 0)
	for _, candidate := range rule.SortedNames(rules) {
		if candidate == name {
			continue
		}
		for _, input := range rules[candidate].Inputs {
			if input == name {
				out = append(out, candidate)
				break
			}
		}
	}
	return out
}

func ruleView(rules map[string]model.Rule, name string) (model.RuleView, error) {
	r, ok := rules[name]
	if !ok {
		return model.RuleView{}, model.Missing("rule does not exist")
	}
	geom, err := rule.GeometryOf(name, rules)
	if err != nil {
		return model.RuleView{}, err
	}
	return model.RuleView{Rule: r, Geometry: geom, Dependents: dependentsOf(rules, name)}, nil
}

// Rules lists the current revision of every rule in a namespace.
func (e *Engine) Rules(space string) ([]model.RuleView, error) {
	out := make([]model.RuleView, 0)
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, _, err := loadNamespace(tx, space)
		if err != nil {
			return err
		}
		rules, err := readCurrentRules(b)
		if err != nil {
			return err
		}
		for _, name := range rule.SortedNames(rules) {
			view, err := ruleView(rules, name)
			if err != nil {
				return err
			}
			out = append(out, view)
		}
		return nil
	})
	return out, err
}

// Rule returns the current revision of one rule plus its direct dependents.
func (e *Engine) Rule(space, name string) (model.RuleView, error) {
	var out model.RuleView
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, _, err := loadNamespace(tx, space)
		if err != nil {
			return err
		}
		rules, err := readCurrentRules(b)
		if err != nil {
			return err
		}
		out, err = ruleView(rules, name)
		return err
	})
	return out, err
}

// PutRule appends a new immutable revision of a rule definition. Existing
// revisions are never rewritten, so runs recorded earlier keep the exact
// definition they executed. A definition that would introduce a cycle, a
// missing reference, a mismatched window geometry or a chain deeper than
// rule.MaxDepth is rejected before anything is stored.
func (e *Engine) PutRule(space, name string, in model.RuleInput) (model.RuleView, error) {
	var out model.RuleView
	if !model.ValidName(name) {
		return out, model.Bad("invalid rule name")
	}
	if err := rule.ValidateInput(in); err != nil {
		return out, err
	}
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, space)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		rules, err := readCurrentRules(b)
		if err != nil {
			return err
		}
		if _, exists := rules[name]; !exists && len(rules) >= rule.MaxRules {
			return model.Capacity("rule count exceeds namespace limit")
		}
		for _, input := range in.Inputs {
			if _, ok := rules[input]; !ok {
				return model.Missing(fmt.Sprintf("rule %q references missing rule %q", name, input))
			}
		}
		graph := rule.Graph(rules)
		graph[name] = append([]string(nil), in.Inputs...)
		cycle, depth := rule.Check(graph, name)
		if cycle != nil {
			return model.Conflict("rule dependency cycle: " + joinCycle(cycle))
		}
		if depth > rule.MaxDepth {
			return model.Conflict(fmt.Sprintf("rule dependency chain exceeds %d", rule.MaxDepth))
		}
		candidate := model.Rule{
			Name: name, Kind: in.Kind, Source: in.Source,
			Inputs: append([]string(nil), in.Inputs...), Combine: in.Combine,
			GroupBy: append([]string(nil), in.GroupBy...),
		}
		if current, ok := rules[name]; ok {
			candidate.Revision = current.Revision + 1
		} else if history, err := readRevisions(b, name); err != nil {
			return err
		} else if len(history) > 0 {
			// A deleted name may be reintroduced, but revisions keep counting
			// so an old run can never be confused with a new definition.
			candidate.Revision = history[len(history)-1].Revision + 1
		} else {
			candidate.Revision = 1
		}
		probe := make(map[string]model.Rule, len(rules)+1)
		for key, value := range rules {
			probe[key] = value
		}
		probe[name] = candidate
		if _, err := rule.GeometryOf(name, probe); err != nil {
			return err
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		candidate.Sequence = seq
		revisions, err := revisionsBucket(b)
		if err != nil {
			return err
		}
		if err := volume.PutJSON(revisions, revisionKey(name, candidate.Revision), candidate); err != nil {
			return err
		}
		currentBucket, err := rulesBucket(b)
		if err != nil {
			return err
		}
		if err := volume.PutJSON(currentBucket, []byte(name), candidate); err != nil {
			return err
		}
		// A definition change is metadata, not measurement data: it must not
		// advance the source sequence that stale rollups compare against.
		out, err = ruleView(probe, name)
		return err
	})
	return out, err
}

func joinCycle(cycle []string) string {
	out := ""
	for i, name := range cycle {
		if i > 0 {
			out += " -> "
		}
		out += name
	}
	return out
}

// DeleteRule retires a rule. Deletion is refused while other current rules
// still reference it so the stored graph never contains a dangling edge;
// earlier revisions and runs remain readable for provenance.
func (e *Engine) DeleteRule(space, name string) (model.RuleDeletion, error) {
	var receipt model.RuleDeletion
	if !model.ValidName(name) {
		return receipt, model.Bad("invalid rule name")
	}
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, space)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		rules, err := readCurrentRules(b)
		if err != nil {
			return err
		}
		current, ok := rules[name]
		if !ok {
			return model.Missing("rule does not exist")
		}
		dependents := dependentsOf(rules, name)
		if len(dependents) > 0 {
			return model.Conflict(fmt.Sprintf(
				"rule %q is still referenced by %v", name, dependents))
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		receipt = model.RuleDeletion{
			Name: name, Revision: current.Revision, Sequence: seq, Deleted: true,
			Dependents: []string{},
		}
		deletes, err := b.CreateBucketIfNotExists(volume.RuleDeletes)
		if err != nil {
			return err
		}
		if err := volume.PutJSON(deletes, []byte(name), receipt); err != nil {
			return err
		}
		stored, err := rulesBucket(b)
		if err != nil {
			return err
		}
		return stored.Delete([]byte(name))
	})
	return receipt, err
}

// RuleRevisions returns every stored revision of a rule, oldest first. It
// keeps revisions that are no longer current so a recorded run's definition
// remains explainable after the rule changes or is deleted.
func (e *Engine) RuleRevisions(space, name string) (model.RuleHistory, error) {
	var out model.RuleHistory
	if !model.ValidName(name) {
		return out, model.Bad("invalid rule name")
	}
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, _, err := loadNamespace(tx, space)
		if err != nil {
			return err
		}
		revisions, err := readRevisions(b, name)
		if err != nil {
			return err
		}
		if len(revisions) == 0 {
			return model.Missing("rule does not exist")
		}
		out.Name, out.Revisions = name, revisions
		rules, err := readCurrentRules(b)
		if err != nil {
			return err
		}
		if current, ok := rules[name]; ok {
			out.Current = current.Revision
		} else {
			out.Deleted = true
			if deletes := b.Bucket(volume.RuleDeletes); deletes != nil {
				var deletion model.RuleDeletion
				if data := deletes.Get([]byte(name)); data != nil &&
					json.Unmarshal(data, &deletion) == nil {
					out.DeletedAt = deletion.Sequence
				}
			}
		}
		return nil
	})
	return out, err
}
