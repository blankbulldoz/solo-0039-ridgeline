package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"ridgeline/internal/model"
	"ridgeline/internal/rule"
	"ridgeline/internal/volume"
	"ridgeline/internal/window"

	bolt "go.etcd.io/bbolt"
)

// planRun resolves the dependency closure of root against the stored graph
// before any data is read. Cycles, missing references and oversized chains
// fail here rather than halfway through execution.
func planRun(root string, rules map[string]model.Rule) ([]string, map[string]model.RuleGeometry, error) {
	if _, ok := rules[root]; !ok {
		return nil, nil, model.Missing("rule " + root + " does not exist")
	}
	order := make([]string, 0)
	geometry := map[string]model.RuleGeometry{}
	state := map[string]int{}
	var walk func(string, int) error
	walk = func(name string, depth int) error {
		if depth > rule.MaxDepth {
			return model.Conflict(fmt.Sprintf("rule dependency chain exceeds %d", rule.MaxDepth))
		}
		switch state[name] {
		case 1:
			return model.Conflict("rule dependency cycle at " + name)
		case 2:
			return nil
		}
		r, ok := rules[name]
		if !ok {
			return model.Missing("rule " + name + " does not exist")
		}
		state[name] = 1
		for _, input := range r.Inputs {
			if err := walk(input, depth+1); err != nil {
				return err
			}
		}
		state[name] = 2
		geom, err := rule.GeometryOf(name, rules)
		if err != nil {
			return err
		}
		geometry[name] = geom
		order = append(order, name)
		return nil
	}
	if err := walk(root, 1); err != nil {
		return nil, nil, err
	}
	return order, geometry, nil
}

// Execute runs the dependency closure of one root rule against a single data
// view. The caller owns the transaction; every rule in the closure sees the
// same sequence and the same raw samples. A source rule is evaluated at most
// once because nodes are memoized by name, so a shared sub-rule costs one
// computation regardless of how many derived rules reference it. The remaining
// budget is passed to each source scan, and exceeding it fails the whole run.
func Execute(b *bolt.Bucket, n model.Namespace, root string, request model.RuleRunRequest, sequence uint64) (model.RuleRun, error) {
	rules, err := readCurrentRules(b)
	if err != nil {
		return model.RuleRun{}, err
	}
	order, geometry, err := planRun(root, rules)
	if err != nil {
		return model.RuleRun{}, err
	}
	budget := request.MaxPoints
	if budget == 0 || budget > n.MaxScan {
		budget = n.MaxScan
	}
	decoded := 0
	nodesByName := make(map[string]model.RuleNode, len(order))
	for _, name := range order {
		def := rules[name]
		node := model.RuleNode{Rule: def, Geometry: geometry[name], Cells: []model.Cell{}}
		if def.Kind == rule.KindSource {
			q := def.Source.Query(request.Start, request.End)
			// The remaining budget, not the namespace default, binds this
			// source scan so the closure as a whole stays within one budget.
			priced := n
			if remaining := budget - decoded; remaining < n.MaxScan {
				priced.MaxScan = remaining
			}
			raw, err := scan(b, priced, q, sequence)
			if err != nil {
				return model.RuleRun{}, err
			}
			cells, err := window.Aggregate(raw.Series, q)
			if err != nil {
				return model.RuleRun{}, err
			}
			node.Cells = cells
			node.Stats = raw.Stats
			node.Decoded = raw.Stats.Decoded
			decoded += raw.Stats.Decoded
		} else {
			inputs := make([]model.RuleNode, 0, len(def.Inputs))
			for _, input := range def.Inputs {
				inputs = append(inputs, nodesByName[input])
			}
			cells, err := rule.Combine(def, geometry[name], inputs)
			if err != nil {
				return model.RuleRun{}, err
			}
			node.Cells = cells
		}
		nodesByName[name] = node
	}
	nodes := make([]model.RuleNode, 0, len(order))
	for _, name := range order {
		node := nodesByName[name]
		for _, input := range node.Inputs {
			nodesByName[input] = incrementReferences(nodesByName[input])
		}
		nodesByName[name] = node
	}
	for _, name := range order {
		node := nodesByName[name]
		node.Shared = node.References > 1
		nodes = append(nodes, node)
	}
	return model.RuleRun{
		Root: root, Revision: rules[root].Revision,
		Start: request.Start, End: request.End,
		ViewSequence: sequence, DataSequence: n.LastSequence, Budget: budget,
		Decoded: decoded, Order: append([]string(nil), order...),
		Result: nodesByName[root].Cells, Nodes: nodes,
	}, nil
}

func incrementReferences(node model.RuleNode) model.RuleNode {
	node.References++
	return node
}

// RunRules executes a root rule and, when requested, records the run in the
// same transaction. Because execution and recording share one transaction, the
// recorded dependency chain is exactly the one that produced the returned
// cells: data, definitions and provenance cannot interleave with a concurrent
// write.
func (e *Engine) RunRules(space, root string, request model.RuleRunRequest) (model.RuleRunView, error) {
	var view model.RuleRunView
	if err := model.ValidateQuery(model.Query{Start: request.Start, End: request.End}); err != nil {
		return view, err
	}
	if request.MaxPoints < 0 || request.MaxPoints > 2000000 {
		return view, model.Bad("invalid run point budget")
	}
	action := func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, space)
		if err != nil {
			return err
		}
		if request.Record && n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		run, err := Execute(b, n, root, request, volume.Current(tx))
		if err != nil {
			return err
		}
		if request.Record {
			if tx.Writable() {
				run, err = recordRun(tx, b, space, run)
				if err != nil {
					return err
				}
			} else {
				return model.Conflict("recording requires a writable transaction")
			}
		}
		rules, err := readCurrentRules(b)
		if err != nil {
			return err
		}
		view = model.RuleRunView{
			RuleRun:         run,
			Definitions:     definitionsFor(b, run, rules),
			Revisions:       revisionStates(run, rules),
			DataStale:       false,
			DefinitionStale: false,
		}
		return nil
	}
	var err error
	if request.Record {
		err = e.DB.Raw.Update(action)
	} else {
		err = e.DB.Raw.View(action)
	}
	return view, err
}

// recordRun stores a completed run. Recording advances the database sequence
// (so provenance has an ordering) but never changes the namespace's
// LastSequence, which is the data watermark used by stale checks.
func recordRun(tx *bolt.Tx, b *bolt.Bucket, space string, run model.RuleRun) (model.RuleRun, error) {
	seq, err := volume.Next(tx)
	if err != nil {
		return run, err
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%d/%d/%d/%d",
		space, run.Root, run.Revision, run.Start, run.End, seq)))
	run.ID = hex.EncodeToString(digest[:16])
	run.RecordSequence = seq
	bucket, err := runsBucket(b)
	if err != nil {
		return run, err
	}
	return run, volume.PutJSON(bucket, []byte(run.ID), run)
}

func revisionStates(run model.RuleRun, rules map[string]model.Rule) []model.RevisionState {
	out := make([]model.RevisionState, 0, len(run.Nodes))
	for _, node := range run.Nodes {
		state := model.RevisionState{Name: node.Name, Executed: node.Revision, Current: node.Revision}
		if current, ok := rules[node.Name]; ok {
			state.Current = current.Revision
		}
		state.Changed = state.Current != state.Executed
		out = append(out, state)
	}
	return out
}

// definitionsFor resolves each executed revision against stored history. The
// frozen copy embedded in the node is the authoritative definition; the
// history lookup proves that copy is still consistent with what was stored.
func definitionsFor(b *bolt.Bucket, run model.RuleRun, rules map[string]model.Rule) []model.RuleRevisionView {
	out := make([]model.RuleRevisionView, 0, len(run.Nodes))
	for _, node := range run.Nodes {
		current, present := rules[node.Name]
		definition := model.RuleRevisionView{
			Name: node.Name, Revision: node.Revision, Rule: node.Rule,
			Current: present && current.Revision == node.Revision,
			Deleted: !present,
		}
		if revisions, err := readRevisions(b, node.Name); err == nil {
			for _, stored := range revisions {
				if stored.Revision == node.Revision {
					definition.Rule = stored
					break
				}
			}
		}
		out = append(out, definition)
	}
	return out
}

// RuleRun returns a recorded run with an explanation of whether the data or
// any executed definition has changed since it was recorded. Old results stay
// readable and self-describing: the response embeds the definition each node
// executed, not the rule's current revision.
func (e *Engine) RuleRun(space, id string) (model.RuleRunView, error) {
	var view model.RuleRunView
	if len(id) != 32 {
		return view, model.Bad("invalid run identifier")
	}
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, space)
		if err != nil {
			return err
		}
		bucket := b.Bucket(volume.RuleRuns)
		if bucket == nil || bucket.Get([]byte(id)) == nil {
			return model.Missing("rule run does not exist")
		}
		if err := volume.GetJSON(bucket, []byte(id), &view.RuleRun); err != nil {
			return err
		}
		rules, err := readCurrentRules(b)
		if err != nil {
			return err
		}
		view.DataStale = view.DataSequence != n.LastSequence
		view.Revisions = revisionStates(view.RuleRun, rules)
		view.Definitions = definitionsFor(b, view.RuleRun, rules)
		for _, state := range view.Revisions {
			if state.Changed {
				view.DefinitionStale = true
			}
		}
		return nil
	})
	return view, err
}

// RuleRuns lists recorded runs newest first.
func (e *Engine) RuleRuns(space string) ([]model.RuleRunSummary, error) {
	out := make([]model.RuleRunSummary, 0)
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, _, err := loadNamespace(tx, space)
		if err != nil {
			return err
		}
		bucket := b.Bucket(volume.RuleRuns)
		if bucket == nil {
			return nil
		}
		var runs []model.RuleRun
		if err := bucket.ForEach(func(_, value []byte) error {
			var run model.RuleRun
			if err := json.Unmarshal(value, &run); err != nil {
				return err
			}
			runs = append(runs, run)
			return nil
		}); err != nil {
			return err
		}
		sort.Slice(runs, func(i, j int) bool { return runs[i].RecordSequence > runs[j].RecordSequence })
		for _, run := range runs {
			out = append(out, runSummary(run))
		}
		return nil
	})
	return out, err
}

func runSummary(run model.RuleRun) model.RuleRunSummary {
	return model.RuleRunSummary{
		Run: run.ID, Root: run.Root, Revision: run.Revision,
		Start: run.Start, End: run.End,
		ViewSequence: run.ViewSequence, DataSequence: run.DataSequence,
		RecordSequence: run.RecordSequence, Decoded: run.Decoded,
		Rules: len(run.Nodes),
	}
}

// RuleRunsFor lists recorded runs, newest first, optionally restricted to one
// root rule.
func (e *Engine) RuleRunsFor(space, root string) ([]model.RuleRunSummary, error) {
	out, err := e.RuleRuns(space)
	if err != nil || root == "" {
		return out, err
	}
	filtered := make([]model.RuleRunSummary, 0, len(out))
	for _, summary := range out {
		if summary.Root == root {
			filtered = append(filtered, summary)
		}
	}
	return filtered, nil
}
