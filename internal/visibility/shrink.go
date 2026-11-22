package visibility

import (
	"encoding/json"
	"errors"
)

// Replayer executes a fixed operation sequence from a clean model namespace
// against a fresh engine adapter. It is the contract used by shrink: every
// call must be independent of previous calls.
type Replayer func(ops []Operation) (*Divergence, error)

// DependencyReplayer rebuilds the model and the engine from scratch for each
// candidate. Creating the namespace is part of the harness, not the sequence.
func DependencyReplayer(space string, policy Policy, start, end int64, matchers []Matcher,
	makeAdapter func() (Adapter, func() error, error)) Replayer {
	return func(ops []Operation) (*Divergence, error) {
		model := NewStore()
		if err := model.Create(space, policy); err != nil {
			return nil, err
		}
		adapter, closeFn, err := makeAdapter()
		if err != nil {
			return nil, err
		}
		defer closeFn()
		rec := NewRecorder(adapter, model, space, start, end, matchers)
		for i, op := range ops {
			res, err := rec.Apply(op)
			if err != nil {
				return nil, err
			}
			if d := rec.Compare(i, res); d != nil {
				return d, nil
			}
		}
		return nil, nil
	}
}

// Shrink reduces a failing operation sequence to a 1-minimal subsequence that
// still diverges. It uses delta debugging over contiguous chunks and then a
// single-operation deletion pass. The returned sequence is deterministic for a
// given input because chunk sizes halve from a fixed ceiling.
//
// A candidate is only accepted when it reproduces a divergence at the same
// observable contract; the first divergence index may move, which is expected.
func Shrink(ops []Operation, replay Replayer, maxRounds int) ([]Operation, *Divergence, error) {
	current := append([]Operation(nil), ops...)
	div, err := replay(current)
	if err != nil {
		return nil, nil, err
	}
	if div == nil {
		return current, nil, errors.New("sequence does not diverge; nothing to shrink")
	}
	// Drop leading operations that are not needed to reproduce. Chunk removal
	// below can also do this, but the seed prologue is long and shared by
	// every seed, so handling it first keeps shrinking quick and minimal.
	current, div, err = dropPrefixes(current, div, replay)
	if err != nil {
		return nil, nil, err
	}
	if maxRounds <= 0 {
		maxRounds = 200
	}
	for round := 0; round < maxRounds; round++ {
		improved := false
		// Try whole-chunk removals, largest first.
		for size := nextChunk(len(current)); size >= 1 && !improved; size /= 2 {
			if size == len(current) {
				continue
			}
			for begin := 0; begin+size <= len(current); begin++ {
				candidate := without(current, begin, begin+size)
				if len(candidate) == 0 {
					continue
				}
				found, err := replay(candidate)
				if err != nil {
					return nil, nil, err
				}
				if found != nil {
					current, div = candidate, found
					improved = true
					break
				}
			}
		}
		// Single-operation deletion pass for 1-minimality.
		if improved {
			continue
		}
		for index := 0; index < len(current); index++ {
			candidate := without(current, index, index+1)
			if len(candidate) == 0 {
				continue
			}
			found, err := replay(candidate)
			if err != nil {
				return nil, nil, err
			}
			if found != nil {
				current, div = candidate, found
				improved = true
				break
			}
		}
		if !improved {
			return current, div, nil
		}
	}
	return current, div, nil
}

// dropPrefixes removes leading operations as long as the remainder still
// diverges. It tries ever larger prefixes, so a whole seed prologue disappears
// in a handful of replays. Every accepted candidate is replayed and verified;
// nothing relies on a monotonicity assumption that a mutation sequence need
// not satisfy.
func dropPrefixes(ops []Operation, div *Divergence, replay Replayer) ([]Operation, *Divergence, error) {
	for _, size := range prefixSizes(len(ops)) {
		if size >= len(ops) {
			continue
		}
		found, err := replay(ops[size:])
		if err != nil {
			return nil, nil, err
		}
		if found == nil {
			continue
		}
		ops, div = ops[size:], found
	}
	return ops, div, nil
}

// prefixSizes yields powers of two below n, largest first, then every smaller
// size so a final fine-grained pass is possible.
func prefixSizes(n int) []int {
	var sizes []int
	for size := 1; size < n; size *= 2 {
		sizes = append([]int{size}, sizes...)
	}
	return sizes
}

func nextChunk(n int) int {
	if n <= 1 {
		return 0
	}
	size := 1
	for size*2 < n {
		size *= 2
	}
	return size
}

func without(ops []Operation, from, to int) []Operation {
	out := make([]Operation, 0, len(ops)-(to-from))
	out = append(out, ops[:from]...)
	out = append(out, ops[to:]...)
	return out
}

// MarshalSequence renders operations as JSON for a failure artifact.
func MarshalSequence(ops []Operation) ([]byte, error) {
	return json.MarshalIndent(ops, "", "  ")
}

// UnmarshalSequence parses a saved failure artifact.
func UnmarshalSequence(data []byte) ([]Operation, error) {
	var ops []Operation
	if err := json.Unmarshal(data, &ops); err != nil {
		return nil, err
	}
	return ops, nil
}
