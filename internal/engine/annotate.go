package engine

import (
	"encoding/json"
	"fmt"

	"ridgeline/internal/model"
	selectseries "ridgeline/internal/select"
	"ridgeline/internal/volume"

	bolt "go.etcd.io/bbolt"
)

func readAnnotations(b *bolt.Bucket) ([]model.Annotation, error) {
	out := make([]model.Annotation, 0)
	bucket := b.Bucket(volume.Annotations)
	if bucket == nil {
		// Namespaces created before interval annotations existed.
		return out, nil
	}
	err := bucket.ForEach(func(_, value []byte) error {
		var a model.Annotation
		if err := json.Unmarshal(value, &a); err != nil {
			return err
		}
		out = append(out, a)
		return nil
	})
	return out, err
}

// covering selects annotations whose half-open interval overlaps
// [start, end) and whose creation-time series binding intersects the
// selected series. Annotations bound to no selected series are not the
// caller's context; annotations never disappear with the samples.
func covering(annotations []model.Annotation, selected []model.Series, start, end int64) []model.Annotation {
	bound := make(map[string]bool, len(selected))
	for _, s := range selected {
		bound[s.ID] = true
	}
	out := make([]model.Annotation, 0)
	for _, a := range annotations {
		if a.Start >= end || a.End <= start {
			continue
		}
		for _, id := range a.Series {
			if bound[id] {
				out = append(out, a)
				break
			}
		}
	}
	return out
}

func (e *Engine) Annotate(name string, input model.AnnotationInput) (model.Annotation, error) {
	var out model.Annotation
	if err := model.ValidateAnnotationInput(input); err != nil {
		return out, err
	}
	selector, err := selectseries.Compile(input.Matchers)
	if err != nil {
		return out, err
	}
	if input.Matchers == nil {
		input.Matchers = []model.Matcher{}
	}
	err = e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		all, err := catalog(b)
		if err != nil {
			return err
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		out = model.Annotation{
			ID: fmt.Sprintf("%020d", seq), Start: input.Start, End: input.End,
			Kind: input.Kind, Note: input.Note, Matchers: input.Matchers,
			Sequence: seq, Series: []string{},
		}
		// The selector resolves once at creation; series admitted later
		// with matching labels never join this binding.
		for _, s := range selectseries.Pick(all, selector) {
			out.Series = append(out.Series, s.ID)
		}
		notes, err := b.CreateBucketIfNotExists(volume.Annotations)
		if err != nil {
			return err
		}
		if err := volume.PutJSON(notes, []byte(out.ID), out); err != nil {
			return err
		}
		n.LastSequence = seq
		return volume.PutJSON(b, volume.Config, n)
	})
	return out, err
}

func (e *Engine) Correct(name, id string, input model.AnnotationCorrection) (model.Annotation, error) {
	var out model.Annotation
	if err := model.ValidateAnnotationCorrection(input); err != nil {
		return out, err
	}
	err := e.DB.Raw.Update(func(tx *bolt.Tx) error {
		b, n, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		if n.ReadOnly {
			return model.Conflict("namespace is read-only")
		}
		notes := b.Bucket(volume.Annotations)
		if notes == nil || notes.Get([]byte(id)) == nil {
			return model.Missing("annotation does not exist")
		}
		if err := volume.GetJSON(notes, []byte(id), &out); err != nil {
			return err
		}
		seq, err := volume.Next(tx)
		if err != nil {
			return err
		}
		// A correction rewrites interval, kind and note in place; the
		// identity and the creation-time series binding are retained.
		out.Start, out.End, out.Kind, out.Note = input.Start, input.End, input.Kind, input.Note
		out.Sequence, out.Revisions = seq, out.Revisions+1
		if err := volume.PutJSON(notes, []byte(out.ID), out); err != nil {
			return err
		}
		n.LastSequence = seq
		return volume.PutJSON(b, volume.Config, n)
	})
	return out, err
}

func (e *Engine) Annotations(name string) ([]model.Annotation, error) {
	var out []model.Annotation
	err := e.DB.Raw.View(func(tx *bolt.Tx) error {
		b, _, err := loadNamespace(tx, name)
		if err != nil {
			return err
		}
		out, err = readAnnotations(b)
		return err
	})
	return out, err
}
