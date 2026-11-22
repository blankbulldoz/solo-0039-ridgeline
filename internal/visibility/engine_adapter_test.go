package visibility_test

import (
	"errors"
	"os"
	"path/filepath"

	"ridgeline/internal/engine"
	"ridgeline/internal/model"
	visibility "ridgeline/internal/visibility"
)

// engineAdapter is the only place where tests touch the production engine. It
// exposes public entry points and converts results into the model's neutral
// observation types. It never calls the visibility model to compute an answer.
type engineAdapter struct {
	engine *engine.Engine
	path   string
}

func openAdapter(t testingT, path string) *engineAdapter {
	t.Helper()
	a, err := openAdapterErr(path)
	if err != nil {
		t.Fatalf("open engine %s: %v", path, err)
	}
	return a
}

func openAdapterErr(path string) (*engineAdapter, error) {
	e, err := engine.Open(path)
	if err != nil {
		return nil, err
	}
	return &engineAdapter{engine: e, path: path}, nil
}

type testingT interface {
	Helper()
	Fatalf(format string, args ...any)
}

func (a *engineAdapter) Close() error { return a.engine.Close() }

func toPolicy(p visibility.Policy) model.Namespace {
	return model.Namespace{
		Name: "space", ReadOnly: p.ReadOnly, RetentionMS: p.RetentionMS,
		LatenessMS: p.LatenessMS, FutureMS: p.FutureMS, MaxSeries: p.MaxSeries,
		MaxWrite: p.MaxWrite, MaxScan: p.MaxScan,
	}
}

func (a *engineAdapter) Create(space string, p visibility.Policy) error {
	n := toPolicy(p)
	n.Name = space
	_, err := a.engine.Create(n)
	return err
}

func toLabels(l visibility.Labels) model.Labels {
	out := make(model.Labels, len(l))
	for k, v := range l {
		out[k] = v
	}
	return out
}

func fromLabels(l model.Labels) visibility.Labels {
	out := make(visibility.Labels, len(l))
	for k, v := range l {
		out[k] = v
	}
	return out
}

func toMatchers(ms []visibility.Matcher) []model.Matcher {
	out := make([]model.Matcher, 0, len(ms))
	for _, m := range ms {
		out = append(out, model.Matcher{Label: m.Label, Op: m.Op, Value: m.Value})
	}
	return out
}

func errorCode(err error) string {
	if err == nil {
		return ""
	}
	var domain *model.Error
	if errors.As(err, &domain) {
		return domain.Code
	}
	return "internal"
}

func (a *engineAdapter) Apply(op visibility.Operation) (string, error) {
	space := op.Space
	switch op.Kind {
	case visibility.WriteKind:
		points := make([]model.Observation, 0, len(op.Points))
		for _, p := range op.Points {
			points = append(points, model.Observation{
				Labels: toLabels(p.Labels), Time: p.Time, Value: p.Value,
			})
		}
		_, err := a.engine.Write(space, model.WriteRequest{Now: op.Now, Points: points})
		if err != nil {
			var domain *model.Error
			if !errors.As(err, &domain) {
				return "", err
			}
			return domain.Code, nil
		}
		return "", nil
	case visibility.DeleteKind:
		_, err := a.engine.Delete(space, model.Query{
			Start: op.Start, End: op.End, Matchers: toMatchers(op.Matchers),
		})
		if err != nil {
			var domain *model.Error
			if !errors.As(err, &domain) {
				return "", err
			}
			return domain.Code, nil
		}
		return "", nil
	case visibility.SealKind:
		_, err := a.engine.Seal(space)
		return errorCode(err), nonDomain(err)
	case visibility.CompactKind:
		_, err := a.engine.Compact(space)
		return errorCode(err), nonDomain(err)
	case visibility.RetainKind:
		_, err := a.engine.Retain(space, op.Now)
		return errorCode(err), nonDomain(err)
	case visibility.ModeKind:
		_, err := a.engine.ReadOnly(space, op.ReadOnly)
		return errorCode(err), nonDomain(err)
	default:
		return "invalid", nil
	}
}

func nonDomain(err error) error {
	var domain *model.Error
	if err == nil || errors.As(err, &domain) {
		return nil
	}
	return err
}

func (a *engineAdapter) Range(space string, start, end int64, matchers []visibility.Matcher) (visibility.Snapshot, error) {
	result, err := a.engine.Range(space, model.Query{Start: start, End: end, Matchers: toMatchers(matchers)})
	if err != nil {
		return visibility.Snapshot{}, err
	}
	snap := visibility.Snapshot{Floor: result.Floor, Series: []visibility.SeriesView{}}
	for _, sp := range result.Series {
		points := make([]visibility.Point, 0, len(sp.Points))
		for _, p := range sp.Points {
			points = append(points, visibility.Point{Time: p.Time, Value: p.Value})
		}
		snap.Series = append(snap.Series, visibility.SeriesView{
			Key: sp.Series.ID, Labels: fromLabels(sp.Series.Labels), Points: points,
		})
	}
	return snap, nil
}

func (a *engineAdapter) State(space string) (int64, int64, bool, error) {
	n, err := a.engine.Namespace(space)
	if err != nil {
		return 0, 0, false, err
	}
	return n.Floor, n.Watermark, n.HasWatermark, nil
}

func (a *engineAdapter) Identity(labels visibility.Labels) string {
	return model.SeriesID(toLabels(labels))
}

// imageToFile copies the engine's consistent image to a fresh path, verifying
// that the bytes and the advertised sequence are usable.
func imageToFile(t testingT, a *engineAdapter, destination string) uint64 {
	t.Helper()
	image, err := a.engine.DB.Snapshot()
	if err != nil {
		t.Fatalf("snapshot image: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		t.Fatalf("prepare image directory: %v", err)
	}
	if err := os.WriteFile(destination, image.Data, 0o600); err != nil {
		t.Fatalf("write image: %v", err)
	}
	return image.Sequence
}
