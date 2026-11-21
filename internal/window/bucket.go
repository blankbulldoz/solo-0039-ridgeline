package window

import (
	"encoding/json"
	"sort"

	"ridgeline/internal/model"
)

func Validate(q model.Query) error {
	if err := model.ValidateQuery(q); err != nil {
		return err
	}
	if q.Step <= 0 || q.Step > model.MaxTime || !model.ValidTime(q.Origin) {
		return model.Bad("window step and origin outside supported range")
	}
	switch q.Function {
	case "count", "sum", "min", "max", "avg", "first", "last", "rate":
		return nil
	default:
		return model.Bad("unknown aggregation function")
	}
}

func Bucket(t, origin, step int64) int64 {
	delta := t - origin
	index := delta / step
	if delta < 0 && delta%step != 0 {
		index--
	}
	return origin + index*step
}

func Group(labels model.Labels, names []string) (string, model.Labels) {
	out := model.Labels{}
	for _, name := range names {
		if value, present := labels[name]; present {
			out[name] = value
		}
	}
	data, _ := json.Marshal(out)
	return string(data), out
}

// GroupKey canonicalizes cell labels for grouping, sorting and deduplication.
func GroupKey(labels model.Labels) string {
	data, _ := json.Marshal(labels)
	return string(data)
}

// SortCells orders cells by canonical labels then window start. The ordering is
// stable so a stored rollup result round-trips unchanged.
func SortCells(cells []model.Cell) {
	sort.Slice(cells, func(i, j int) bool {
		a, b := GroupKey(cells[i].Labels), GroupKey(cells[j].Labels)
		if a == b {
			return cells[i].Start < cells[j].Start
		}
		return a < b
	})
}
