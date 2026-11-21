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
	if q.Width < 0 || q.Width > model.MaxTime || !model.ValidTime(q.Start-q.Width) {
		return model.Bad("window width outside supported range")
	}
	if q.Width > 0 && q.Function == "rate" {
		return model.Bad("sliding width is only supported for count, sum, min, max, avg, first and last")
	}
	if q.Width > 0 {
		if outputs := int64(alignCeil(q.End, q.Origin, q.Step)-alignCeil(q.Start, q.Origin, q.Step)) / q.Step; outputs > MaxOutputs {
			return model.Capacity("sliding window output count exceeds the supported limit")
		}
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

func SortCells(cells []model.Cell) {
	sort.Slice(cells, func(i, j int) bool {
		a, _ := json.Marshal(cells[i].Labels)
		b, _ := json.Marshal(cells[j].Labels)
		if string(a) == string(b) {
			return cells[i].Start < cells[j].Start
		}
		return string(a) < string(b)
	})
}
