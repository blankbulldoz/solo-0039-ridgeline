package model

// LabelRename is the pure result of moving one label key within a label set.
// Presence is meaningful: an explicit empty value is moved as a value, while a
// missing key leaves the label set untouched.
type LabelRename struct {
	Labels    Labels
	Present   bool
	FromValue string
	ToExists  bool
	ToValue   string
}

func RenameLabel(labels Labels, from, to string) LabelRename {
	out := make(Labels, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	fromValue, present := out[from]
	if !present {
		return LabelRename{Labels: labels}
	}
	toValue, toExists := out[to]
	if from != to {
		delete(out, from)
		out[to] = fromValue
	}
	return LabelRename{
		Labels: out, Present: true, FromValue: fromValue,
		ToExists: toExists, ToValue: toValue,
	}
}

// DropLabel returns a copy without the named key.
func DropLabel(labels Labels, name string) Labels {
	out := make(Labels, len(labels))
	for k, v := range labels {
		if k != name {
			out[k] = v
		}
	}
	return out
}

// SetLabel returns a copy with one key set.
func SetLabel(labels Labels, name, value string) Labels {
	out := make(Labels, len(labels)+1)
	for k, v := range labels {
		out[k] = v
	}
	out[name] = value
	return out
}
