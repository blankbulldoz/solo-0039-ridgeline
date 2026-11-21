// Package session segments one series of visible points into collection
// sessions and classifies how firmly each segment boundary is established.
package session

// Boundary classifications for one segment edge.
const (
	// Started is a confirmed session start inside the report window.
	Started = "started"
	// Ended is a confirmed session end inside the report window.
	Ended = "ended"
	// CarriedIn means the session continues across the left window edge.
	CarriedIn = "carried_in"
	// CarriedOut means the session continues across the right window edge.
	CarriedOut = "carried_out"
	// UnknownLeft means readable history cannot prove a new start.
	UnknownLeft = "unknown_left"
	// UnknownRight means data past the confirmation deadline can still
	// arrive, so the session cannot be called ended.
	UnknownRight = "unknown_right"
)

// Bounds carries the report window and the confirmation horizons shared by
// every series in one session query.
type Bounds struct {
	Start int64 // report window [Start, End)
	End   int64
	Gap   int64 // adjacent visible points farther than Gap break a session
	AsOf  int64 // caller-confirmed collection deadline
	Floor int64 // retention floor: older history is physically gone
}

// Run is one maximal run of in-window points with no internal gap.
type Run struct {
	First  int64
	Last   int64
	Points int
	Left   string
	Right  string
}

// Split segments the in-window points of one series. before holds the
// visible timestamps in [Start-Gap, Start) and after the visible timestamps
// in [End, End+Gap); every slice is sorted ascending. Only runs with at
// least one in-window point are reported.
func Split(b Bounds, points, before, after []int64) []Run {
	var runs []Run
	for lo, i := 0, 1; i <= len(points); i++ {
		if i < len(points) && points[i]-points[i-1] <= b.Gap {
			continue
		}
		run := Run{First: points[lo], Last: points[i-1], Points: i - lo}
		predecessor, seen := int64(0), false
		if lo > 0 {
			predecessor, seen = points[lo-1], true
		} else if len(before) > 0 {
			predecessor, seen = before[len(before)-1], true
		}
		run.Left = left(b, run.First, predecessor, seen)
		successor := int64(0)
		seen = false
		if i < len(points) {
			successor, seen = points[i], true
		} else if len(after) > 0 {
			successor, seen = after[0], true
		}
		run.Right = right(b, run.Last, successor, seen)
		runs = append(runs, run)
		lo = i
	}
	return runs
}

// left classifies the start edge of a run whose first in-window point is
// first. A visible predecessor within Gap carries the session across the
// window edge. Otherwise the start is confirmed only when the whole
// lookback interval [first-Gap, first) is both readable (at or above the
// retention floor) and arrived (at or before the confirmation deadline);
// missing readable history is never reported as a new start.
func left(b Bounds, first, predecessor int64, seen bool) string {
	if seen && first-predecessor <= b.Gap {
		return CarriedIn
	}
	if first-b.Gap < b.Floor || first-1 > b.AsOf {
		return UnknownLeft
	}
	return Started
}

// right classifies the end edge of a run whose last in-window point is
// last. A visible successor within Gap carries the session across the
// window edge. Otherwise the end is confirmed only when the whole gap
// interval (last, last+Gap] is at or before the caller-confirmed
// collection deadline; data that has not fully arrived is never reported
// as an end.
func right(b Bounds, last, successor int64, seen bool) string {
	if seen && successor-last <= b.Gap {
		return CarriedOut
	}
	if last+b.Gap > b.AsOf {
		return UnknownRight
	}
	return Ended
}
