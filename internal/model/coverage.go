package model

// Interval is a half-open time interval [Start, End).
type Interval struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// Empty reports whether the interval contains no timestamp.
func (i Interval) Empty() bool {
	return i.End <= i.Start
}

const (
	// CoverageIntact means the retention floor lies at or before the request.
	CoverageIntact = "intact"
	// CoveragePartial means the floor cuts through the requested interval.
	CoveragePartial = "partial"
	// CoverageExpired means the whole request precedes the floor.
	CoverageExpired = "expired"
)

// Coverage explains how a requested interval relates to the namespace's
// irreversible retention floor. Requested is what the caller asked for,
// Retained is the part that is still readable, and Expired is the part that
// can never be read again.
//
// Retention is an admission boundary, not a record that samples existed.
// Retained therefore does not assert prior data; an empty retained region
// means no visible admitted samples (deletions may also hide samples), while
// an empty expired region is ambiguous between never measured and removed.
type Coverage struct {
	Requested Interval `json:"requested"`
	Retained  Interval `json:"retained"`
	Expired   Interval `json:"expired"`
	Floor     int64    `json:"floor"`
	Status    string   `json:"status"`
	Note      string   `json:"note"`
}

// CoverageFor derives request-level retention scope for [start, end).
func CoverageFor(start, end, floor int64) Coverage {
	out := Coverage{
		Requested: Interval{Start: start, End: end},
		Floor:     floor,
	}

	retainedStart := start
	if floor > retainedStart {
		retainedStart = floor
	}
	if retainedStart > end {
		retainedStart = end
	}
	out.Retained = Interval{Start: retainedStart, End: end}

	expiredEnd := end
	if floor < expiredEnd {
		expiredEnd = floor
	}
	if expiredEnd < start {
		expiredEnd = start
	}
	out.Expired = Interval{Start: start, End: expiredEnd}

	switch {
	case out.Expired.Empty():
		out.Status = CoverageIntact
		out.Note = "the retention floor lies at or before this request, so no part has expired; " +
			"an empty result means no visible admitted samples (or deletions hide them), not that retention removed data"
	case out.Retained.Empty():
		out.Status = CoverageExpired
		out.Note = "the whole request precedes the retention floor and is permanently unrecoverable; " +
			"an empty result cannot distinguish never-measured history from expired data"
	default:
		out.Status = CoveragePartial
		out.Note = "the expired part is permanently unrecoverable and can no longer be distinguished from never-measured history; " +
			"the retained part is only an admission boundary, so emptiness there still means no visible admitted sample"
	}
	return out
}

// CellScope describes one aligned aggregation window. Complete is true only
// when Covered equals Window, meaning the stored value covers the whole window
// and can be compared with other complete windows. Retained shows the part
// left by the floor, and Expired shows the part no longer readable.
type CellScope struct {
	Window   Interval `json:"window"`
	Retained Interval `json:"retained"`
	Covered  Interval `json:"covered"`
	Expired  Interval `json:"expired"`
	Complete bool     `json:"complete"`
	Note     string   `json:"note,omitempty"`
}

// CellScopeFor derives the scope of the window [windowStart, windowEnd) for a
// request of [start, end) under the given retention floor.
func CellScopeFor(windowStart, windowEnd, start, end, floor int64) CellScope {
	out := CellScope{Window: Interval{Start: windowStart, End: windowEnd}}

	retainedStart := windowStart
	if floor > retainedStart {
		retainedStart = floor
	}
	if retainedStart > windowEnd {
		retainedStart = windowEnd
	}
	out.Retained = Interval{Start: retainedStart, End: windowEnd}

	coveredStart := windowStart
	if floor > coveredStart {
		coveredStart = floor
	}
	if start > coveredStart {
		coveredStart = start
	}
	coveredEnd := windowEnd
	if end < coveredEnd {
		coveredEnd = end
	}
	if coveredEnd < coveredStart {
		coveredStart = coveredEnd
	}
	out.Covered = Interval{Start: coveredStart, End: coveredEnd}

	expiredEnd := windowEnd
	if floor < expiredEnd {
		expiredEnd = floor
	}
	if expiredEnd < windowStart {
		expiredEnd = windowStart
	}
	out.Expired = Interval{Start: windowStart, End: expiredEnd}

	out.Complete = out.Covered == out.Window
	clippedRetained := out.Covered.Start > out.Retained.Start || out.Covered.End < out.Retained.End
	switch {
	case out.Complete:
	case !out.Expired.Empty() && clippedRetained:
		out.Note = "partly expired and partly outside the requested interval; " +
			"the value covers only the retained request remainder and is not comparable with a complete window"
	case !out.Expired.Empty():
		out.Note = "crosses the retention floor; " +
			"the value covers only the retained remainder and is not comparable with a complete window"
	default:
		out.Note = "crosses the requested interval boundary; " +
			"the value covers only the requested remainder and is not comparable with a complete window"
	}
	return out
}

// RollupSource separates a stored materialization from what the current
// retention floor still allows. It distinguishes a complete statistic formed
// before retention from a recomputation at the current floor, which may cover
// only parts of the requested windows.
type RollupSource struct {
	Requested         Interval `json:"requested"`
	Recorded          bool     `json:"recorded"`
	MaterializedFloor int64    `json:"materialized_floor"`
	CurrentFloor      int64    `json:"current_floor"`
	ExpiredSince      Interval `json:"expired_since"`
	Current           Coverage `json:"current"`
	Verifiable        bool     `json:"verifiable"`
	Complete          bool     `json:"complete"`
	RecomputeShrinks  bool     `json:"recompute_shrinks_source"`
	Note              string   `json:"note"`
}

// storedCoveragePresent reports whether the materialization recorded the scope
// it was computed under. A zero-valued Coverage is the marker for a record
// written before scope provenance existed: a query interval can never be the
// empty [0,0) interval that this check would otherwise accept.
func storedCoveragePresent(c Coverage) bool {
	return c.Requested.Start != 0 || c.Requested.End != 0
}

// RollupSourceFor derives the source explanation for a materialization read
// under currentFloor. ExpiredSince is the slice of the source that was
// readable when materialized but is below the floor now.
func RollupSourceFor(m Materialization, currentFloor int64) RollupSource {
	query := m.Query
	stored := m.Result.Coverage
	out := RollupSource{
		Requested:         Interval{Start: query.Start, End: query.End},
		Recorded:          storedCoveragePresent(stored),
		MaterializedFloor: stored.Floor,
		CurrentFloor:      currentFloor,
		Current:           CoverageFor(query.Start, query.End, currentFloor),
	}

	if out.Recorded {
		sinceStart := query.Start
		if stored.Floor > sinceStart {
			sinceStart = stored.Floor
		}
		sinceEnd := query.End
		if currentFloor < sinceEnd {
			sinceEnd = currentFloor
		}
		if sinceEnd < sinceStart {
			sinceEnd = sinceStart
		}
		out.ExpiredSince = Interval{Start: sinceStart, End: sinceEnd}
	}
	out.Verifiable = out.Current.Expired.Empty()
	out.RecomputeShrinks = !out.ExpiredSince.Empty()

	complete := out.Recorded && stored.Expired.Empty()
	if complete {
		for _, cell := range m.Result.Cells {
			if !cell.Coverage.Complete {
				complete = false
				break
			}
		}
	}
	out.Complete = complete

	switch {
	case !out.Recorded:
		out.Note = "the materialization carries no recorded source scope (written before scope provenance existed); " +
			"whether its windows were complete cannot be established, so compare it with a recomputation explicitly"
	case out.Verifiable && complete:
		out.Note = "the source interval remains fully readable at the current floor and the stored result covered every window in full; " +
			"it can still be re-verified by recomputation"
	case out.Verifiable:
		out.Note = "the source interval remains readable, but the stored result already covered only part of its windows " +
			"(request clipping or an earlier floor); a recomputation now covers the same visible source"
	case complete:
		out.Note = "the stored result was a complete statistic when materialized; the source in expired_since is now below the floor " +
			"and cannot be re-read or re-verified, and recomputing the same query at the current floor yields partial windows " +
			"that are not interchangeable with the stored value"
	default:
		out.Note = "the stored result was already partial when materialized and part of its source has since expired; " +
			"neither the stored value nor a current-floor recomputation covers the full requested windows"
	}
	return out
}
