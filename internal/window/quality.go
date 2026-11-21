package window

import (
	"ridgeline/internal/model"
)

// qualityOf normalizes a stored quality; records written before observation
// quality existed carry no value and are trusted.
func qualityOf(s model.Sample) model.Quality {
	if s.Quality == "" {
		return model.QualityTrusted
	}
	return s.Quality
}

// cellKey identifies one (series, group, window) triple. Filtering accounting
// is per series because only excluded observations from the same series can
// interrupt that series' own counter progression.
type cellKey struct {
	series string
	group  string
	start  int64
}

// selection is the per-query quality filter plus the accounting needed to
// explain which observations were used and which were excluded.
type selection struct {
	include model.QualitySet
	// full enables per-quality arithmetic contributions. It is set by an
	// explicit quality request; a plain default query still explains
	// exclusions but does not pay for per-quality sums.
	full bool

	included map[cellKey]model.QualityTally
	excluded map[cellKey]model.QualityTally
	spans    map[cellKey]*span
	// suppressed records output cells (group, start) that the reducer could not
	// produce because filtering left too few observations. The reducer knows how
	// many included observations each function needs, so it decides this
	// explicitly instead of inferring it from tallies.
	suppressed map[groupStart]bool

	total  map[model.Quality]*model.QualityContribution
	byCell map[cellKey]map[model.Quality]*model.QualityContribution
}

type span struct {
	minT, maxT int64
	seen       bool
	excluded   []int64
}

// groupStart identifies one output cell without its series.
type groupStart struct {
	group string
	start int64
}

func newSelection(q model.Query) (*selection, error) {
	include, err := model.NewQualitySet(q.IncludeQualities)
	if err != nil {
		return nil, err
	}
	return &selection{
		include:    include,
		full:       reportRequested(q),
		included:   map[cellKey]model.QualityTally{},
		excluded:   map[cellKey]model.QualityTally{},
		spans:      map[cellKey]*span{},
		suppressed: map[groupStart]bool{},
		total:      map[model.Quality]*model.QualityContribution{},
		byCell:     map[cellKey]map[model.Quality]*model.QualityContribution{},
	}, nil
}

func (s *selection) wants(quality model.Quality) bool {
	return s.include.Has(quality)
}

func (s *selection) contribution(key cellKey, quality model.Quality) *model.QualityContribution {
	c := s.total[quality]
	if c == nil {
		c = &model.QualityContribution{Quality: quality}
		s.total[quality] = c
	}
	cell := s.byCell[key]
	if cell == nil {
		cell = map[model.Quality]*model.QualityContribution{}
		s.byCell[key] = cell
	}
	cc := cell[quality]
	if cc == nil {
		cc = &model.QualityContribution{Quality: quality}
		cell[quality] = cc
	}
	return cc
}

// includeObservation records an observation admitted into a cell reduction and
// extends the cell's included time span. The arithmetic amount is added
// separately so that rate can attribute reset-aware increase instead of raw
// values.
func (s *selection) includeObservation(key cellKey, p model.Sample) {
	quality := qualityOf(p)
	tally := s.included[key]
	tally.Add(quality)
	s.included[key] = tally
	if s.full {
		s.contribution(key, quality).Observations++
		s.total[quality].Observations++
	}

	sp := s.spans[key]
	if sp == nil {
		sp = &span{}
		s.spans[key] = sp
	}
	if !sp.seen {
		sp.seen = true
		sp.minT, sp.maxT = p.Time, p.Time
	}
	if p.Time < sp.minT {
		sp.minT = p.Time
	}
	if p.Time > sp.maxT {
		sp.maxT = p.Time
	}
}

// excludeObservation records an observation the filter removed. Every excluded
// time is retained regardless of arrival order; excludedBetween decides later
// whether it sat inside the included span of its series.
func (s *selection) excludeObservation(key cellKey, p model.Sample) {
	tally := s.excluded[key]
	tally.Add(qualityOf(p))
	s.excluded[key] = tally
	if sp := s.spans[key]; sp != nil {
		sp.excluded = append(sp.excluded, p.Time)
	} else {
		s.spans[key] = &span{excluded: []int64{p.Time}}
	}
}

// contribute adds amount to a quality's contribution for a cell and for the
// whole result. Observations are counted by includeObservation.
func (s *selection) contribute(key cellKey, quality model.Quality, amount float64) {
	if !s.full {
		return
	}
	s.contribution(key, quality).Sum += amount
	s.total[quality].Sum += amount
}

// suppress records that an output cell produced no value only because quality
// filtering left fewer observations than the reducer needs.
func (s *selection) suppress(group string, start int64) {
	s.suppressed[groupStart{group, start}] = true
}

// markSuppressed flags output cells that are absent only because filtering
// removed observations. A cell counts as suppressed when the reducer produced
// no value for it and at least one of its series had enough observations in
// total but fewer than minPerSeries after filtering. produced holds the cells
// the reducer actually emitted.
func (s *selection) markSuppressed(produced map[groupStart]bool, minPerSeries int) {
	type group struct {
		group string
		start int64
	}
	starved := map[group]bool{}
	// Consider every series-cell that contributed either included or excluded
	// observations; a cell with only excluded observations was starved too.
	cells := map[cellKey]bool{}
	for cell := range s.included {
		cells[cell] = true
	}
	for cell := range s.excluded {
		cells[cell] = true
	}
	for cell := range cells {
		kept := s.included[cell].Total()
		dropped := s.excluded[cell].Total()
		if kept < minPerSeries && kept+dropped >= minPerSeries {
			starved[group{cell.group, cell.start}] = true
		}
	}
	for g := range starved {
		if !produced[groupStart{g.group, g.start}] {
			s.suppress(g.group, g.start)
		}
	}
}

// groupSummary merges the per-series accounting of one output cell. A cell is
// reported as suppressed when its value is absent only because filtering
// removed observations that would otherwise have satisfied the reducer.
func (s *selection) groupSummary(group string, start int64) *model.QualitySummary {
	var included, excluded model.QualityTally
	contrib := map[model.Quality]*model.QualityContribution{}
	for key, tally := range s.included {
		if key.group == group && key.start == start {
			included.Merge(tally)
		}
	}
	for key, tally := range s.excluded {
		if key.group == group && key.start == start {
			excluded.Merge(tally)
		}
	}
	for key, cell := range s.byCell {
		if key.group != group || key.start != start {
			continue
		}
		for quality, c := range cell {
			dst := contrib[quality]
			if dst == nil {
				dst = &model.QualityContribution{Quality: quality}
				contrib[quality] = dst
			}
			dst.Observations += c.Observations
			dst.Sum += c.Sum
		}
	}
	suppressed := 0
	if s.suppressed[groupStart{group, start}] {
		suppressed = 1
	}
	between := s.excludedBetween(group, start)
	return &model.QualitySummary{
		Included:        included,
		Excluded:        excluded,
		Contributions:   contributionsInOrder(contrib),
		ExcludedBetween: between,
		SpansExcluded:   between > 0,
		Suppressed:      suppressed,
	}
}

// resultSummary reports the whole reduction. Each (group, window) cell is
// counted at most once as suppressed.
func (s *selection) resultSummary() *model.QualitySummary {
	var included, excluded model.QualityTally
	for _, tally := range s.included {
		included.Merge(tally)
	}
	for _, tally := range s.excluded {
		excluded.Merge(tally)
	}
	between := 0
	for _, sp := range s.spans {
		between += excludedBetween(sp)
	}
	return &model.QualitySummary{
		Included:        included,
		Excluded:        excluded,
		Contributions:   contributionsInOrder(s.total),
		ExcludedBetween: between,
		SpansExcluded:   between > 0,
		Suppressed:      len(s.suppressed),
	}
}

func (s *selection) needsExplanation() bool {
	for _, tally := range s.excluded {
		if tally.Total() > 0 {
			return true
		}
	}
	return false
}

// cellHasExcluded reports whether a specific cell dropped an observation.
func (s *selection) cellHasExcluded(group string, start int64) bool {
	for key, tally := range s.excluded {
		if key.group == group && key.start == start && tally.Total() > 0 {
			return true
		}
	}
	return false
}

func (s *selection) excludedBetween(group string, start int64) int {
	total := 0
	for key, sp := range s.spans {
		if key.group == group && key.start == start {
			total += excludedBetween(sp)
		}
	}
	return total
}

// excludedBetween counts excluded observations strictly inside the included
// time span of their own series. An exclusion outside that span cannot have
// interrupted a within-series calculation.
func excludedBetween(sp *span) int {
	if sp == nil || !sp.seen {
		return 0
	}
	total := 0
	for _, t := range sp.excluded {
		if t > sp.minT && t < sp.maxT {
			total++
		}
	}
	return total
}

func contributionsInOrder(m map[model.Quality]*model.QualityContribution) []model.QualityContribution {
	out := make([]model.QualityContribution, 0, len(m))
	for _, quality := range model.QualityOrder {
		if c := m[quality]; c != nil {
			out = append(out, *c)
		}
	}
	return out
}

func reportRequested(q model.Query) bool {
	return q.ReportQuality || len(q.IncludeQualities) > 0
}
