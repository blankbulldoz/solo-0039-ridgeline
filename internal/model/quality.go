package model

// Quality describes how much a single observation can be trusted. It is a
// property of an observation, not of a series: putting it into labels would
// split one measurement stream into unrelated series.
type Quality string

const (
	// QualityTrusted observations were measured directly and are usable.
	QualityTrusted Quality = "trusted"
	// QualityEstimated observations are approximations and remain usable.
	QualityEstimated Quality = "estimated"
	// QualityInvalid observations may not be used in reductions.
	QualityInvalid Quality = "invalid"
)

// QualityOrder is the canonical reporting order for quality breakdowns.
var QualityOrder = []Quality{QualityTrusted, QualityEstimated, QualityInvalid}

// ParseQuality normalizes an omitted quality to trusted and rejects unknown
// spellings. An omitted quality is the pre-existing scalar observation shape.
func ParseQuality(value string) (Quality, error) {
	switch Quality(value) {
	case "", QualityTrusted:
		return QualityTrusted, nil
	case QualityEstimated, QualityInvalid:
		return Quality(value), nil
	default:
		return "", Bad("unknown observation quality")
	}
}

// QualitySet is the set of qualities admitted into a calculation.
type QualitySet map[Quality]bool

func DefaultQualitySet() QualitySet {
	return QualitySet{QualityTrusted: true, QualityEstimated: true}
}

// NewQualitySet validates an explicit include list and removes duplicates. An
// empty list preserves the default of trusted plus estimated.
func NewQualitySet(list []Quality) (QualitySet, error) {
	if len(list) == 0 {
		return DefaultQualitySet(), nil
	}
	set := QualitySet{}
	for _, quality := range list {
		switch Quality(quality) {
		case QualityTrusted, QualityEstimated, QualityInvalid:
			set[Quality(quality)] = true
		default:
			return nil, Bad("invalid quality include value")
		}
	}
	return set, nil
}

func (s QualitySet) Has(quality Quality) bool {
	if quality == "" {
		quality = QualityTrusted
	}
	return s[quality]
}

// QualityTally counts observations per quality.
type QualityTally struct {
	Trusted   int `json:"trusted"`
	Estimated int `json:"estimated"`
	Invalid   int `json:"invalid"`
}

func (t *QualityTally) Add(quality Quality) {
	switch quality {
	case QualityEstimated:
		t.Estimated++
	case QualityInvalid:
		t.Invalid++
	default:
		t.Trusted++
	}
}

func (t *QualityTally) Merge(other QualityTally) {
	t.Trusted += other.Trusted
	t.Estimated += other.Estimated
	t.Invalid += other.Invalid
}

func (t QualityTally) Total() int {
	return t.Trusted + t.Estimated + t.Invalid
}

func (t QualityTally) Count(quality Quality) int {
	switch quality {
	case QualityEstimated:
		return t.Estimated
	case QualityInvalid:
		return t.Invalid
	default:
		return t.Trusted
	}
}

// QualityContribution reports what one included quality contributed to a
// reduction. For count/sum/min/max/avg/first/last, Sum is the arithmetic sum
// of the included values. For rate, Observations counts the observations used
// and Sum is the reset-aware increase attributed to the later observation of
// each step, so the client can see which qualities drove the observed change.
type QualityContribution struct {
	Quality      Quality `json:"quality"`
	Observations int     `json:"observations"`
	Sum          float64 `json:"sum"`
}

// QualitySummary explains the quality composition of a request or result.
// Included and Excluded always partition the visible observations considered.
// ExcludedBetween counts excluded observations strictly between two included
// observations of the same series and window; SpansExcluded reports that a
// counter rate was computed across such a gap. Suppressed counts would-be
// cells that produced no value because quality filtering left too few
// observations.
type QualitySummary struct {
	Included        QualityTally          `json:"included"`
	Excluded        QualityTally          `json:"excluded"`
	Contributions   []QualityContribution `json:"contributions,omitempty"`
	ExcludedBetween int                   `json:"excluded_between"`
	SpansExcluded   bool                  `json:"spans_excluded"`
	Suppressed      int                   `json:"suppressed"`
}
