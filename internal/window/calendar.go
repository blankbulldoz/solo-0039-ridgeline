package window

import (
	"strings"
	"time"

	"ridgeline/internal/model"
)

// Locator maps a UTC millisecond timestamp to its half-open aggregation
// window. Start and End are UTC milliseconds delimiting the real window
// interval; Local names the window in civil calendar terms and is empty
// for fixed-step windows.
type Locator func(t int64) (start, end int64, local string)

var weekdays = map[string]time.Weekday{
	"sunday":    time.Sunday,
	"monday":    time.Monday,
	"tuesday":   time.Tuesday,
	"wednesday": time.Wednesday,
	"thursday":  time.Thursday,
	"friday":    time.Friday,
	"saturday":  time.Saturday,
}

// Locate resolves the windowing scheme of an aggregate query. A query
// carries either a fixed step and origin or a calendar specification,
// never both.
func Locate(q model.Query) (Locator, error) {
	if q.Calendar != nil {
		return calendarLocator(*q.Calendar)
	}
	if q.Step <= 0 || q.Step > model.MaxTime || !model.ValidTime(q.Origin) {
		return nil, model.Bad("window step and origin outside supported range")
	}
	origin, step := q.Origin, q.Step
	return func(t int64) (int64, int64, string) {
		start := Bucket(t, origin, step)
		return start, start + step, ""
	}, nil
}

func calendarLocator(c model.Calendar) (Locator, error) {
	if c.Timezone == "" {
		return nil, model.Bad("calendar timezone is required")
	}
	loc, err := time.LoadLocation(c.Timezone)
	if err != nil {
		return nil, model.Bad("unknown IANA timezone")
	}
	switch c.Unit {
	case "day":
		if c.WeekStart != "" {
			return nil, model.Bad("week_start applies only to week windows")
		}
		return dayLocator(loc), nil
	case "week":
		start := time.Monday
		if c.WeekStart != "" {
			weekday, ok := weekdays[strings.ToLower(c.WeekStart)]
			if !ok {
				return nil, model.Bad("unknown week_start weekday")
			}
			start = weekday
		}
		return weekLocator(loc, start), nil
	case "month":
		if c.WeekStart != "" {
			return nil, model.Bad("week_start applies only to week windows")
		}
		return monthLocator(loc), nil
	default:
		return nil, model.Bad("unknown calendar unit")
	}
}

// midnight returns the first instant of the local civil date containing t.
func midnight(t int64, loc *time.Location) time.Time {
	y, m, d := time.UnixMilli(t).In(loc).Date()
	return time.Date(y, m, d, 0, 0, 0, 0, loc)
}

// dayLocator buckets by local civil day. Days that contain a UTC offset
// transition are shorter or longer than 24 hours; the emitted boundaries
// are the true instants of local midnight.
func dayLocator(loc *time.Location) Locator {
	return func(t int64) (int64, int64, string) {
		start := midnight(t, loc)
		end := start.AddDate(0, 0, 1)
		return start.UnixMilli(), end.UnixMilli(), start.Format("2006-01-02")
	}
}

// weekLocator buckets by local civil weeks anchored on the configured
// first weekday. A week containing an offset transition keeps its seven
// civil days and is not exactly 168 hours long.
func weekLocator(loc *time.Location, first time.Weekday) Locator {
	return func(t int64) (int64, int64, string) {
		day := midnight(t, loc)
		back := (int(day.Weekday()) - int(first) + 7) % 7
		start := day.AddDate(0, 0, -back)
		end := start.AddDate(0, 0, 7)
		return start.UnixMilli(), end.UnixMilli(), start.Format("2006-01-02")
	}
}

// monthLocator buckets by local calendar month, from local midnight on
// the first day of the month to local midnight on the first day of the
// next month.
func monthLocator(loc *time.Location) Locator {
	return func(t int64) (int64, int64, string) {
		y, m, _ := time.UnixMilli(t).In(loc).Date()
		start := time.Date(y, m, 1, 0, 0, 0, 0, loc)
		end := start.AddDate(0, 1, 0)
		return start.UnixMilli(), end.UnixMilli(), start.Format("2006-01")
	}
}
