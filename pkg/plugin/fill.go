package plugin

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// Gap filling for $__timeGroup's third argument, as Grafana's Postgres and
// MySQL datasources do it: $__timeGroup(time, '5m', NULL | previous | linear |
// <number>). The SQL is unchanged; the backend fills the merged frame.

// bucketSpec is one $__timeGroup bucket. sql renders it for DuckDB and floor /
// next walk the same buckets in Go, so the fill grid lands exactly on the rows
// Arc returns. Both come from newBucketSpec, so the two cannot disagree on
// which branch a bucket takes.
type bucketSpec struct {
	secs int
	// unit is empty for epoch arithmetic, else the date_trunc unit ("hour",
	// "day", "week") applied to the wall clock in loc.
	unit string
	tz   string
	loc  *time.Location
}

// newBucketSpec decides how an interval buckets in a dashboard timezone. tz
// must already be validated.
func newBucketSpec(interval, tz string) (bucketSpec, bool) {
	secs, ok := intervalToSeconds(interval)
	if !ok {
		return bucketSpec{}, false
	}
	b := bucketSpec{secs: secs, tz: tz, loc: time.UTC}
	// A bucket of an hour or more in a NON-UTC dashboard must align to
	// local calendar boundaries, not UTC ones: with epoch arithmetic a
	// "day" starts at 00:00 UTC, which in UTC-6 is 18:00 the previous
	// evening, so every bar mixes two local days. date_trunc on the
	// wall-clock time gets this right, including across DST transitions
	// where a local day is not 86400 seconds long.
	//
	// Only whole calendar units (hour/day/week) have a date_trunc
	// equivalent. 6h, 12h, 3d and every sub-hour size stay on the epoch
	// path, which is correct for them in whole-hour zones and also avoids
	// DuckDB's nanosecond-residual behaviour.
	//
	// UTC dashboards NEVER take this branch, so their SQL is byte-identical
	// to every release before this feature. That matters twice over:
	// date_trunc on a TIMESTAMPTZ truncates in the DuckDB session's
	// timezone, so a "UTC" dashboard would silently follow Arc's session
	// setting rather than UTC.
	if tz != "UTC" && secs >= 3600 {
		if unit, ok := truncUnitForSeconds(secs, interval, tz); ok {
			loc, err := time.LoadLocation(tz)
			if err != nil {
				return bucketSpec{}, false
			}
			b.unit, b.loc = unit, loc
		}
	}
	return b, true
}

// sql renders the bucket expression for column.
func (b bucketSpec) sql(column string) string {
	if b.unit != "" {
		// timezone(tz, ts), not `ts AT TIME ZONE tz`. The infix form's
		// direction depends on the operand's type, and which of
		// TIMESTAMP/TIMESTAMPTZ it yields varies between DuckDB/ICU
		// builds — on Arc's build the round trip converted forward
		// twice and local midnight rendered as noon. The function form
		// is explicit: the inner call takes the instant to wall-clock
		// in tz, the outer reads that wall clock back as an instant.
		// Verified against DuckDB 1.4.3 on Arc: a 05:59Z and a 06:01Z
		// row land in different local days for America/Costa_Rica.
		q := quoteTimezone(b.tz)
		return fmt.Sprintf("timezone(%s, date_trunc('%s', timezone(%s, %s)))", q, b.unit, q, column)
	}
	// Use epoch_ns() (BIGINT) with // (integer division) instead of epoch() (DOUBLE)
	// to avoid floating-point precision loss that causes timestamps near hour
	// boundaries (e.g. 05:59:59.999) to round up to the next bucket (06:00:00).
	// DuckDB's / operator returns DOUBLE; // returns BIGINT.
	return fmt.Sprintf("to_timestamp((epoch_ns(%s) // 1000000000 // %d) * %d)", column, b.secs, b.secs)
}

// floor is the start of the bucket holding t, as the SQL computes it.
func (b bucketSpec) floor(t time.Time) time.Time {
	if b.unit == "" {
		s := int64(b.secs)
		u := t.Unix()
		f := u / s
		if u%s != 0 && u < 0 {
			f--
		}
		return time.Unix(f*s, 0).UTC()
	}
	l := t.In(b.loc)
	switch b.unit {
	case "hour":
		return time.Date(l.Year(), l.Month(), l.Day(), l.Hour(), 0, 0, 0, b.loc).UTC()
	case "week":
		back := (int(l.Weekday()) + 6) % 7 // date_trunc('week') starts on Monday
		return time.Date(l.Year(), l.Month(), l.Day()-back, 0, 0, 0, 0, b.loc).UTC()
	default: // day
		return time.Date(l.Year(), l.Month(), l.Day(), 0, 0, 0, 0, b.loc).UTC()
	}
}

// next is the start of the bucket after the one starting at start.
func (b bucketSpec) next(start time.Time) time.Time {
	if b.unit == "" {
		return start.Add(time.Duration(b.secs) * time.Second)
	}
	l := start.In(b.loc)
	var n time.Time
	switch b.unit {
	case "hour":
		n = b.floor(start.Add(time.Hour))
		if !n.After(start) {
			// A fall-back hour repeats the wall clock, and DuckDB merges the
			// two into one bucket; step past the repeat.
			n = b.floor(start.Add(2 * time.Hour))
		}
	case "week":
		n = time.Date(l.Year(), l.Month(), l.Day()+7, 0, 0, 0, 0, b.loc).UTC()
	default:
		n = time.Date(l.Year(), l.Month(), l.Day()+1, 0, 0, 0, 0, b.loc).UTC()
	}
	return n
}

type fillMode int

const (
	fillNull fillMode = iota + 1
	fillPrevious
	fillLinear
	fillValue
)

// parseFillArg reads $__timeGroup's third argument: NULL, previous, linear,
// or a number. Postgres knows the first two and numbers; linear is ours.
func parseFillArg(arg string) (fillMode, float64, bool) {
	switch strings.ToLower(strings.TrimSpace(arg)) {
	case "null":
		return fillNull, 0, true
	case "previous":
		return fillPrevious, 0, true
	case "linear":
		return fillLinear, 0, true
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(arg), 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, 0, false
	}
	return fillValue, v, true
}

// fillPlan is a query's fill request, read once before the query runs: chunk
// goroutines only expand macros, they never record anything.
type fillPlan struct {
	mode     fillMode
	value    float64
	bucket   bucketSpec
	from, to time.Time
	// skip, when set, is why the query asked for fill but will not get it.
	skip string
}

// maxFillPoints bounds a filled frame: grid points times series.
const maxFillPoints = 1_000_000

// planFill reads the fill argument from sql, whose interval tokens are still
// unexpanded on backend-only paths. Returns nil when the query asks for no
// fill.
func planFill(sql string, tr time.Duration, from, to time.Time, tz string) *fillPlan {
	if !strings.Contains(sql, "$__timeGroup(") {
		return nil
	}
	sql = replaceIntervalToken(sql, "$__interval_ms", strconv.FormatInt(intervalMilliseconds(tr), 10))
	sql = replaceIntervalToken(sql, "$__interval", calculateInterval(tr))
	var args [][]string
	replaceMacroOccurrences(sql, "$__timeGroup(", func(arg string) (string, bool) {
		args = append(args, strings.Split(arg, ","))
		return "", false
	})
	var plan *fillPlan
	for _, parts := range args {
		if len(parts) < 3 {
			continue
		}
		raw := strings.TrimSpace(parts[2])
		mode, value, ok := parseFillArg(raw)
		if !ok {
			return &fillPlan{skip: fmt.Sprintf("Gaps were not filled: %q is not a fill mode. $__timeGroup's third argument is NULL, previous, linear or a number.", raw)}
		}
		bucket, ok := newBucketSpec(strings.Trim(strings.TrimSpace(parts[1]), "'\""), tz)
		if !ok {
			return nil // the macro is not expanded either; Arc reports the query
		}
		plan = &fillPlan{mode: mode, value: value, bucket: bucket, from: from, to: to}
	}
	if plan != nil && len(args) > 1 {
		return &fillPlan{skip: fmt.Sprintf("Gaps were not filled: fill needs exactly one $__timeGroup in the query, and this one has %d.", len(args))}
	}
	return plan
}

// applyFill returns the frame with every series given a row for every bucket
// in the range, and missing or NULL values filled. It returns the frame
// unchanged, with a notice saying why, whenever filling could invent data or
// misplace it.
func applyFill(frame *data.Frame, plan *fillPlan) *data.Frame {
	if plan == nil || frame == nil {
		return frame
	}
	if plan.skip != "" {
		addNotice(frame, fillNotice(plan.skip))
		return frame
	}
	if frame.Meta != nil {
		for _, n := range frame.Meta.Notices {
			if n.Severity == data.NoticeSeverityWarning {
				// A row cap or a dropped chunk: the missing rows are not gaps.
				addNotice(frame, fillNotice("Gaps were not filled, because the result is incomplete (see the other notice)."))
				return frame
			}
		}
	}
	out, why := fillFrame(frame, plan)
	if why != "" {
		addNotice(frame, fillNotice("Gaps were not filled: "+why))
		return frame
	}
	return out
}

func fillNotice(text string) data.Notice {
	return data.Notice{Severity: data.NoticeSeverityInfo, Text: text}
}

func fillFrame(frame *data.Frame, plan *fillPlan) (*data.Frame, string) {
	rows, err := frame.RowLen()
	if err != nil || rows == 0 {
		return frame, ""
	}
	schema := frame.TimeSeriesSchema()
	if schema.Type == data.TimeSeriesTypeNot {
		return nil, "the result is not a time series."
	}
	timeField := frame.Fields[schema.TimeIndex]

	// The bucket grid over the range: from the bucket holding `from` to the
	// last one starting before `to`.
	var grid []time.Time
	index := map[int64]int{}
	for g := plan.bucket.floor(plan.from); g.Before(plan.to); g = plan.bucket.next(g) {
		if len(grid) >= maxFillPoints {
			return nil, fmt.Sprintf("the range holds more than %d buckets.", maxFillPoints)
		}
		index[g.UnixNano()] = len(grid)
		grid = append(grid, g)
	}

	// Series by their label values, in order of first appearance.
	type series struct {
		row  int   // a row holding its label values
		rows []int // grid position -> row, or -1
	}
	var order []*series
	byKey := map[string]*series{}
	var key strings.Builder
	for r := 0; r < rows; r++ {
		key.Reset()
		for _, fi := range schema.FactorIndices {
			v, ok := frame.Fields[fi].ConcreteAt(r)
			if ok {
				fmt.Fprintf(&key, "1%v\x00", v)
			} else {
				key.WriteString("0\x00")
			}
		}
		s := byKey[key.String()]
		if s == nil {
			if (len(order)+1)*len(grid) > maxFillPoints {
				return nil, fmt.Sprintf("the series times buckets exceed %d points.", maxFillPoints)
			}
			s = &series{row: r, rows: make([]int, len(grid))}
			for i := range s.rows {
				s.rows[i] = -1
			}
			byKey[key.String()] = s
			order = append(order, s)
		}
		tv, ok := timeField.ConcreteAt(r)
		if !ok {
			return nil, "a row has no time."
		}
		t := tv.(time.Time)
		gi, ok := index[t.UnixNano()]
		if !ok {
			return nil, "the rows' times are not the $__timeGroup buckets, so the query's buckets differ from the fill's."
		}
		if s.rows[gi] >= 0 {
			return nil, "two rows share a series and a bucket."
		}
		s.rows[gi] = r
	}

	n := len(grid) * len(order)
	fields := make([]*data.Field, len(frame.Fields))
	isFactor := map[int]bool{}
	for _, fi := range schema.FactorIndices {
		isFactor[fi] = true
	}
	for fi, f := range frame.Fields {
		switch {
		case fi == schema.TimeIndex:
			ts := make([]time.Time, 0, n)
			for _, g := range grid {
				for range order {
					ts = append(ts, g)
				}
			}
			fields[fi] = data.NewField(f.Name, f.Labels, ts)
		case isFactor[fi]:
			nf := data.NewFieldFromFieldType(f.Type(), n)
			nf.Name, nf.Labels = f.Name, f.Labels
			for gi := range grid {
				for si, s := range order {
					nf.Set(gi*len(order)+si, f.At(s.row))
				}
			}
			fields[fi] = nf
		case f.Type().Numeric():
			vals := make([]*float64, n)
			col := make([]*float64, len(grid))
			times := make([]int64, len(grid))
			for gi, g := range grid {
				times[gi] = g.UnixNano()
			}
			for si, s := range order {
				for gi, r := range s.rows {
					col[gi] = nil
					if r >= 0 {
						if v, err := f.NullableFloatAt(r); err == nil && v != nil && !math.IsNaN(*v) {
							x := *v
							col[gi] = &x
						}
					}
				}
				fillColumn(col, times, plan)
				for gi := range grid {
					vals[gi*len(order)+si] = col[gi]
				}
			}
			fields[fi] = data.NewField(f.Name, f.Labels, vals)
		default:
			// Text and other values: kept where present, NULL in new rows.
			nf := data.NewFieldFromFieldType(f.Type().NullableType(), n)
			nf.Name, nf.Labels = f.Name, f.Labels
			for gi := range grid {
				for si, s := range order {
					if r := s.rows[gi]; r >= 0 {
						if v, ok := f.ConcreteAt(r); ok {
							nf.SetConcrete(gi*len(order)+si, v)
						}
					}
				}
			}
			fields[fi] = nf
		}
		fields[fi].Config = f.Config
	}
	out := data.NewFrame(frame.Name, fields...)
	out.RefID = frame.RefID
	out.Meta = frame.Meta
	return out, ""
}

// fillColumn fills one series' values along the grid in place.
func fillColumn(col []*float64, times []int64, plan *fillPlan) {
	switch plan.mode {
	case fillValue:
		for i := range col {
			if col[i] == nil {
				v := plan.value
				col[i] = &v
			}
		}
	case fillPrevious:
		var last *float64
		for i := range col {
			if col[i] == nil {
				col[i] = last
			} else {
				last = col[i]
			}
		}
	case fillLinear:
		// By time, not by index: a DST day makes buckets unequal. The ends
		// stay empty; there is nothing to interpolate from.
		prev := -1
		for i := range col {
			if col[i] == nil {
				continue
			}
			if prev >= 0 && i-prev > 1 {
				a, b := *col[prev], *col[i]
				span := float64(times[i] - times[prev])
				for j := prev + 1; j < i; j++ {
					v := a + (b-a)*float64(times[j]-times[prev])/span
					col[j] = &v
				}
			}
			prev = i
		}
	}
}

// expandBucketMacros expands the two macros the builder's transformations
// use to read buckets before the range, as InfluxQL does: derivative(max(x))
// with GROUP BY time has a value at the range's first bucket, computed from
// the bucket before it, and that first bucket holds every point in it, not
// only those after the range start.
//
//	$__timeFilterLookback(time, interval, n): like $__timeFilter, from n
//	  buckets before the bucket holding the range start.
//	$__timeGroupStart(interval): the start of the bucket holding the range
//	  start, a timestamp literal, to drop the lookback rows again.
//
// Both use bucketSpec, so local-calendar buckets (days in a non-UTC
// dashboard) step back by local days, as $__timeGroup buckets them.
func expandBucketMacros(sql string, from, to time.Time, tz string) string {
	tz = validateTimezone(tz)
	sql = replaceMacroOccurrences(sql, "$__timeFilterLookback(", func(arg string) (string, bool) {
		parts := strings.Split(arg, ",")
		if len(parts) != 3 {
			return "", false
		}
		column := strings.TrimSpace(parts[0])
		if err := validateColumnArg(column); err != nil {
			return "", false
		}
		b, ok := newBucketSpec(strings.Trim(strings.TrimSpace(parts[1]), "'\""), tz)
		n, err := strconv.Atoi(strings.TrimSpace(parts[2]))
		if !ok || err != nil || n < 0 || n > 1000 {
			return "", false
		}
		start := b.floor(from)
		for i := 0; i < n; i++ {
			start = b.floor(start.Add(-time.Nanosecond))
		}
		return fmt.Sprintf("(%s) >= '%s' AND (%s) < '%s'", column, start.Format(time.RFC3339), column, to.Format(time.RFC3339)), true
	})
	return replaceMacroOccurrences(sql, "$__timeGroupStart(", func(arg string) (string, bool) {
		b, ok := newBucketSpec(strings.Trim(strings.TrimSpace(arg), "'\""), tz)
		if !ok {
			return "", false
		}
		return "'" + b.floor(from).Format(time.RFC3339) + "'", true
	})
}
