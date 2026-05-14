package plugin

import (
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// --- autoSplitDuration ---

func TestAutoSplitDuration_Under3h_NoSplit(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 18, 12, 0, 0, 0, time.UTC), // 2h
	}
	dur, ok := autoSplitDuration(tr)
	if ok || dur != 0 {
		t.Errorf("expected no split for <3h range, got dur=%v ok=%v", dur, ok)
	}
}

func TestAutoSplitDuration_3hTo24h_1hChunks(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 18, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 18, 12, 0, 0, 0, time.UTC), // 12h
	}
	dur, ok := autoSplitDuration(tr)
	if !ok || dur != time.Hour {
		t.Errorf("expected 1h chunks for 12h range, got dur=%v ok=%v", dur, ok)
	}
}

func TestAutoSplitDuration_1dTo7d_6hChunks(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 18, 0, 0, 0, 0, time.UTC), // 3d
	}
	dur, ok := autoSplitDuration(tr)
	if !ok || dur != 6*time.Hour {
		t.Errorf("expected 6h chunks for 3d range, got dur=%v ok=%v", dur, ok)
	}
}

func TestAutoSplitDuration_7dTo30d_1dChunks(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC), // 14d
	}
	dur, ok := autoSplitDuration(tr)
	if !ok || dur != 24*time.Hour {
		t.Errorf("expected 1d chunks for 14d range, got dur=%v ok=%v", dur, ok)
	}
}

func TestAutoSplitDuration_Over30d_7dChunks(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC), // 45d
	}
	dur, ok := autoSplitDuration(tr)
	if !ok || dur != 7*24*time.Hour {
		t.Errorf("expected 7d chunks for 45d range, got dur=%v ok=%v", dur, ok)
	}
}

func TestAutoSplitDuration_ZeroRange_NoSplit(t *testing.T) {
	now := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
	tr := backend.TimeRange{From: now, To: now}
	dur, ok := autoSplitDuration(tr)
	if ok || dur != 0 {
		t.Errorf("expected no split for zero range, got dur=%v ok=%v", dur, ok)
	}
}

func TestAutoSplitDuration_NegativeRange_NoSplit(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 18, 12, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC), // to < from
	}
	dur, ok := autoSplitDuration(tr)
	if ok || dur != 0 {
		t.Errorf("expected no split for negative range, got dur=%v ok=%v", dur, ok)
	}
}

func TestAutoSplitDuration_Exactly3h_1hChunks(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 18, 13, 0, 0, 0, time.UTC), // exactly 3h
	}
	dur, ok := autoSplitDuration(tr)
	if !ok || dur != time.Hour {
		t.Errorf("expected 1h chunks for exactly 3h range, got dur=%v ok=%v", dur, ok)
	}
}

// --- parseSplitDuration ---

func TestParseSplitDuration_Off(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC),
	}
	dur, ok := parseSplitDuration("off", tr)
	if ok || dur != 0 {
		t.Errorf("expected no split for 'off', got dur=%v ok=%v", dur, ok)
	}
}

func TestParseSplitDuration_Auto(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC), // 14d
	}
	dur, ok := parseSplitDuration("auto", tr)
	if !ok || dur != 24*time.Hour {
		t.Errorf("expected auto=1d for 14d range, got dur=%v ok=%v", dur, ok)
	}
}

func TestParseSplitDuration_Empty_DefaultsToAuto(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC),
	}
	dur, ok := parseSplitDuration("", tr)
	durAuto, okAuto := parseSplitDuration("auto", tr)
	if dur != durAuto || ok != okAuto {
		t.Errorf("empty string should behave like 'auto': got (%v,%v) vs (%v,%v)", dur, ok, durAuto, okAuto)
	}
}

func TestParseSplitDuration_Explicit(t *testing.T) {
	tr := backend.TimeRange{} // unused for explicit values
	cases := []struct {
		input    string
		expected time.Duration
	}{
		{"1h", time.Hour},
		{"6h", 6 * time.Hour},
		{"12h", 12 * time.Hour},
		{"1d", 24 * time.Hour},
		{"3d", 3 * 24 * time.Hour},
		{"7d", 7 * 24 * time.Hour},
	}
	for _, c := range cases {
		dur, ok := parseSplitDuration(c.input, tr)
		if !ok || dur != c.expected {
			t.Errorf("parseSplitDuration(%q): expected %v, got %v (ok=%v)", c.input, c.expected, dur, ok)
		}
	}
}

func TestParseSplitDuration_UnknownValue(t *testing.T) {
	tr := backend.TimeRange{}
	dur, ok := parseSplitDuration("999x", tr)
	if ok || dur != 0 {
		t.Errorf("expected no split for unknown value, got dur=%v ok=%v", dur, ok)
	}
}

// --- splitTimeRange ---

func TestSplitTimeRange_AlignedBoundaries(t *testing.T) {
	// 6h chunks, range 14:30 to 02:30 next day
	// Expected: [14:30,18:00), [18:00,00:00), [00:00,02:30)
	from := time.Date(2026, 2, 18, 14, 30, 0, 0, time.UTC)
	to := time.Date(2026, 2, 19, 2, 30, 0, 0, time.UTC)
	chunks := splitTimeRange(from, to, 6*time.Hour)

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d: %v", len(chunks), chunks)
	}

	// First chunk: 14:30 -> 18:00
	expect(t, chunks[0].From, from, "chunk[0].From")
	expect(t, chunks[0].To, time.Date(2026, 2, 18, 18, 0, 0, 0, time.UTC), "chunk[0].To")

	// Second chunk: 18:00 -> 00:00
	expect(t, chunks[1].From, time.Date(2026, 2, 18, 18, 0, 0, 0, time.UTC), "chunk[1].From")
	expect(t, chunks[1].To, time.Date(2026, 2, 19, 0, 0, 0, 0, time.UTC), "chunk[1].To")

	// Third chunk: 00:00 -> 02:30
	expect(t, chunks[2].From, time.Date(2026, 2, 19, 0, 0, 0, 0, time.UTC), "chunk[2].From")
	expect(t, chunks[2].To, to, "chunk[2].To")
}

func TestSplitTimeRange_ExactlyOnBoundary(t *testing.T) {
	// from is exactly on a 1h boundary
	from := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
	to := time.Date(2026, 2, 18, 13, 0, 0, 0, time.UTC)
	chunks := splitTimeRange(from, to, time.Hour)

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d: %v", len(chunks), chunks)
	}

	// Chunks should be [10:00,11:00), [11:00,12:00), [12:00,13:00)
	for i, c := range chunks {
		expectedFrom := from.Add(time.Duration(i) * time.Hour)
		expectedTo := expectedFrom.Add(time.Hour)
		expect(t, c.From, expectedFrom, "chunk.From")
		expect(t, c.To, expectedTo, "chunk.To")
	}
}

func TestSplitTimeRange_SmallRange_NoSplit(t *testing.T) {
	from := time.Date(2026, 2, 18, 10, 15, 0, 0, time.UTC)
	to := time.Date(2026, 2, 18, 10, 45, 0, 0, time.UTC) // 30 min
	chunks := splitTimeRange(from, to, time.Hour)

	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk for range smaller than chunkSize, got %d", len(chunks))
	}
	expect(t, chunks[0].From, from, "chunk.From")
	expect(t, chunks[0].To, to, "chunk.To")
}

func TestSplitTimeRange_ZeroDuration_NoSplit(t *testing.T) {
	from := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
	to := time.Date(2026, 2, 18, 12, 0, 0, 0, time.UTC)
	chunks := splitTimeRange(from, to, 0)

	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk for zero duration, got %d", len(chunks))
	}
}

func TestSplitTimeRange_Contiguous(t *testing.T) {
	// Verify chunks are contiguous with no gaps or overlaps
	from := time.Date(2026, 2, 18, 10, 37, 0, 0, time.UTC)
	to := time.Date(2026, 2, 20, 5, 12, 0, 0, time.UTC)
	chunks := splitTimeRange(from, to, 6*time.Hour)

	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}

	// First chunk starts at from
	expect(t, chunks[0].From, from, "first chunk start")
	// Last chunk ends at to
	expect(t, chunks[len(chunks)-1].To, to, "last chunk end")

	// Each chunk's end == next chunk's start (no gaps)
	for i := 0; i < len(chunks)-1; i++ {
		if !chunks[i].To.Equal(chunks[i+1].From) {
			t.Errorf("gap between chunk %d (to=%v) and chunk %d (from=%v)",
				i, chunks[i].To, i+1, chunks[i+1].From)
		}
	}
}

func TestSplitTimeRange_InternalBoundariesAligned(t *testing.T) {
	from := time.Date(2026, 2, 18, 10, 37, 0, 0, time.UTC)
	to := time.Date(2026, 2, 20, 5, 12, 0, 0, time.UTC)
	chunks := splitTimeRange(from, to, 6*time.Hour)

	// All internal boundaries (not first From or last To) should be on 6h epoch multiples
	for i := 0; i < len(chunks)-1; i++ {
		boundary := chunks[i].To.Unix()
		if boundary%(6*3600) != 0 {
			t.Errorf("internal boundary at %v (epoch %d) not aligned to 6h",
				chunks[i].To, boundary)
		}
	}
}

func TestSplitTimeRange_1dChunks_30dRange(t *testing.T) {
	from := time.Date(2026, 1, 19, 8, 30, 0, 0, time.UTC)
	to := time.Date(2026, 2, 18, 8, 30, 0, 0, time.UTC)
	chunks := splitTimeRange(from, to, 24*time.Hour)

	// 30 days = ~31 chunks (first and last partial + 29 full)
	if len(chunks) < 30 || len(chunks) > 32 {
		t.Fatalf("expected ~31 chunks for 30d range with 1d chunks, got %d", len(chunks))
	}

	// Verify contiguity
	for i := 0; i < len(chunks)-1; i++ {
		if !chunks[i].To.Equal(chunks[i+1].From) {
			t.Errorf("gap at chunk %d", i)
		}
	}
}

func TestSplitTimeRange_BoundaryNoDuplicates(t *testing.T) {
	// Verify that adjacent chunks use >= / < semantics so a row at exactly
	// the boundary timestamp matches only one chunk (no duplicates).
	from := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
	to := time.Date(2026, 2, 18, 13, 0, 0, 0, time.UTC)
	chunks := splitTimeRange(from, to, time.Hour)

	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}

	// Expand each chunk's $__timeFilter and verify boundary semantics
	boundaryTime := chunks[0].To // e.g. 11:00:00
	sql := "SELECT * FROM t WHERE $__timeFilter(time)"

	chunk1SQL := ApplyMacrosWithSplit(sql, chunks[0], backend.TimeRange{From: from, To: to})
	chunk2SQL := ApplyMacrosWithSplit(sql, chunks[1], backend.TimeRange{From: from, To: to})

	// Chunk 1 should use: time < '...11:00:00Z' (exclusive end)
	boundaryStr := boundaryTime.Format(time.RFC3339)
	if !strings.Contains(chunk1SQL, "time < '"+boundaryStr+"'") {
		t.Errorf("chunk 1 should exclude boundary with <: %s", chunk1SQL)
	}
	// Chunk 2 should use: time >= '...11:00:00Z' (inclusive start)
	if !strings.Contains(chunk2SQL, "time >= '"+boundaryStr+"'") {
		t.Errorf("chunk 2 should include boundary with >=: %s", chunk2SQL)
	}
}

// --- mergeFrames ---

func TestMergeFrames_Empty(t *testing.T) {
	result := mergeFrames(nil)
	if result != nil {
		t.Errorf("expected nil for empty input")
	}

	result = mergeFrames([]*data.Frame{})
	if result != nil {
		t.Errorf("expected nil for empty slice")
	}
}

func TestMergeFrames_Single(t *testing.T) {
	f := data.NewFrame("test",
		data.NewField("time", nil, []time.Time{time.Now()}),
		data.NewField("value", nil, []float64{1.0}),
	)
	result := mergeFrames([]*data.Frame{f})
	if result != f {
		t.Errorf("expected same frame for single input")
	}
}

func TestMergeFrames_TwoFrames(t *testing.T) {
	t1 := time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 2, 18, 11, 0, 0, 0, time.UTC)

	f1 := data.NewFrame("",
		data.NewField("time", nil, []time.Time{t1}),
		data.NewField("value", nil, []float64{1.0}),
	)
	f2 := data.NewFrame("",
		data.NewField("time", nil, []time.Time{t2}),
		data.NewField("value", nil, []float64{2.0}),
	)

	result := mergeFrames([]*data.Frame{f1, f2})
	if result.Rows() != 2 {
		t.Fatalf("expected 2 rows, got %d", result.Rows())
	}
}

func TestMergeFrames_SkipsNilFrames(t *testing.T) {
	f := data.NewFrame("",
		data.NewField("value", nil, []float64{1.0}),
	)
	result := mergeFrames([]*data.Frame{f, nil, nil})
	if result.Rows() != 1 {
		t.Errorf("expected 1 row, got %d", result.Rows())
	}
}

func TestMergeFrames_SkipsIncompatibleSchema(t *testing.T) {
	f1 := data.NewFrame("",
		data.NewField("time", nil, []time.Time{time.Now()}),
		data.NewField("value", nil, []float64{1.0}),
	)
	f2 := data.NewFrame("",
		data.NewField("value", nil, []float64{2.0}),
	) // only 1 field vs 2

	result := mergeFrames([]*data.Frame{f1, f2})
	if result.Rows() != 1 {
		t.Errorf("expected 1 row (incompatible frame skipped), got %d", result.Rows())
	}
}

func TestMergeFrames_SkipsEmptyFirstFrame(t *testing.T) {
	empty := data.NewFrame("")
	f := data.NewFrame("",
		data.NewField("value", nil, []float64{1.0, 2.0}),
	)

	result := mergeFrames([]*data.Frame{empty, f})
	if result.Rows() != 2 {
		t.Errorf("expected 2 rows (empty first frame skipped), got %d", result.Rows())
	}
}

// --- containsLIMIT ---

func TestContainsLIMIT(t *testing.T) {
	cases := []struct {
		sql      string
		expected bool
	}{
		{"SELECT * FROM t LIMIT 10", true},
		{"SELECT * FROM t limit 10", true},
		{"SELECT * FROM t Limit 10", true},
		{"SELECT * FROM t WHERE x > 1", false},
		{"SELECT * FROM t ORDER BY time", false},
		{"SELECT limited FROM t", false},                            // "limited" is not " LIMIT "
		{"SELECT * FROM t WHERE name = 'THE LIMIT 10'", false},      // LIMIT inside string literal
		{"SELECT * FROM t WHERE desc = 'NO LIMIT ' ORDER BY id", false}, // LIMIT inside string literal with trailing space
	}
	for _, c := range cases {
		result := containsLIMIT(newStrippedSQL(c.sql))
		if result != c.expected {
			t.Errorf("containsLIMIT(%q): expected %v, got %v", c.sql, c.expected, result)
		}
	}
}

// --- containsAggregationWithoutTimeGroup ---

func TestContainsAggregationWithoutTimeGroup(t *testing.T) {
	cases := []struct {
		sql      string
		expected bool
		desc     string
	}{
		// Should detect aggregation (no $__timeGroup)
		{"SELECT COUNT(*) FROM t", true, "bare COUNT"},
		{"SELECT SUM(value) FROM t", true, "bare SUM"},
		{"SELECT AVG(value) FROM t", true, "bare AVG"},
		{"SELECT MIN(value) FROM t", true, "bare MIN"},
		{"SELECT MAX(value) FROM t", true, "bare MAX"},
		{"SELECT * FROM t GROUP BY host", true, "GROUP BY without timeGroup"},
		{"SELECT DISTINCT host FROM t", true, "DISTINCT keyword"},

		// Should NOT detect aggregation (has $__timeGroup)
		{"SELECT $__timeGroup(time, '1h'), COUNT(*) FROM t GROUP BY 1", false, "COUNT with timeGroup"},
		{"SELECT $__timeGroup(time, '1h'), AVG(value) FROM t GROUP BY 1", false, "AVG with timeGroup"},

		// Should NOT detect aggregation (no aggregation at all)
		{"SELECT * FROM t WHERE $__timeFilter(time)", false, "simple select"},
		{"SELECT time, value FROM t ORDER BY time", false, "select with order"},

		// Edge case: DISTINCT inside a string value should not trigger (improved with trailing space)
		{"SELECT * FROM t WHERE status = 'ACTIVE'", false, "no aggregation keywords"},

		// Edge case: aggregate function name without parenthesis
		{"SELECT summary FROM t", false, "SUM substring without paren"},

		// DISTINCT-containing functions
		{"SELECT APPROX_COUNT_DISTINCT(device_id) FROM t WHERE $__timeFilter(time)", true, "APPROX_COUNT_DISTINCT"},
		{"SELECT COUNT(DISTINCT device_id) FROM t", true, "COUNT with DISTINCT inside"},

		// DuckDB aggregate functions
		{"SELECT MEDIAN(duration) FROM t WHERE $__timeFilter(time)", true, "MEDIAN"},
		{"SELECT STDDEV(value) FROM t WHERE $__timeFilter(time)", true, "STDDEV"},
		{"SELECT STRING_AGG(host, ',') FROM t WHERE $__timeFilter(time)", true, "STRING_AGG"},
		{"SELECT LIST(value) FROM t WHERE $__timeFilter(time)", true, "LIST"},
		{"SELECT ARG_MIN(host, duration) FROM t WHERE $__timeFilter(time)", true, "ARG_MIN"},
		{"SELECT ARG_MAX(host, duration) FROM t WHERE $__timeFilter(time)", true, "ARG_MAX"},
		{"SELECT HISTOGRAM(value) FROM t WHERE $__timeFilter(time)", true, "HISTOGRAM"},
		{"SELECT QUANTILE_CONT(value, 0.5) FROM t WHERE $__timeFilter(time)", true, "QUANTILE_CONT"},
		{"SELECT VARIANCE(value) FROM t WHERE $__timeFilter(time)", true, "VARIANCE"},
		{"SELECT ANY_VALUE(host) FROM t WHERE $__timeFilter(time)", true, "ANY_VALUE"},
		// Functions added after DuckDB docs audit
		{"SELECT COUNTIF(status = 200) FROM t WHERE $__timeFilter(time)", true, "COUNTIF"},
		{"SELECT FAVG(value) FROM t WHERE $__timeFilter(time)", true, "FAVG"},
		{"SELECT FSUM(value) FROM t WHERE $__timeFilter(time)", true, "FSUM"},
		{"SELECT GEOMETRIC_MEAN(value) FROM t WHERE $__timeFilter(time)", true, "GEOMETRIC_MEAN"},
		{"SELECT WEIGHTED_AVG(value, weight) FROM t WHERE $__timeFilter(time)", true, "WEIGHTED_AVG"},
		{"SELECT APPROX_QUANTILE(value, 0.5) FROM t WHERE $__timeFilter(time)", true, "APPROX_QUANTILE"},
		{"SELECT MAD(value) FROM t WHERE $__timeFilter(time)", true, "MAD"},
		{"SELECT RESERVOIR_QUANTILE(value, 0.5) FROM t WHERE $__timeFilter(time)", true, "RESERVOIR_QUANTILE"},
		{"SELECT REGR_SLOPE(y, x) FROM t WHERE $__timeFilter(time)", true, "REGR_SLOPE"},
		{"SELECT KURTOSIS_POP(value) FROM t WHERE $__timeFilter(time)", true, "KURTOSIS_POP"},
		{"SELECT SKEWNESS_POP(value) FROM t WHERE $__timeFilter(time)", true, "SKEWNESS_POP"},
		{"SELECT BITSTRING_AGG(flag) FROM t WHERE $__timeFilter(time)", true, "BITSTRING_AGG"},

		// Window functions — each chunk restarts the window
		{"SELECT time, value, ROW_NUMBER() OVER (ORDER BY time) FROM t WHERE $__timeFilter(time)", true, "window ROW_NUMBER"},
		{"SELECT time, LAG(value) OVER (ORDER BY time) FROM t WHERE $__timeFilter(time)", true, "window LAG"},
		{"SELECT time, value, RANK() OVER(PARTITION BY host ORDER BY value) FROM t WHERE $__timeFilter(time)", true, "window RANK no space"},
	}
	for _, c := range cases {
		result := containsAggregationWithoutTimeGroup(newStrippedSQL(c.sql))
		if result != c.expected {
			t.Errorf("%s: containsAggregationWithoutTimeGroup(%q): expected %v, got %v", c.desc, c.sql, c.expected, result)
		}
	}
}

// --- expandTimeGroup ---

func TestExpandTimeGroup_Basic(t *testing.T) {
	sql := "SELECT $__timeGroup(time, '1h') AS time FROM t"
	result := expandTimeGroup(sql)
	expected := "SELECT to_timestamp((epoch_ns(time) // 1000000000 // 3600) * 3600) AS time FROM t"
	if result != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, result)
	}
}

func TestExpandTimeGroup_10Minutes(t *testing.T) {
	sql := "$__timeGroup(time, '10 minutes')"
	result := expandTimeGroup(sql)
	expected := "to_timestamp((epoch_ns(time) // 1000000000 // 600) * 600)"
	if result != expected {
		t.Errorf("expected:\n  %s\ngot:\n  %s", expected, result)
	}
}

func TestExpandTimeGroup_NoMacro(t *testing.T) {
	sql := "SELECT time, value FROM t"
	result := expandTimeGroup(sql)
	if result != sql {
		t.Errorf("expected unchanged SQL, got: %s", result)
	}
}

func TestExpandTimeGroup_Multiple(t *testing.T) {
	sql := "SELECT $__timeGroup(time, '1h'), $__timeGroup(created_at, '1d') FROM t"
	result := expandTimeGroup(sql)
	if result == sql {
		t.Errorf("expected macros to be expanded")
	}
	if !strings.Contains(result, "epoch_ns(time) // 1000000000 // 3600") || !strings.Contains(result, "epoch_ns(created_at) // 1000000000 // 86400") {
		t.Errorf("expected both macros expanded, got: %s", result)
	}
}

func TestExpandTimeGroup_MalformedInput(t *testing.T) {
	sql := "SELECT $__timeGroup(time) AS time FROM t"
	result := expandTimeGroup(sql)
	if result != sql {
		t.Errorf("expected malformed macro to be left unexpanded, got: %s", result)
	}
}

// --- intervalToSeconds ---

func TestIntervalToSeconds(t *testing.T) {
	cases := []struct {
		input    string
		expected int
	}{
		{"1s", 1},
		{"10s", 10},
		{"1m", 60},
		{"10m", 600},
		{"1h", 3600},
		{"1d", 86400},
		{"1 second", 1},
		{"10 seconds", 10},
		{"1 minute", 60},
		{"10 minutes", 600},
		{"1 hour", 3600},
		{"1 day", 86400},
	}
	for _, c := range cases {
		result, ok := intervalToSeconds(c.input)
		if !ok || result != c.expected {
			t.Errorf("intervalToSeconds(%q): expected (%d, true), got (%d, %v)", c.input, c.expected, result, ok)
		}
	}
	// Unknown intervals must now fail loudly rather than silently bucket at 1h.
	if _, ok := intervalToSeconds("1minutes"); ok {
		t.Errorf("intervalToSeconds(\"1minutes\") should fail; previously silently returned 3600s")
	}
	if _, ok := intervalToSeconds("nonsense"); ok {
		t.Errorf("intervalToSeconds(\"nonsense\") should fail")
	}
}

// --- ApplyMacros ---

func TestApplyMacros_TimeFilter(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 18, 11, 0, 0, 0, time.UTC),
	}
	sql := "SELECT * FROM t WHERE $__timeFilter(time)"
	result := ApplyMacros(sql, tr)

	if strings.Contains(result, "$__timeFilter") {
		t.Errorf("macro not expanded: %s", result)
	}
	if !strings.Contains(result, "2026-02-18T10:00:00Z") || !strings.Contains(result, "2026-02-18T11:00:00Z") {
		t.Errorf("expected time range in result: %s", result)
	}
}

func TestApplyMacros_TimeFilter_CustomColumn(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 18, 11, 0, 0, 0, time.UTC),
	}
	sql := "SELECT * FROM t WHERE $__timeFilter(created_at)"
	result := ApplyMacros(sql, tr)

	if strings.Contains(result, "$__timeFilter") {
		t.Errorf("macro not expanded: %s", result)
	}
	if !strings.Contains(result, "created_at >= '2026-02-18T10:00:00Z'") {
		t.Errorf("expected custom column in filter: %s", result)
	}
	if !strings.Contains(result, "created_at < '2026-02-18T11:00:00Z'") {
		t.Errorf("expected custom column in end filter: %s", result)
	}
}

func TestApplyMacros_Interval(t *testing.T) {
	cases := []struct {
		hours    int
		expected string
	}{
		{2, "10 seconds"},    // < 6h
		{12, "1 minute"},     // > 6h, < 24h
		{48, "10 minutes"},   // > 24h, < 7d
		{200, "1 hour"},      // > 7d
	}
	for _, c := range cases {
		tr := backend.TimeRange{
			From: time.Date(2026, 2, 18, 0, 0, 0, 0, time.UTC),
			To:   time.Date(2026, 2, 18, 0, 0, 0, 0, time.UTC).Add(time.Duration(c.hours) * time.Hour),
		}
		result := ApplyMacros("GROUP BY $__interval", tr)
		if !strings.Contains(result, c.expected) {
			t.Errorf("for %dh range, expected interval %q in: %s", c.hours, c.expected, result)
		}
	}
}

// TestApplyMacros_TimeFilter_MultipleOccurrences locks in the searchFrom
// advancement after a successful expansion: a second macro in the same SQL
// must also expand, exactly once, with the same time bounds.
func TestApplyMacros_TimeFilter_MultipleOccurrences(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 18, 11, 0, 0, 0, time.UTC),
	}
	sql := "SELECT * FROM a WHERE $__timeFilter(t1) UNION SELECT * FROM b WHERE $__timeFilter(t2)"
	result := ApplyMacros(sql, tr)

	if strings.Contains(result, "$__timeFilter") {
		t.Fatalf("expected both macros expanded, got: %s", result)
	}
	if !strings.Contains(result, "t1 >= '2026-02-18T10:00:00Z'") {
		t.Errorf("expected t1 filter: %s", result)
	}
	if !strings.Contains(result, "t2 >= '2026-02-18T10:00:00Z'") {
		t.Errorf("expected t2 filter: %s", result)
	}
	// Count expansions: each $__timeFilter produces exactly two `>= '...'` /
	// `< '...'` pairs. Two macros → 2 `>=` and 2 `<` occurrences.
	if got := strings.Count(result, ">= '"); got != 2 {
		t.Errorf("expected 2 `>=` occurrences (one per macro), got %d: %s", got, result)
	}
}

// TestApplyMacros_TimeFilter_RejectsUnsafeColumn locks in the searchFrom
// advancement on the rejection branch: an invalid macro must be left
// un-expanded AND must not prevent a following valid macro from expanding.
func TestApplyMacros_TimeFilter_RejectsUnsafeColumn(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 18, 11, 0, 0, 0, time.UTC),
	}
	// First macro has an injection payload — must be rejected and left as-is.
	// Second macro is valid — must still expand.
	sql := "WHERE $__timeFilter(t1 OR 1=1) AND x = 5 AND $__timeFilter(t2)"
	result := ApplyMacros(sql, tr)

	// First (unsafe) macro should be left un-expanded so Arc surfaces an error.
	if !strings.Contains(result, "$__timeFilter(t1 OR 1=1)") {
		t.Errorf("unsafe macro should be left un-expanded: %s", result)
	}
	// Second (safe) macro must still expand — proves the rejection branch
	// advances searchFrom correctly rather than spinning or skipping ahead.
	if !strings.Contains(result, "t2 >= '2026-02-18T10:00:00Z'") {
		t.Errorf("valid macro after rejection must still expand: %s", result)
	}
}

// TestApplyMacros_NotExpandedInsideStringLiteral locks in the C4 fix: a macro
// occurring inside a single-quoted string literal must NOT be expanded.
// Before this fix `WHERE message = '$__timeFilter(time)'` would have its
// literal content silently rewritten.
func TestApplyMacros_NotExpandedInsideStringLiteral(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 18, 11, 0, 0, 0, time.UTC),
	}
	sql := "SELECT * FROM logs WHERE msg = 'see $__timeFilter(time) docs' AND $__timeFilter(time)"
	result := ApplyMacros(sql, tr)

	// The literal content must be untouched.
	if !strings.Contains(result, "'see $__timeFilter(time) docs'") {
		t.Errorf("macro inside string literal should NOT be expanded: %s", result)
	}
	// The bare macro outside the literal must still expand.
	if strings.Count(result, "$__timeFilter(") != 1 {
		t.Errorf("expected exactly one un-expanded $__timeFilter (the one inside the literal), got: %s", result)
	}
	if !strings.Contains(result, "time >= '2026-02-18T10:00:00Z'") {
		t.Errorf("expected outer macro to expand: %s", result)
	}
}

// TestStripStringLiteralsAndComments_BlockCommentSpacing locks in the gemini
// review fixup: block comments must be replaced with a single space so adjacent
// tokens stay separated. Before the fix `SELECT/*x*/col` became `SELECTcol`,
// which would break the SELECT keyword check.
func TestStripStringLiteralsAndComments_BlockCommentSpacing(t *testing.T) {
	got := stripStringLiteralsAndComments("SELECT/*hidden*/col FROM t")
	if !strings.Contains(got, "SELECT ") || !strings.Contains(got, " col") {
		t.Errorf("block comment removal must leave a space; got %q", got)
	}
	// And it should NOT produce "SELECTcol"
	if strings.Contains(got, "SELECTcol") {
		t.Errorf("block comment removal merged adjacent tokens: %q", got)
	}
}

// TestCommentedOutTimeFilter_DisablesSplitting locks in the gemini review
// fixup: a `$__timeFilter` macro inside a comment should NOT count as "has
// time filter" for the splitting heuristic — the macro engine won't expand
// it, so each chunk would re-run the full query.
func TestCommentedOutTimeFilter_DisablesSplitting(t *testing.T) {
	sql := "SELECT * FROM t -- WHERE $__timeFilter(time)"
	stripped := newStrippedSQL(sql)
	if hasTimeFilterMacro(stripped) {
		t.Errorf("a commented-out $__timeFilter should not count as present: %q", sql)
	}
}

// TestCommentedOutTimeGroup_DoesNotDisableAggregationGuard locks in the
// companion gemini fixup: a commented-out `$__timeGroup` should NOT make the
// aggregation guard think the query is safe to split.
func TestCommentedOutTimeGroup_DoesNotDisableAggregationGuard(t *testing.T) {
	sql := "SELECT host, COUNT(*) FROM t -- $__timeGroup(time, '1h')\nGROUP BY host"
	stripped := newStrippedSQL(sql)
	if !containsAggregationWithoutTimeGroup(stripped) {
		t.Errorf("commented-out $__timeGroup must not disable the aggregation guard: %q", sql)
	}
}

// TestApplyMacros_NotExpandedInsideComment locks in the C4 fix for comments.
func TestApplyMacros_NotExpandedInsideComment(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 18, 11, 0, 0, 0, time.UTC),
	}
	sql := "SELECT * FROM t\n-- use $__timeFilter(time) here\nWHERE $__timeFilter(time)"
	result := ApplyMacros(sql, tr)

	if !strings.Contains(result, "-- use $__timeFilter(time) here") {
		t.Errorf("macro inside line comment should NOT be expanded: %s", result)
	}
}

// TestApplyMacros_TimeFilter_NestedParens locks in the paren-matching fix:
// $__timeFilter(COALESCE(t1, t2)) used to find the FIRST `)` and produce
// broken SQL. Now we leave it un-expanded because the column arg isn't a
// simple identifier (validateColumnArg rejects it).
func TestApplyMacros_TimeFilter_NestedParens(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 18, 11, 0, 0, 0, time.UTC),
	}
	// Nested-paren arg: validator rejects (not a plain identifier), macro
	// is left as-is. The important thing is the function returns and the
	// outer macro after it still expands.
	sql := "WHERE $__timeFilter(COALESCE(t1, t2)) AND x = 1"
	done := make(chan string, 1)
	go func() { done <- ApplyMacros(sql, tr) }()
	select {
	case result := <-done:
		// Macro left un-expanded (validator rejected the arg). What MUST not
		// happen: SQL truncation or duplication. Verify it's still well-formed.
		if !strings.Contains(result, "$__timeFilter(COALESCE(t1, t2))") {
			t.Errorf("expected nested-paren macro to be left un-expanded: %s", result)
		}
		if !strings.Contains(result, "x = 1") {
			t.Errorf("trailing SQL should not be truncated: %s", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ApplyMacros did not return — possible infinite loop on nested parens")
	}
}

// TestExpandTimeGroup_UnknownInterval locks in M4: unknown intervals are no
// longer silently bucketed at 1h.
func TestExpandTimeGroup_UnknownInterval(t *testing.T) {
	sql := "SELECT $__timeGroup(time, '1minutes') AS time FROM t"
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 18, 11, 0, 0, 0, time.UTC),
	}
	result := ApplyMacros(sql, tr)
	// Macro left un-expanded so Arc surfaces a clear error rather than
	// silently using the wrong bucket size.
	if !strings.Contains(result, "$__timeGroup(time, '1minutes')") {
		t.Errorf("unknown interval should leave macro un-expanded: %s", result)
	}
}

// TestExpandTimeGroup_ExtraArgs locks in M3: extra arguments warn loudly
// and leave the macro un-expanded.
func TestExpandTimeGroup_ExtraArgs(t *testing.T) {
	sql := "SELECT $__timeGroup(time, '1h', surprise) AS time FROM t"
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 18, 11, 0, 0, 0, time.UTC),
	}
	result := ApplyMacros(sql, tr)
	if !strings.Contains(result, "$__timeGroup(time, '1h', surprise)") {
		t.Errorf("extra args should leave macro un-expanded: %s", result)
	}
}

// TestApplyMacros_TimeFilter_NoInfiniteLoopOnUnclosedParen ensures a malformed
// macro with no closing paren returns the SQL unchanged rather than spinning.
func TestApplyMacros_TimeFilter_NoInfiniteLoopOnUnclosedParen(t *testing.T) {
	tr := backend.TimeRange{
		From: time.Date(2026, 2, 18, 10, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 18, 11, 0, 0, 0, time.UTC),
	}
	sql := "SELECT * FROM t WHERE $__timeFilter(time"
	done := make(chan string, 1)
	go func() { done <- ApplyMacros(sql, tr) }()
	select {
	case result := <-done:
		if result != sql {
			t.Errorf("expected unchanged SQL on malformed macro, got: %s", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expandTimeFilter did not return — possible infinite loop")
	}
}

func TestApplyMacrosWithSplit_UsesChunkForFilter_OriginalForInterval(t *testing.T) {
	chunk := backend.TimeRange{
		From: time.Date(2026, 2, 18, 6, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 18, 12, 0, 0, 0, time.UTC),
	}
	originalRange := backend.TimeRange{
		From: time.Date(2026, 2, 10, 0, 0, 0, 0, time.UTC),
		To:   time.Date(2026, 2, 18, 0, 0, 0, 0, time.UTC), // 8 days (> 7d)
	}

	sql := "WHERE $__timeFilter(time) GROUP BY $__interval"
	result := ApplyMacrosWithSplit(sql, chunk, originalRange)

	// Time filter should use chunk boundaries
	if !strings.Contains(result, "2026-02-18T06:00:00Z") {
		t.Errorf("expected chunk From in filter: %s", result)
	}
	// Interval should use original 8d range (> 7d) → "1 hour"
	if !strings.Contains(result, "1 hour") {
		t.Errorf("expected '1 hour' interval from 8d original range: %s", result)
	}
}

// helpers

func expect(t *testing.T, got, want time.Time, label string) {
	t.Helper()
	if !got.Equal(want) {
		t.Errorf("%s: expected %v, got %v", label, want, got)
	}
}

