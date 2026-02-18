package plugin

import (
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
		{"SELECT limited FROM t", false}, // "limited" is not " LIMIT "
	}
	for _, c := range cases {
		result := containsLIMIT(c.sql)
		if result != c.expected {
			t.Errorf("containsLIMIT(%q): expected %v, got %v", c.sql, c.expected, result)
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
	if !contains(result, "epoch_ns(time) // 1000000000 // 3600") || !contains(result, "epoch_ns(created_at) // 1000000000 // 86400") {
		t.Errorf("expected both macros expanded, got: %s", result)
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
		{"unknown", 3600}, // default
	}
	for _, c := range cases {
		result := intervalToSeconds(c.input)
		if result != c.expected {
			t.Errorf("intervalToSeconds(%q): expected %d, got %d", c.input, c.expected, result)
		}
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

	if contains(result, "$__timeFilter") {
		t.Errorf("macro not expanded: %s", result)
	}
	if !contains(result, "2026-02-18T10:00:00Z") || !contains(result, "2026-02-18T11:00:00Z") {
		t.Errorf("expected time range in result: %s", result)
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
		if !contains(result, c.expected) {
			t.Errorf("for %dh range, expected interval %q in: %s", c.hours, c.expected, result)
		}
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
	if !contains(result, "2026-02-18T06:00:00Z") {
		t.Errorf("expected chunk From in filter: %s", result)
	}
	// Interval should use original 8d range (> 7d) → "1 hour"
	if !contains(result, "1 hour") {
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

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsStr(s, substr))
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
