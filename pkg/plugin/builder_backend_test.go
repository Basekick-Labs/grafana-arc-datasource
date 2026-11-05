package plugin

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

// Backend fixes the query builder needs (issue #20). Each also changes what
// Code-mode queries get, so the cases are written as SQL a person could type.

var builderTestRange = backend.TimeRange{
	From: time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC),
	To:   time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC),
}

// A double-quoted identifier may contain an apostrophe. The macro walkers
// used to read it as the start of a string literal and stop expanding.
func TestMacrosExpandAfterQuotedIdentifierWithApostrophe(t *testing.T) {
	for _, sql := range []string{
		`SELECT $__timeGroup(time, '1m') AS "time", avg("it's") AS "v" FROM "cpu" WHERE $__timeFilter(time) GROUP BY 1 ORDER BY 1`,
		`SELECT "it's" FROM cpu WHERE $__timeFilter(time) AND "o'neil" = 'x'`,
		`SELECT "say ""it's"" twice" FROM cpu WHERE $__timeFilter(time)`,
		`SELECT "a--b", "c/*d" FROM cpu WHERE $__timeFilter(time)`,
		`SELECT "f(x" FROM cpu WHERE $__timeFilter(time) AND $__timeFrom() < now()`,
	} {
		out := ApplyMacros(sql, builderTestRange, "UTC")
		if strings.Contains(out, "$__") {
			t.Errorf("macro left unexpanded:\n in: %s\nout: %s", sql, out)
		}
		if !strings.Contains(out, "(time) >= '2026-11-05T10:00:00Z' AND (time) < '2026-11-05T12:00:00Z'") {
			t.Errorf("time filter not expanded:\n in: %s\nout: %s", sql, out)
		}
		if !hasTimeFilterMacro(newStrippedSQL(sql)) {
			t.Errorf("split heuristics no longer see $__timeFilter in %s", sql)
		}
	}
}

// $__timeFrom() / $__timeTo() go through a different walker than the macros
// with arguments; it needs the same identifier handling.
func TestTimeFromToExpandAfterQuotedIdentifierWithApostrophe(t *testing.T) {
	sql := `SELECT "it's" FROM cpu WHERE time >= $__timeFrom() AND time < $__timeTo()`
	out := ApplyMacros(sql, builderTestRange, "UTC")
	if strings.Contains(out, "$__") || !strings.Contains(out, "time >= '2026-11-05T10:00:00Z' AND time < '2026-11-05T12:00:00Z'") {
		t.Errorf("got %s", out)
	}
}

// Quoted identifiers are names: their text is copied verbatim, and the
// existing literal and comment handling is unchanged.
func TestQuotedIdentifiersPreservedAndLiteralsStillSkipped(t *testing.T) {
	sql := `SELECT "$__timeFilter(x)", 'it''s $__timeFilter(time)' FROM cpu -- $__timeFilter(time)
WHERE $__timeFilter(time)`
	out := ApplyMacros(sql, builderTestRange, "UTC")
	for _, keep := range []string{`"$__timeFilter(x)"`, `'it''s $__timeFilter(time)'`, `-- $__timeFilter(time)`} {
		if !strings.Contains(out, keep) {
			t.Errorf("%s was rewritten:\n%s", keep, out)
		}
	}
	if !strings.Contains(out, "WHERE (time) >= '2026-11-05T10:00:00Z'") {
		t.Errorf("real macro not expanded:\n%s", out)
	}
	if got := findMatchingParen(`("a)b" + 1)`, 0); got != 10 {
		t.Errorf(`findMatchingParen skipped into an identifier: got %d, want 10`, got)
	}
}

// $__interval is replaced inside literals on purpose (INTERVAL '$__interval');
// a `--` inside a quoted name must not hide the tokens after it.
func TestIntervalTokenAfterIdentifierWithCommentMarker(t *testing.T) {
	out := replaceIntervalToken(`SELECT "x--y", time_bucket('$__interval', time)`, "$__interval", "1 minute")
	if !strings.Contains(out, `time_bucket('1 minute', time)`) || !strings.Contains(out, `"x--y"`) {
		t.Errorf("got %s", out)
	}
}

// Sub-second $__interval (Grafana's value on short ranges) used to leave
// $__timeGroup unexpanded. It now buckets by whole seconds.
func TestTimeGroupAcceptsMilliseconds(t *testing.T) {
	out := ApplyMacros(`SELECT $__timeGroup(time, '500ms') AS time, avg(v) FROM cpu WHERE $__timeFilter(time) GROUP BY 1`, builderTestRange, "UTC")
	if strings.Contains(out, "$__timeGroup") {
		t.Fatalf("500ms left $__timeGroup unexpanded: %s", out)
	}
	one := ApplyMacros(`SELECT $__timeGroup(time, '1s') AS time`, builderTestRange, "UTC")
	half := ApplyMacros(`SELECT $__timeGroup(time, '500ms') AS time`, builderTestRange, "UTC")
	if one != half {
		t.Errorf("500ms should bucket like 1s:\n1s:    %s\n500ms: %s", one, half)
	}
	two := ApplyMacros(`SELECT $__timeGroup(time, '2s') AS time`, builderTestRange, "UTC")
	if got := ApplyMacros(`SELECT $__timeGroup(time, '1500ms') AS time`, builderTestRange, "UTC"); got != two {
		t.Errorf("1500ms should round up to 2s:\n2s:     %s\n1500ms: %s", two, got)
	}
}

// Chunks and buckets both align to multiples of their own width, so a bucket
// that does not divide the chunk straddles chunk edges.
func TestSplitRefusesBucketsThatDoNotDivideTheChunk(t *testing.T) {
	q := func(interval string) string {
		return `SELECT $__timeGroup(time, '` + interval + `') AS time, avg(v) FROM cpu WHERE $__timeFilter(time) GROUP BY 1`
	}
	for _, c := range []struct {
		interval string
		chunk    time.Duration
		unsafe   bool
	}{
		{"7m", time.Hour, true},      // 60 is not a multiple of 7
		{"4h", 6 * time.Hour, true},  // 6 is not a multiple of 4
		{"25m", time.Hour, true},     // 60 is not a multiple of 25
		{"5m", time.Hour, false},     // divides
		{"2h", 6 * time.Hour, false}, // divides
		{"1h", time.Hour, false},     // equal
		{"7m", 7 * time.Hour, false}, // 420 is a multiple of 7
		{"1d", 7 * 24 * time.Hour, false},
		{"500ms", time.Hour, false}, // a 1s bucket divides everything
		{"2h", time.Hour, true},     // wider than the chunk (unchanged)
	} {
		sql := q(c.interval)
		if got := splitCorruptsBuckets(sql, newStrippedSQL(sql), "UTC", c.chunk); got != c.unsafe {
			t.Errorf("interval %s, chunk %s: splitCorruptsBuckets = %v, want %v", c.interval, c.chunk, got, c.unsafe)
		}
	}
}

// An empty statement is answered by the plugin, with a message that points at
// the likely cause, instead of being sent to Arc.
func TestQueryRejectsEmptySQL(t *testing.T) {
	srv := jsonServer(t, `{"columns":["v"],"data":[[1]]}`)
	inst := newTestInstance(t, srv.URL, ProtocolJSON)
	d := &ArcDatasource{}
	for _, sql := range []string{"", "   ", "\n\t"} {
		raw, _ := json.Marshal(map[string]any{"sql": sql, "format": "table"})
		resp := d.query(context.Background(), inst, backend.DataQuery{RefID: "A", JSON: raw, TimeRange: builderTestRange})
		if resp.Error == nil || !strings.Contains(resp.Error.Error(), "Query is empty") {
			t.Errorf("sql %q: error = %v", sql, resp.Error)
		}
	}
	// rawSql from another SQL datasource still runs when sql is empty.
	raw, _ := json.Marshal(map[string]any{"sql": "", "rawSql": "SELECT 1 AS v", "format": "table"})
	if resp := d.query(context.Background(), inst, backend.DataQuery{RefID: "A", JSON: raw, TimeRange: builderTestRange}); resp.Error != nil {
		t.Errorf("rawSql migration broke: %v", resp.Error)
	}
}
