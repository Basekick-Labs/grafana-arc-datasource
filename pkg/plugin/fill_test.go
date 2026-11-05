package plugin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

func utc(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

// The grid walks the buckets the SQL produces. Each case is a branch of
// newBucketSpec; the matrix compares them against DuckDB too.
func TestBucketSpecGrid(t *testing.T) {
	for _, c := range []struct {
		name, interval, tz, from string
		want                     []string
	}{
		{"epoch 5m", "5m", "UTC", "2026-11-05T10:07:00Z",
			[]string{"2026-11-05T10:05:00Z", "2026-11-05T10:10:00Z", "2026-11-05T10:15:00Z"}},
		{"6h in a non-UTC zone stays epoch", "6h", "America/New_York", "2026-11-05T13:00:00Z",
			[]string{"2026-11-05T12:00:00Z", "2026-11-05T18:00:00Z", "2026-11-06T00:00:00Z"}},
		{"1h in a whole-hour zone is epoch", "1h", "America/Argentina/Buenos_Aires", "2026-11-05T10:30:00Z",
			[]string{"2026-11-05T10:00:00Z", "2026-11-05T11:00:00Z"}},
		{"1h in a :30 zone", "1h", "Asia/Kolkata", "2026-11-05T10:10:00Z",
			[]string{"2026-11-05T09:30:00Z", "2026-11-05T10:30:00Z", "2026-11-05T11:30:00Z"}},
		{"local days across spring forward", "1d", "America/New_York", "2026-03-07T12:00:00Z",
			[]string{"2026-03-07T05:00:00Z", "2026-03-08T05:00:00Z", "2026-03-09T04:00:00Z", "2026-03-10T04:00:00Z"}},
		{"local days across fall back", "1d", "America/New_York", "2026-10-31T12:00:00Z",
			[]string{"2026-10-31T04:00:00Z", "2026-11-01T04:00:00Z", "2026-11-02T05:00:00Z"}},
		{"Monday weeks", "1w", "America/Argentina/Buenos_Aires", "2026-11-05T12:00:00Z",
			[]string{"2026-11-02T03:00:00Z", "2026-11-09T03:00:00Z"}},
		// Epoch weeks start on Thursday, like the epoch; 2026-11-05 is one.
		{"7d is not a calendar week", "7d", "America/Argentina/Buenos_Aires", "2026-11-07T12:00:00Z",
			[]string{"2026-11-05T00:00:00Z", "2026-11-12T00:00:00Z"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			b, ok := newBucketSpec(c.interval, c.tz)
			if !ok {
				t.Fatal("rejected")
			}
			g := b.floor(utc(c.from))
			for i, w := range c.want {
				if !g.Equal(utc(w)) {
					t.Fatalf("bucket %d = %s, want %s", i, g.Format(time.RFC3339), w)
				}
				g = b.next(g)
			}
		})
	}
}

// Pacific/Chatham is the one :45 zone with DST. At fall back its wall clock
// repeats an hour, which DuckDB merges into one bucket; the grid must still
// move forward.
func TestBucketSpecHourGridAlwaysAdvances(t *testing.T) {
	b, _ := newBucketSpec("1h", "Pacific/Chatham")
	if b.unit != "hour" {
		t.Fatalf("unit = %q, want hour", b.unit)
	}
	g := b.floor(utc("2026-04-04T10:00:00Z"))
	for i := 0; i < 48; i++ {
		n := b.next(g)
		if !n.After(g) {
			t.Fatalf("grid stuck at %s", g)
		}
		g = n
	}
}

func TestPlanFill(t *testing.T) {
	from, to := utc("2026-11-05T10:00:00Z"), utc("2026-11-05T11:00:00Z")
	for _, c := range []struct {
		sql      string
		mode     fillMode
		value    float64
		skip     string
		wantPlan bool
	}{
		{"SELECT $__timeGroup(time, '5m') FROM t", 0, 0, "", false},
		{"SELECT $__timeGroup(time, '5m', NULL) FROM t", fillNull, 0, "", true},
		{"SELECT $__timeGroup(time, '5m', previous) FROM t", fillPrevious, 0, "", true},
		{"SELECT $__timeGroup(time, '5m', LINEAR) FROM t", fillLinear, 0, "", true},
		{"SELECT $__timeGroup(time, '5m', -1.5) FROM t", fillValue, -1.5, "", true},
		{"SELECT $__timeGroup(time, $__interval, 0) FROM t", fillValue, 0, "", true},
		{"SELECT $__timeGroup(time, '5m', zero) FROM t", 0, 0, `"zero" is not a fill mode`, true},
		{"SELECT $__timeGroup(time, '5m', 0), $__timeGroup(time, '1h') FROM t", 0, 0, "has 2", true},
		{"SELECT '$__timeGroup(time, ''5m'', 0)' FROM t", 0, 0, "", false},
	} {
		p := planFill(c.sql, to.Sub(from), from, to, "UTC")
		if (p != nil) != c.wantPlan {
			t.Errorf("%s: plan = %+v", c.sql, p)
			continue
		}
		if p == nil {
			continue
		}
		if c.skip != "" {
			if !strings.Contains(p.skip, c.skip) {
				t.Errorf("%s: skip = %q", c.sql, p.skip)
			}
			continue
		}
		if p.skip != "" || p.mode != c.mode || p.value != c.value {
			t.Errorf("%s: plan = %+v", c.sql, p)
		}
	}
}

// A long frame of two hosts at 5m over 10:00-10:30: host a misses 10:10 and
// has NULL at 10:20; host b has only 10:05 and 10:20.
func gappyFrame() *data.Frame {
	f := func(v float64) *float64 { return &v }
	return data.NewFrame("A",
		data.NewField("time", nil, []time.Time{
			utc("2026-11-05T10:00:00Z"), utc("2026-11-05T10:05:00Z"), utc("2026-11-05T10:05:00Z"),
			utc("2026-11-05T10:15:00Z"), utc("2026-11-05T10:20:00Z"), utc("2026-11-05T10:20:00Z"), utc("2026-11-05T10:25:00Z"),
		}),
		data.NewField("host", nil, []string{"a", "a", "b", "a", "a", "b", "a"}),
		data.NewField("v", nil, []*float64{f(1), f(2), f(10), f(4), nil, f(40), f(6)}),
	)
}

func seriesValues(t *testing.T, frame *data.Frame, host string) []string {
	t.Helper()
	var out []string
	for r := 0; r < frame.Rows(); r++ {
		if frame.Fields[1].At(r).(string) != host {
			continue
		}
		v, _ := frame.Fields[2].NullableFloatAt(r)
		if v == nil {
			out = append(out, "null")
		} else {
			out = append(out, strconv.FormatFloat(*v, 'f', -1, 64))
		}
	}
	return out
}

func TestApplyFillModes(t *testing.T) {
	b, _ := newBucketSpec("5m", "UTC")
	from, to := utc("2026-11-05T10:00:00Z"), utc("2026-11-05T10:30:00Z")
	for _, c := range []struct {
		mode  fillMode
		value float64
		a, b  string
	}{
		{fillNull, 0, "1 2 null 4 null 6", "null 10 null null 40 null"},
		{fillValue, 0, "1 2 0 4 0 6", "0 10 0 0 40 0"},
		{fillPrevious, 0, "1 2 2 4 4 6", "null 10 10 10 40 40"},
		{fillLinear, 0, "1 2 3 4 5 6", "null 10 20 30 40 null"},
	} {
		out := applyFill(gappyFrame(), &fillPlan{mode: c.mode, value: c.value, bucket: b, from: from, to: to})
		if out.Rows() != 12 {
			t.Fatalf("mode %d: rows = %d, want 12 (6 buckets x 2 hosts)", c.mode, out.Rows())
		}
		if got := strings.Join(seriesValues(t, out, "a"), " "); got != c.a {
			t.Errorf("mode %d host a = %s, want %s", c.mode, got, c.a)
		}
		if got := strings.Join(seriesValues(t, out, "b"), " "); got != c.b {
			t.Errorf("mode %d host b = %s, want %s", c.mode, got, c.b)
		}
		if _, err := data.LongToWide(out, nil); err != nil {
			t.Errorf("mode %d: filled frame does not convert to wide: %v", c.mode, err)
		}
	}
}

// Linear interpolates by time: on the spring-forward day a local day is 23 h.
func TestFillLinearByTime(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	col := []*float64{f(0), nil, f(47)}
	times := []int64{0, int64(23 * time.Hour), int64(47 * time.Hour)}
	fillColumn(col, times, &fillPlan{mode: fillLinear})
	if *col[1] != 23 {
		t.Errorf("middle = %v, want 23", *col[1])
	}
}

func TestApplyFillRefusesToInventData(t *testing.T) {
	b, _ := newBucketSpec("5m", "UTC")
	plan := func() *fillPlan {
		return &fillPlan{mode: fillValue, bucket: b, from: utc("2026-11-05T10:00:00Z"), to: utc("2026-11-05T10:30:00Z")}
	}
	notice := func(f *data.Frame) string {
		var s []string
		for _, n := range f.Meta.Notices {
			s = append(s, n.Text)
		}
		return strings.Join(s, " | ")
	}

	capped := gappyFrame()
	addNotice(capped, data.Notice{Severity: data.NoticeSeverityWarning, Text: "Results reached Arc's row cap"})
	if out := applyFill(capped, plan()); out.Rows() != 7 || !strings.Contains(notice(out), "incomplete") {
		t.Errorf("capped: rows %d, notices %q", out.Rows(), notice(out))
	}

	offGrid := gappyFrame()
	offGrid.Fields[0].Set(0, utc("2026-11-05T10:01:00Z"))
	if out := applyFill(offGrid, plan()); out.Rows() != 7 || !strings.Contains(notice(out), "not the $__timeGroup buckets") {
		t.Errorf("off grid: rows %d, notices %q", out.Rows(), notice(out))
	}

	huge := plan()
	huge.bucket, _ = newBucketSpec("1s", "UTC")
	huge.to = huge.from.Add(12 * 24 * time.Hour)
	if out := applyFill(gappyFrame(), huge); out.Rows() != 7 || !strings.Contains(notice(out), "1000000") {
		t.Errorf("huge: rows %d, notices %q", out.Rows(), notice(out))
	}

	skipped := &fillPlan{skip: "Gaps were not filled: fill needs exactly one $__timeGroup"}
	if out := applyFill(gappyFrame(), skipped); out.Rows() != 7 || !strings.Contains(notice(out), "exactly one") {
		t.Errorf("skip: rows %d, notices %q", out.Rows(), notice(out))
	}
}

// Through the query path: split and unsplit, the merged frame is filled once;
// a table is never filled.
func TestQueryFillsTimeSeriesOnly(t *testing.T) {
	// Answers each request with the rows inside its $__timeFilter range, so
	// split chunks each return their part.
	rows := [][]any{{"2026-11-05T01:00:00Z", "a", 1}, {"2026-11-05T02:30:00Z", "a", 4}}
	rangeRe := regexp.MustCompile(`>= '([^']+)' AND \(time\) < '([^']+)'`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m := rangeRe.FindStringSubmatch(readSQL(r))
		out := [][]any{}
		for _, row := range rows {
			if m != nil && row[0].(string) >= m[1] && row[0].(string) < m[2] {
				out = append(out, row)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"columns": []string{"time", "host", "v"}, "data": out})
	}))
	defer srv.Close()
	inst := newTestInstance(t, srv.URL, ProtocolJSON)
	end := utc("2026-11-05T03:00:00Z")
	run := func(format, split string) *data.Frame {
		q, _ := json.Marshal(map[string]any{
			"sql":    `SELECT $__timeGroup(time, '30m', previous) AS "time", host, max(v) AS v FROM t WHERE $__timeFilter(time) GROUP BY 1, 2`,
			"format": format, "splitDuration": split,
		})
		resp := (&ArcDatasource{}).query(context.Background(), inst, backend.DataQuery{
			RefID: "A", JSON: q, TimeRange: backend.TimeRange{From: end.Add(-3 * time.Hour), To: end},
		})
		if resp.Error != nil {
			t.Fatalf("%s/%s: %v", format, split, resp.Error)
		}
		return resp.Frames[0]
	}
	for _, split := range []string{"off", "1h"} {
		f := run("time_series", split)
		if f.Rows() != 6 {
			t.Errorf("split %s: rows = %d, want 6 half-hours from 00:00", split, f.Rows())
		}
	}
	if f := run("table", "off"); f.Rows() != 2 {
		t.Errorf("table: rows = %d, want the 2 rows Arc returned", f.Rows())
	}
}

func readSQL(r *http.Request) string {
	var body struct {
		SQL string `json:"sql"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	return body.SQL
}
