package plugin

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

// A window function reads across the whole range, so a query using one is
// never split, $__timeGroup or not. Before, $__timeGroup ended the check and
// lag() restarted at every chunk edge.
func TestWindowFunctionWithTimeGroupIsNotSplit(t *testing.T) {
	for _, c := range []struct {
		name, sql string
		split     bool
	}{
		{"named window", `SELECT $__timeGroup(time, '1m') AS "time", avg(v) - lag(avg(v)) OVER w AS d FROM t WHERE $__timeFilter(time) GROUP BY 1 WINDOW w AS (ORDER BY 1)`, false},
		{"inline window", `SELECT $__timeGroup(time, '1m') AS "time", sum(avg(v)) OVER (ORDER BY 1) AS s FROM t WHERE $__timeFilter(time) GROUP BY 1`, false},
		{"CTE", "WITH b AS (SELECT $__timeGroup(time, '1m') AS t, max(v) AS v FROM m WHERE $__timeFilter(time) GROUP BY 1)\nSELECT t, v - lag(v) OVER (ORDER BY t) FROM b", false},
		// A builder transformation whose buckets are wider than a chunk: the
		// bucket rule would refuse the split too, and the editor must still
		// learn the reason that always applies.
		{"builder transformation", "WITH arc_builder_buckets AS (SELECT $__timeGroup(time, '2h') AS \"__t\", max(v) AS \"__v0\" FROM m WHERE $__timeFilterLookback(time, '2h', 1) GROUP BY 1)\nSELECT \"__t\" AS \"time\", \"__v0\" - lag(\"__v0\") OVER w AS d FROM arc_builder_buckets WINDOW w AS (ORDER BY \"__t\") QUALIFY \"__t\" >= $__timeGroupStart('2h')", false},
		{"control: no window", `SELECT $__timeGroup(time, '1m') AS "time", avg(v) FROM t WHERE $__timeFilter(time) GROUP BY 1`, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"columns":["time","v"],"data":[["2026-11-05T01:00:00Z",1]]}`)
			}))
			defer srv.Close()
			inst := newTestInstance(t, srv.URL, ProtocolJSON)
			q, _ := json.Marshal(map[string]any{"sql": c.sql, "splitDuration": "1h", "format": "table"})
			end := time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC)
			resp := (&ArcDatasource{}).query(context.Background(), inst, backend.DataQuery{
				RefID: "A", JSON: q, TimeRange: backend.TimeRange{From: end.Add(-3 * time.Hour), To: end},
			})
			if resp.Error != nil {
				t.Fatalf("query: %v", resp.Error)
			}
			if got := requests.Load(); (got > 1) != c.split {
				t.Fatalf("requests = %d, want split = %v", got, c.split)
			}
			custom, _ := resp.Frames[0].Meta.Custom.(map[string]interface{})
			if !c.split && custom["splitSkipped"] != "window function" {
				t.Errorf("meta custom = %v, want splitSkipped", custom)
			}
		})
	}
}

// Arc answers SHOW only on its JSON endpoint; Arrow, the default protocol,
// refuses it. SHOW goes to /api/v1/query whatever the protocol.
func TestShowStatementsUseTheJSONEndpoint(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"columns":["database"],"data":[["matrix"]]}`)
	}))
	defer srv.Close()
	for _, proto := range []string{ProtocolArrow, ProtocolMsgpack, ProtocolJSON} {
		inst := newTestInstance(t, srv.URL, proto)
		for _, sql := range []string{"SHOW DATABASES", "  show tables from matrix;", "SHOW MEASUREMENTS"} {
			mu.Lock()
			paths = nil
			mu.Unlock()
			if _, err := executeProtocolQuery(context.Background(), inst, sql); err != nil {
				t.Fatalf("%s %q: %v", proto, sql, err)
			}
			if len(paths) != 1 || paths[0] != "/api/v1/query" {
				t.Errorf("%s %q went to %v, want /api/v1/query", proto, sql, paths)
			}
		}
	}
	for _, sql := range []string{"SELECT 1", "SELECT 'SHOW DATABASES'", "-- SHOW DATABASES\nSELECT 1", "SHOWCASE"} {
		if showStatementRe.MatchString(sql) {
			t.Errorf("%q is not a SHOW statement", sql)
		}
	}
}
