package plugin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

// Metadata routes for the query builder (issue #20), against a fake Arc.

// fakeArc answers the metadata API. Handlers per path; every request is
// recorded with its X-Arc-Database header.
type fakeArc struct {
	mu       sync.Mutex
	handlers map[string]http.HandlerFunc
	calls    []string
}

func newFakeArc(t *testing.T) (*fakeArc, *httptest.Server) {
	t.Helper()
	f := &fakeArc{handlers: map[string]http.HandlerFunc{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("%s %s: Authorization = %q", r.Method, r.URL.Path, got)
		}
		f.mu.Lock()
		f.calls = append(f.calls, r.Method+" "+r.URL.EscapedPath()+" db="+r.Header.Get("X-Arc-Database"))
		h := f.handlers[r.Method+" "+r.URL.EscapedPath()]
		f.mu.Unlock()
		if h == nil {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"no such route in fakeArc"}`))
			return
		}
		h(w, r)
	}))
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeArc) on(methodPath string, h http.HandlerFunc) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handlers[methodPath] = h
}

func (f *fakeArc) callCount(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}

func jsonReply(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

// resourceCall runs one CallResource through a fresh datasource bound to srv.
type resourceResult struct {
	status int
	body   map[string]any
}

func callResource(t *testing.T, d *ArcDatasource, pc backend.PluginContext, method, path string) resourceResult {
	t.Helper()
	p, _, _ := strings.Cut(path, "?")
	var res resourceResult
	err := d.CallResource(context.Background(), &backend.CallResourceRequest{
		PluginContext: pc, Method: method, Path: p, URL: path,
	}, backend.CallResourceResponseSenderFunc(func(r *backend.CallResourceResponse) error {
		res.status = r.Status
		if len(r.Body) > 0 {
			if err := json.Unmarshal(r.Body, &res.body); err != nil {
				t.Fatalf("%s: body is not JSON: %s", path, r.Body)
			}
		}
		return nil
	}))
	if err != nil {
		t.Fatalf("CallResource(%s): %v", path, err)
	}
	return res
}

func pluginCtxFor(t *testing.T, url string, extra map[string]any) backend.PluginContext {
	t.Helper()
	settings := map[string]any{"url": url, "database": "metrics", "protocol": "json"}
	for k, v := range extra {
		settings[k] = v
	}
	raw, _ := json.Marshal(settings)
	return backend.PluginContext{DataSourceInstanceSettings: &backend.DataSourceInstanceSettings{
		UID: t.Name(), JSONData: raw, DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
	}}
}

func strList(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		s, _ := it.(string)
		out = append(out, s)
	}
	return out
}

func TestResourceDatabasesSortedAndCached(t *testing.T) {
	f, srv := newFakeArc(t)
	f.on("GET /api/v1/databases", jsonReply(200, `{"databases":[{"name":"zeta","measurement_count":1},{"name":"alpha","measurement_count":3}],"count":2}`))
	d, pc := NewArcDatasource(), pluginCtxFor(t, srv.URL, nil)

	res := callResource(t, d, pc, "GET", "databases")
	if res.status != 200 || strings.Join(strList(res.body["databases"]), ",") != "alpha,zeta" || res.body["scoped"] != false {
		t.Fatalf("databases = %d %v", res.status, res.body)
	}
	callResource(t, d, pc, "GET", "databases")
	if n := f.callCount("GET /api/v1/databases"); n != 1 {
		t.Errorf("Arc was asked %d times for databases; the second lookup should come from the cache", n)
	}
}

// A key scoped to one database cannot list them all. That is a normal
// deployment, so the route offers the configured database instead of failing.
func TestResourceDatabasesScopedKeyFallsBackToConfigured(t *testing.T) {
	f, srv := newFakeArc(t)
	f.on("GET /api/v1/databases", jsonReply(403, `{"success":false,"error":"Permission denied"}`))
	res := callResource(t, NewArcDatasource(), pluginCtxFor(t, srv.URL, nil), "GET", "databases")
	if res.status != 200 || strings.Join(strList(res.body["databases"]), ",") != "metrics" || res.body["scoped"] != true {
		t.Fatalf("scoped fallback = %d %v", res.status, res.body)
	}
}

func TestResourceMeasurementsUsesRequestedDatabase(t *testing.T) {
	f, srv := newFakeArc(t)
	f.on("GET /api/v1/databases/metrics/measurements", jsonReply(200, `{"database":"metrics","measurements":[{"name":"mem"},{"name":"cpu"}],"count":2}`))
	f.on("GET /api/v1/databases/other-db/measurements", jsonReply(200, `{"database":"other-db","measurements":[{"name":"disk-io"}],"count":1}`))
	d, pc := NewArcDatasource(), pluginCtxFor(t, srv.URL, nil)

	res := callResource(t, d, pc, "GET", "measurements")
	if res.status != 200 || strings.Join(strList(res.body["measurements"]), ",") != "cpu,mem" {
		t.Fatalf("default database measurements = %d %v", res.status, res.body)
	}
	res = callResource(t, d, pc, "GET", "measurements?database=other-db")
	if res.status != 200 || strings.Join(strList(res.body["measurements"]), ",") != "disk-io" {
		t.Fatalf("override database measurements = %d %v", res.status, res.body)
	}
	// The override reached Arc as the database header too, and the cache is
	// keyed per database: each database was fetched once.
	if f.callCount("GET /api/v1/databases/other-db/measurements db=other-db") != 1 {
		t.Errorf("override request: calls %v", f.calls)
	}
	callResource(t, d, pc, "GET", "measurements?database=other-db")
	callResource(t, d, pc, "GET", "measurements")
	if f.callCount("GET /api/v1/databases/") != 2 {
		t.Errorf("cache missed: calls %v", f.calls)
	}
}

func TestResourceDatabaseOverrideRules(t *testing.T) {
	_, srv := newFakeArc(t)
	off := pluginCtxFor(t, srv.URL, map[string]any{"allowDatabaseOverride": false})
	res := callResource(t, NewArcDatasource(), off, "GET", "measurements?database=other")
	if res.status != 400 || res.body["kind"] != "bad_request" || !strings.Contains(res.body["error"].(string), "Allow Database Override") {
		t.Fatalf("override disabled = %d %v", res.status, res.body)
	}
	// The configured database itself is not an override.
	f2, srv2 := newFakeArc(t)
	f2.on("GET /api/v1/databases/metrics/measurements", jsonReply(200, `{"measurements":[]}`))
	pc := pluginCtxFor(t, srv2.URL, map[string]any{"allowDatabaseOverride": false})
	if res := callResource(t, NewArcDatasource(), pc, "GET", "measurements?database=metrics"); res.status != 200 {
		t.Fatalf("configured database with override off = %d %v", res.status, res.body)
	}
	for _, bad := range []string{"a%2Fb", "x%20y", "..", "%27or%271"} {
		res := callResource(t, NewArcDatasource(), pluginCtxFor(t, srv.URL, nil), "GET", "measurements?database="+bad)
		if res.status != 400 || res.body["kind"] != "bad_request" {
			t.Errorf("database %q = %d %v, want 400 bad_request", bad, res.status, res.body)
		}
	}
}

func TestResourceErrorKinds(t *testing.T) {
	for _, c := range []struct {
		arcStatus int
		status    int
		kind      string
	}{
		{401, 403, "forbidden"}, {403, 403, "forbidden"}, {404, 404, "not_found"},
		{503, 503, "unavailable"}, {500, 502, "error"},
	} {
		f, srv := newFakeArc(t)
		f.on("GET /api/v1/databases/metrics/measurements", jsonReply(c.arcStatus, `{"error":"arc says no"}`))
		res := callResource(t, NewArcDatasource(), pluginCtxFor(t, srv.URL, nil), "GET", "measurements")
		if res.status != c.status || res.body["kind"] != c.kind || !strings.Contains(res.body["error"].(string), "arc says no") {
			t.Errorf("Arc %d -> %d %v, want %d %s with Arc's message", c.arcStatus, res.status, res.body, c.status, c.kind)
		}
	}
}

func TestResourceRejectsNonGet(t *testing.T) {
	_, srv := newFakeArc(t)
	res := callResource(t, NewArcDatasource(), pluginCtxFor(t, srv.URL, nil), "POST", "databases")
	if res.status != 405 {
		t.Fatalf("POST databases = %d", res.status)
	}
}

// DESCRIBE's column order is whatever the union of files gives: here time is
// not first, as on a live Arc.
const describeReply = `{"success":true,"columns":["column_name","column_type","null","key","default","extra"],` +
	`"data":[["usage_idle","DOUBLE","YES",null,null,null],["time","TIMESTAMP WITH TIME ZONE","YES",null,null,null],["host","VARCHAR","YES",null,null,null]],"row_count":3}`

func TestResourceColumnsFromSchema(t *testing.T) {
	f, srv := newFakeArc(t)
	f.on("GET /api/v1/databases/metrics/measurements/cpu/schema", jsonReply(200,
		`{"database":"metrics","measurement":"cpu","fields":[{"name":"time","type":"TIMESTAMP WITH TIME ZONE"},{"name":"host","type":"VARCHAR"}]}`))
	res := callResource(t, NewArcDatasource(), pluginCtxFor(t, srv.URL, nil), "GET", "columns?measurement=cpu")
	cols, _ := res.body["columns"].([]any)
	if res.status != 200 || res.body["source"] != "schema" || len(cols) != 2 {
		t.Fatalf("columns = %d %v", res.status, res.body)
	}
	if f.callCount("POST /api/v1/query") != 0 {
		t.Errorf("a stored schema must not trigger DESCRIBE: %v", f.calls)
	}
}

// No stored schema: DESCRIBE over recent data with literal time bounds (so
// Arc can prune partitions), then the registry once more.
func TestResourceColumnsDescribeFallback(t *testing.T) {
	for _, schemaStatus := range []int{404, 503} {
		f, srv := newFakeArc(t)
		f.on("GET /api/v1/databases/metrics/measurements/disk-io/schema", jsonReply(schemaStatus, `{"error":"no schema"}`))
		var describeSQL atomic.Value
		f.on("POST /api/v1/query", func(w http.ResponseWriter, r *http.Request) {
			var req struct{ SQL string }
			_ = json.NewDecoder(r.Body).Decode(&req)
			describeSQL.Store(req.SQL)
			jsonReply(200, describeReply)(w, r)
		})
		res := callResource(t, NewArcDatasource(), pluginCtxFor(t, srv.URL, nil), "GET", "columns?measurement=disk-io")
		cols, _ := res.body["columns"].([]any)
		if res.status != 200 || res.body["source"] != "describe" || len(cols) != 3 {
			t.Fatalf("schema %d: columns = %d %v", schemaStatus, res.status, res.body)
		}
		var names []string
		for _, c := range cols {
			names = append(names, c.(map[string]any)["name"].(string))
		}
		if strings.Join(names, ",") != "time,host,usage_idle" {
			t.Errorf("DESCRIBE columns in order %v, want time first then by name", names)
		}
		sql, _ := describeSQL.Load().(string)
		if !strings.HasPrefix(sql, `DESCRIBE SELECT * FROM "disk-io" WHERE time >= '`) || strings.Contains(sql, "now()") {
			t.Errorf("DESCRIBE SQL = %q; want literal bounds on a quoted measurement", sql)
		}
		if n := f.callCount("GET /api/v1/databases/metrics/measurements/disk-io/schema"); n != 2 {
			t.Errorf("schema asked %d times, want 2 (before and after DESCRIBE)", n)
		}
	}
}

// Arc builds the stored schema on a measurement's first query, so the
// DESCRIBE may create it; the retry then answers from the registry.
func TestResourceColumnsSchemaAppearsAfterDescribe(t *testing.T) {
	f, srv := newFakeArc(t)
	var schemaCalls atomic.Int32
	f.on("GET /api/v1/databases/metrics/measurements/cpu/schema", func(w http.ResponseWriter, r *http.Request) {
		if schemaCalls.Add(1) == 1 {
			jsonReply(404, `{"error":"no anchor"}`)(w, r)
			return
		}
		jsonReply(200, `{"fields":[{"name":"time","type":"TIMESTAMP WITH TIME ZONE"},{"name":"host","type":"VARCHAR"},{"name":"v","type":"BIGINT"},{"name":"ok","type":"BOOLEAN"}]}`)(w, r)
	})
	f.on("POST /api/v1/query", jsonReply(200, describeReply))
	res := callResource(t, NewArcDatasource(), pluginCtxFor(t, srv.URL, nil), "GET", "columns?measurement=cpu")
	cols, _ := res.body["columns"].([]any)
	if res.status != 200 || res.body["source"] != "schema" || len(cols) != 4 {
		t.Fatalf("columns = %d %v", res.status, res.body)
	}
}

func TestResourceColumnsDescribeFailures(t *testing.T) {
	// No permission to query the measurement: say so.
	f, srv := newFakeArc(t)
	f.on("GET /api/v1/databases/metrics/measurements/cpu/schema", jsonReply(404, `{"error":"no anchor"}`))
	f.on("POST /api/v1/query", jsonReply(403, `{"error":"Permission denied"}`))
	res := callResource(t, NewArcDatasource(), pluginCtxFor(t, srv.URL, nil), "GET", "columns?measurement=cpu")
	if res.status != 403 || res.body["kind"] != "forbidden" {
		t.Fatalf("DESCRIBE 403 = %d %v", res.status, res.body)
	}
	// Any other failure: no columns, but not an error the editor must block
	// on. The builder accepts typed names.
	f2, srv2 := newFakeArc(t)
	f2.on("GET /api/v1/databases/metrics/measurements/cpu/schema", jsonReply(404, `{"error":"no anchor"}`))
	f2.on("POST /api/v1/query", jsonReply(500, `{"error":"boom"}`))
	res = callResource(t, NewArcDatasource(), pluginCtxFor(t, srv2.URL, nil), "GET", "columns?measurement=cpu")
	if res.status != 200 || res.body["source"] != "none" || res.body["message"] == nil {
		t.Fatalf("DESCRIBE 500 = %d %v", res.status, res.body)
	}
	// A schema forbidden is passed through, without trying DESCRIBE.
	f3, srv3 := newFakeArc(t)
	f3.on("GET /api/v1/databases/metrics/measurements/cpu/schema", jsonReply(403, `{"error":"denied"}`))
	res = callResource(t, NewArcDatasource(), pluginCtxFor(t, srv3.URL, nil), "GET", "columns?measurement=cpu")
	if res.status != 403 || f3.callCount("POST") != 0 {
		t.Fatalf("schema 403 = %d %v, calls %v", res.status, res.body, f3.calls)
	}
}

func TestResourceColumnsValidatesMeasurement(t *testing.T) {
	_, srv := newFakeArc(t)
	for _, m := range []string{"", "a.b", "a%2Fb", "1cpu", "x%22y", "cpu%3B"} {
		res := callResource(t, NewArcDatasource(), pluginCtxFor(t, srv.URL, nil), "GET", "columns?measurement="+m)
		if res.status != 400 || res.body["kind"] != "bad_request" {
			t.Errorf("measurement %q = %d %v, want 400", m, res.status, res.body)
		}
	}
}

// The per-query database override copies the settings struct. The cache is a
// pointer, so the copy shares it rather than copying a mutex (go vet copylocks)
// or starting empty.
func TestMetaCacheSurvivesSettingsCopy(t *testing.T) {
	inst := &ArcInstanceSettings{meta: newMetaCache()}
	copied := *inst
	copied.meta.set("k", []byte("v"), columnsTTL)
	if got, ok := inst.meta.get("k"); !ok || string(got) != "v" {
		t.Fatalf("cache not shared across settings copy: %q %v", got, ok)
	}
	var nilCache *metaCache
	nilCache.set("k", []byte("v"), columnsTTL)
	if _, ok := nilCache.get("k"); ok {
		t.Fatal("a nil cache must cache nothing")
	}
}
