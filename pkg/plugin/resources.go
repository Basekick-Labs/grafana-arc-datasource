package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/backend/resource/httpadapter"
)

// Metadata routes for the query builder (issue #20). The browser asks the
// plugin, not Arc, so the API key never leaves the backend, and the lookups
// do not depend on the datasource's protocol: Arc's Arrow endpoint refuses
// SHOW statements, and these use Arc's HTTP metadata API instead.
//
//	GET databases                         -> {databases: [name], scoped}
//	GET measurements?database=            -> {measurements: [name]}
//	GET columns?database=&measurement=    -> {columns: [{name, type}], source, message?}
//
// Errors answer with Arc's status where it has one and a JSON body
// {error, kind}, kind being forbidden, not_found, unavailable, bad_request or
// error, so the editor can say "no permission" rather than "no data".

const (
	// resourceTimeout bounds one metadata lookup, including the wait for a
	// semaphore slot it shares with dashboard queries.
	resourceTimeout = 15 * time.Second
	// maxMetadataBody caps what a metadata answer may be read into memory.
	// Arc's listings are small; a body this large is not a listing.
	maxMetadataBody = 8 << 20

	databasesTTL    = 5 * time.Minute
	measurementsTTL = 5 * time.Minute
	columnsTTL      = time.Minute

	// describeWindow is how far back the DESCRIBE fallback looks when Arc has
	// no stored schema for a measurement. Literal bounds, not now(), so Arc's
	// partition pruning sees them and only recent files' footers are read.
	describeWindow = 24 * time.Hour
)

// measurementNameRe is Arc's query-side identifier rule
// (internal/api/query.go validIdentifierPattern). Arc refuses any other name in
// FROM, so the builder never asks about one.
var measurementNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_-]*$`)

// newResourceHandler routes CallResource requests to the metadata handlers.
func (d *ArcDatasource) newResourceHandler() backend.CallResourceHandler {
	mux := http.NewServeMux()
	mux.HandleFunc("/databases", d.handleDatabases)
	mux.HandleFunc("/measurements", d.handleMeasurements)
	mux.HandleFunc("/columns", d.handleColumns)
	return httpadapter.New(mux)
}

// CallResource implements backend.CallResourceHandler.
func (d *ArcDatasource) CallResource(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	return d.resources.CallResource(ctx, req, sender)
}

// resourceInstance resolves the instance for a resource request and the
// settings to use for the requested database: the configured one when the
// request names none, otherwise the same override gate and validation the
// query path applies (datasource.go, R2-HI6).
func (d *ArcDatasource) resourceInstance(r *http.Request) (*ArcInstanceSettings, string, error) {
	inst, err := d.getInstance(r.Context(), backend.PluginConfigFromContext(r.Context()))
	if err != nil {
		return nil, "", err
	}
	db := strings.TrimSpace(r.URL.Query().Get("database"))
	if db == "" || db == inst.settings.Database {
		return inst, inst.settings.Database, nil
	}
	if !boolOrDefault(inst.settings.AllowDatabaseOverride, true) {
		return nil, "", &resourceError{Status: http.StatusBadRequest, Kind: "bad_request",
			Message: "per-query database override is not enabled — toggle 'Allow Database Override' in datasource settings"}
	}
	if err := validateDatabaseName(db); err != nil {
		return nil, "", &resourceError{Status: http.StatusBadRequest, Kind: "bad_request", Message: err.Error()}
	}
	overridden := *inst
	overridden.settings.Database = db
	return &overridden, db, nil
}

func (d *ArcDatasource) handleDatabases(w http.ResponseWriter, r *http.Request) {
	if !requireGet(w, r) {
		return
	}
	inst, _, err := d.resourceInstance(r)
	if err != nil {
		writeResourceError(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), resourceTimeout)
	defer cancel()

	key := "databases"
	if body, ok := inst.meta.get(key); ok {
		writeJSONBytes(w, body)
		return
	}
	var resp struct {
		Databases []struct {
			Name string `json:"name"`
		} `json:"databases"`
	}
	err = getJSON(ctx, inst, "/api/v1/databases", &resp)
	var httpErr *arcHTTPError
	if errors.As(err, &httpErr) && httpErr.Status == http.StatusForbidden {
		// Listing every database needs read on all of them. A key scoped to
		// one database is normal, not an error: offer the configured one and
		// let the editor explain the short list (and accept typed names).
		writeJSON(w, map[string]any{"databases": []string{inst.settings.Database}, "scoped": true})
		return
	}
	if err != nil {
		writeResourceError(w, err)
		return
	}
	names := make([]string, 0, len(resp.Databases))
	for _, db := range resp.Databases {
		if db.Name != "" {
			names = append(names, db.Name)
		}
	}
	sort.Strings(names)
	body, _ := json.Marshal(map[string]any{"databases": names, "scoped": false})
	inst.meta.set(key, body, databasesTTL)
	writeJSONBytes(w, body)
}

func (d *ArcDatasource) handleMeasurements(w http.ResponseWriter, r *http.Request) {
	if !requireGet(w, r) {
		return
	}
	inst, db, err := d.resourceInstance(r)
	if err != nil {
		writeResourceError(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), resourceTimeout)
	defer cancel()

	key := "measurements\x00" + db
	if body, ok := inst.meta.get(key); ok {
		writeJSONBytes(w, body)
		return
	}
	var resp struct {
		Measurements []struct {
			Name string `json:"name"`
		} `json:"measurements"`
	}
	if err := getJSON(ctx, inst, "/api/v1/databases/"+url.PathEscape(db)+"/measurements", &resp); err != nil {
		writeResourceError(w, err)
		return
	}
	names := make([]string, 0, len(resp.Measurements))
	for _, m := range resp.Measurements {
		if m.Name != "" {
			names = append(names, m.Name)
		}
	}
	sort.Strings(names)
	body, _ := json.Marshal(map[string]any{"measurements": names})
	inst.meta.set(key, body, measurementsTTL)
	writeJSONBytes(w, body)
}

// columnInfo is one column of a measurement as the builder sees it.
type columnInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func (d *ArcDatasource) handleColumns(w http.ResponseWriter, r *http.Request) {
	if !requireGet(w, r) {
		return
	}
	inst, db, err := d.resourceInstance(r)
	if err != nil {
		writeResourceError(w, err)
		return
	}
	m := strings.TrimSpace(r.URL.Query().Get("measurement"))
	if !measurementNameRe.MatchString(m) {
		writeResourceError(w, &resourceError{Status: http.StatusBadRequest, Kind: "bad_request",
			Message: fmt.Sprintf("invalid measurement name %q", m)})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), resourceTimeout)
	defer cancel()

	key := "columns\x00" + db + "\x00" + m
	if body, ok := inst.meta.get(key); ok {
		writeJSONBytes(w, body)
		return
	}

	cols, err := fetchSchema(ctx, inst, db, m)
	source := "schema"
	var httpErr *arcHTTPError
	if err != nil && errors.As(err, &httpErr) && (httpErr.Status == http.StatusNotFound || httpErr.Status == http.StatusServiceUnavailable) {
		// 404: no stored schema yet (written before Arc's schema registry
		// existed, the registry is disabled, or Arc predates the route).
		// 503: the lookup failed. Either way, ask DuckDB for the columns of
		// recent data, then the registry once more: Arc builds the stored
		// schema on a measurement's first query, so the DESCRIBE itself may
		// have created it.
		descCols, descErr := describeColumns(ctx, inst, m)
		if descErr != nil {
			var dErr *arcHTTPError
			if errors.As(descErr, &dErr) && dErr.Status == http.StatusForbidden {
				writeResourceError(w, descErr)
				return
			}
			log.DefaultLogger.Debug("Column lookup: schema unavailable and DESCRIBE failed",
				"database", db, "measurement", m, "schemaError", err, "describeError", descErr)
			writeJSON(w, map[string]any{
				"columns": []columnInfo{},
				"source":  "none",
				"message": "Arc has no column list for this measurement yet. Type column names, or use Code mode.",
			})
			return
		}
		cols, source, err = descCols, "describe", nil
		if again, againErr := fetchSchema(ctx, inst, db, m); againErr == nil && len(again) > 0 {
			cols, source = again, "schema"
		}
	}
	if err != nil {
		writeResourceError(w, err)
		return
	}
	body, _ := json.Marshal(map[string]any{"columns": cols, "source": source})
	inst.meta.set(key, body, columnsTTL)
	writeJSONBytes(w, body)
}

// fetchSchema reads Arc's stored field schema for a measurement.
func fetchSchema(ctx context.Context, inst *ArcInstanceSettings, db, m string) ([]columnInfo, error) {
	var resp struct {
		Fields []columnInfo `json:"fields"`
	}
	path := "/api/v1/databases/" + url.PathEscape(db) + "/measurements/" + url.PathEscape(m) + "/schema"
	if err := getJSON(ctx, inst, path, &resp); err != nil {
		return nil, err
	}
	return resp.Fields, nil
}

// describeColumns asks DuckDB for the columns of the measurement's recent
// data. It runs on Arc's JSON query endpoint, which accepts DESCRIBE whatever
// protocol the datasource uses for queries.
func describeColumns(ctx context.Context, inst *ArcInstanceSettings, m string) ([]columnInfo, error) {
	now := time.Now().UTC()
	sql := fmt.Sprintf(`DESCRIBE SELECT * FROM "%s" WHERE time >= '%s' AND time < '%s'`,
		m, now.Add(-describeWindow).Format(time.RFC3339), now.Add(time.Minute).Format(time.RFC3339))
	body, err := inst.doRequest(ctx, "/api/v1/query", map[string]string{"sql": sql})
	if err != nil {
		return nil, err
	}
	defer func() { _ = body.Close() }()
	var resp struct {
		Columns []string `json:"columns"`
		Data    [][]any  `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(body, maxMetadataBody)).Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode DESCRIBE response: %w", err)
	}
	nameIdx, typeIdx := -1, -1
	for i, c := range resp.Columns {
		switch c {
		case "column_name":
			nameIdx = i
		case "column_type":
			typeIdx = i
		}
	}
	if nameIdx < 0 || typeIdx < 0 {
		return nil, fmt.Errorf("DESCRIBE response has no column_name/column_type (columns %v)", resp.Columns)
	}
	cols := make([]columnInfo, 0, len(resp.Data))
	for _, row := range resp.Data {
		if nameIdx >= len(row) || typeIdx >= len(row) {
			continue
		}
		name, _ := row[nameIdx].(string)
		typ, _ := row[typeIdx].(string)
		if name != "" {
			cols = append(cols, columnInfo{Name: name, Type: typ})
		}
	}
	if len(cols) == 0 {
		return nil, errors.New("DESCRIBE returned no columns")
	}
	// DESCRIBE lists columns in no particular order (Arc unions files by
	// name). Arc's stored schema puts time first and sorts the rest, so do
	// the same and the editor sees one shape whichever source answered.
	sort.SliceStable(cols, func(i, j int) bool {
		if (cols[i].Name == "time") != (cols[j].Name == "time") {
			return cols[i].Name == "time"
		}
		return cols[i].Name < cols[j].Name
	})
	return cols, nil
}

// getJSON GETs an Arc API path and decodes the JSON body into out.
func getJSON(ctx context.Context, inst *ArcInstanceSettings, path string, out any) error {
	body, err := inst.doGet(ctx, path)
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()
	if err := json.NewDecoder(io.LimitReader(body, maxMetadataBody)).Decode(out); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

// resourceError is a metadata-route failure the plugin itself decided on.
type resourceError struct {
	Status  int
	Kind    string
	Message string
}

func (e *resourceError) Error() string { return e.Message }

// writeResourceError maps err to a status and the {error, kind} body.
func writeResourceError(w http.ResponseWriter, err error) {
	status, kind := http.StatusBadGateway, "error"
	msg := err.Error()
	var rErr *resourceError
	var httpErr *arcHTTPError
	switch {
	case errors.As(err, &rErr):
		status, kind, msg = rErr.Status, rErr.Kind, rErr.Message
	case errors.As(err, &httpErr):
		switch httpErr.Status {
		case http.StatusUnauthorized, http.StatusForbidden:
			status, kind = http.StatusForbidden, "forbidden"
		case http.StatusNotFound:
			status, kind = http.StatusNotFound, "not_found"
		case http.StatusServiceUnavailable:
			status, kind = http.StatusServiceUnavailable, "unavailable"
		}
	case errors.Is(err, context.DeadlineExceeded):
		status, kind, msg = http.StatusGatewayTimeout, "unavailable", "Arc did not answer the metadata lookup in time"
	case errors.Is(err, context.Canceled):
		status, kind, msg = 499, "error", "request cancelled"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg, "kind": kind})
}

func requireGet(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet {
		return true
	}
	writeResourceError(w, &resourceError{Status: http.StatusMethodNotAllowed, Kind: "bad_request", Message: "method not allowed"})
	return false
}

func writeJSON(w http.ResponseWriter, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		writeResourceError(w, err)
		return
	}
	writeJSONBytes(w, body)
}

func writeJSONBytes(w http.ResponseWriter, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// metaCache holds encoded metadata answers per datasource instance. One
// instance has one API key, so an entry can never cross a permission
// boundary; keys carry the effective database, so overrides cannot either.
// A nil cache (tests that build settings by hand) caches nothing.
type metaCache struct {
	mu      sync.Mutex
	entries map[string]metaEntry
}

type metaEntry struct {
	body    []byte
	expires time.Time
}

// maxMetaEntries bounds the cache. Past it, expired entries are dropped and,
// if that is not enough, everything: cheaper than LRU bookkeeping for data
// that is re-fetched in one call.
const maxMetaEntries = 1024

func newMetaCache() *metaCache { return &metaCache{entries: map[string]metaEntry{}} }

func (c *metaCache) get(key string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || time.Now().After(e.expires) {
		return nil, false
	}
	return e.body, true
}

func (c *metaCache) set(key string, body []byte, ttl time.Duration) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= maxMetaEntries {
		now := time.Now()
		for k, e := range c.entries {
			if now.After(e.expires) {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= maxMetaEntries {
			c.entries = map[string]metaEntry{}
		}
	}
	c.entries[key] = metaEntry{body: body, expires: time.Now().Add(ttl)}
}
