package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// ArcDataSourceSettings contains Arc connection settings
type ArcDataSourceSettings struct {
	URL                   string `json:"url"`
	Database              string `json:"database"`
	Timeout               int    `json:"timeout"`               // seconds
	UseArrow              *bool  `json:"useArrow"`              // pointer so unset (fresh install) is distinguishable from explicit false
	MaxConcurrency        int    `json:"maxConcurrency"`        // max parallel chunks for query splitting (default 4)
	MaxResponseMB         int    `json:"maxResponseMB"`         // per-response body size cap in MiB (default 1024 — large analytical queries cross 256 MiB easily, R2-CR7)
	AllowPrivateIPs       bool   `json:"allowPrivateIPs"`       // opt-in: permit Arc URL to resolve to RFC1918/private addresses (corporate intranets)
	AllowDatabaseOverride bool   `json:"allowDatabaseOverride"` // opt-in: permit per-query `database` field to override the datasource default (R2-HI6 confused-deputy guard)
}

// ArcQuery represents a query to Arc
type ArcQuery struct {
	RefID         string `json:"refId"`
	SQL           string `json:"sql"`
	RawSQL        string `json:"rawSql"`        // Postgres/MySQL/MSSQL/ClickHouse compatibility
	Database      string `json:"database"`       // Per-query database override (empty = use datasource default)
	Format        string `json:"format"`         // "time_series" or "table"
	MaxDataPoints int64  `json:"maxDataPoints"`
	SplitDuration string `json:"splitDuration"`  // "auto" (default), "off", or explicit: "1h", "6h", "12h", "1d", "3d", "7d"
}

// ArcInstanceSettings is the cached, parsed view of a datasource instance.
// One is constructed per (settings, secrets) revision by newArcInstance and
// reused across every QueryData / CheckHealth call until the datasource is
// edited. The embedded *http.Client is shared so connection pooling and TLS
// session resumption work — building a fresh client per query (the pre-P3
// shape) defeated both.
//
// `sem` is a shared weighted semaphore that bounds total in-flight Arc HTTP
// requests across BOTH the refId fan-out (QueryData) and the chunk fan-out
// (inside query()) — see R2-CR1. Previously each level had its own
// errgroup.SetLimit(MaxConcurrency), so a 6-panel × 4-chunk dashboard ran
// 24 in-flight requests, not 4. The semaphore is acquired before the HTTP
// dial and released after the response is fully read.
type ArcInstanceSettings struct {
	settings         ArcDataSourceSettings
	apiKey           string
	client           *http.Client
	sem              *semaphore.Weighted
	maxResponseBytes int64 // resolved from MaxResponseMB at construction time
}

// Dispose is called by the InstanceManager when the cached instance is being
// replaced. Closes idle HTTP connections so we don't leak sockets across
// settings updates.
func (s *ArcInstanceSettings) Dispose() {
	if s.client != nil {
		if t, ok := s.client.Transport.(*http.Transport); ok {
			t.CloseIdleConnections()
		}
	}
}

// semReleasingReader wraps an io.ReadCloser so the body Close() releases the
// instance's shared concurrency semaphore. Used by doRequest so callers can
// stream-decode the body (Arrow IPC, JSON) while keeping the concurrency
// slot held for the full duration of the response read.
type semReleasingReader struct {
	io.ReadCloser
	release func()
	once    sync.Once
}

func (r *semReleasingReader) Close() error {
	err := r.ReadCloser.Close()
	r.once.Do(r.release)
	return err
}

// doRequest POSTs a JSON body to the given Arc API path and returns the
// response body wrapped in a size-cap reader and a concurrency-slot
// release-on-close. Callers MUST Close() the returned ReadCloser exactly
// once — on close the shared semaphore slot is released so other in-flight
// queries can proceed.
//
// The semaphore (R2-CR1) is acquired BEFORE the HTTP dial so both the
// refId fan-out and the chunk fan-out queue through the same per-instance
// MaxConcurrency limit — eliminating the multiplicative blow-up where
// 4×4=16 in-flight requests masqueraded as MaxConcurrency=4.
//
// Collapses the previous ~50-line duplication between queryArrow and
// queryJSON (R2-HI10).
func (s *ArcInstanceSettings) doRequest(ctx context.Context, path string, body any) (io.ReadCloser, error) {
	jsonData, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	url := s.settings.URL + path
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.apiKey)
	if s.settings.Database != "" {
		req.Header.Set("X-Arc-Database", s.settings.Database)
	}

	if err := s.sem.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	released := false
	defer func() {
		if !released {
			s.sem.Release(1)
		}
	}()

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, formatRequestError(err)
	}

	capped := http.MaxBytesReader(nil, resp.Body, s.maxResponseBytes)
	if resp.StatusCode != http.StatusOK {
		// Error bodies are small; don't read up to MaxResponseBytes (256 MiB+)
		// just to parse a JSON error message. 16 KiB covers any realistic
		// Arc error payload (gemini 3244935449).
		raw, _ := io.ReadAll(io.LimitReader(capped, 16*1024))
		_ = resp.Body.Close()
		return nil, errors.New(parseArcError(resp.StatusCode, raw))
	}

	// Transfer ownership of the semaphore slot to the returned reader —
	// release happens when the caller closes the body.
	released = true
	return &semReleasingReader{
		ReadCloser: struct {
			io.Reader
			io.Closer
		}{Reader: capped, Closer: resp.Body},
		release: func() { s.sem.Release(1) },
	}, nil
}

// ArcDatasource implements the Grafana datasource interface. The im field
// caches per-instance settings + HTTP client so QueryData does not pay the
// JSON-unmarshal-and-build-client cost on every refresh.
type ArcDatasource struct {
	im instancemgmt.InstanceManager
}

// NewArcDatasource constructs the datasource with the SDK's InstanceManager
// wired up to the newArcInstance factory.
func NewArcDatasource() *ArcDatasource {
	return &ArcDatasource{
		im: datasource.NewInstanceManager(newArcInstance),
	}
}

// newArcInstance is the SDK InstanceFactoryFunc — invoked once per (settings,
// secrets) revision. Validates the configuration, applies defaults, and
// builds the shared HTTP client. The returned value is cached by the
// InstanceManager and reused until the datasource is edited.
func newArcInstance(_ context.Context, instanceSettings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	var dsSettings ArcDataSourceSettings
	if err := json.Unmarshal(instanceSettings.JSONData, &dsSettings); err != nil {
		return nil, fmt.Errorf("failed to unmarshal settings: %w", err)
	}

	if err := validateURL(dsSettings.URL); err != nil {
		return nil, err
	}

	apiKey := strings.TrimSpace(instanceSettings.DecryptedSecureJSONData["apiKey"])
	if apiKey == "" {
		return nil, errors.New("API key is required")
	}

	if dsSettings.Timeout == 0 {
		dsSettings.Timeout = 30
	}
	if dsSettings.Database == "" {
		dsSettings.Database = "default"
	}
	if err := validateDatabaseName(dsSettings.Database); err != nil {
		return nil, err
	}
	if dsSettings.MaxConcurrency <= 0 {
		dsSettings.MaxConcurrency = 4
	}
	if dsSettings.MaxConcurrency > MaxConcurrencyCap {
		dsSettings.MaxConcurrency = MaxConcurrencyCap
	}
	// Per-response size cap (R2-CR7). 256 MiB (the original hardcoded value)
	// was too low for 6M+ row analytical queries — Arc reports "Arrow IPC
	// stream truncated after headers committed" because the plugin closes the
	// connection when the response exceeds the cap. Default to 1024 MiB
	// (fits ~30M rows with a few float64 columns), capped at MaxResponseMBCap
	// to prevent a runaway accidental config from OOMing the plugin.
	if dsSettings.MaxResponseMB <= 0 {
		dsSettings.MaxResponseMB = DefaultMaxResponseMB
	}
	if dsSettings.MaxResponseMB > MaxResponseMBCap {
		dsSettings.MaxResponseMB = MaxResponseMBCap
	}
	if dsSettings.UseArrow == nil {
		t := true
		dsSettings.UseArrow = &t
	}

	inst := &ArcInstanceSettings{
		settings:         dsSettings,
		apiKey:           apiKey,
		sem:              semaphore.NewWeighted(int64(dsSettings.MaxConcurrency)),
		maxResponseBytes: int64(dsSettings.MaxResponseMB) * 1024 * 1024,
	}
	// SSRF dial policy is two-axis (gemini 3244943519): a loopback URL only
	// unlocks loopback IPs (so a 302 redirect to `10.0.0.5` is still
	// blocked), and `AllowPrivateIPs` opens both loopback and RFC1918/CGNAT.
	policy := dialPolicy{
		allowLoopback: isLoopbackURL(dsSettings.URL),
		allowPrivate:  dsSettings.AllowPrivateIPs,
	}
	inst.client = newHTTPClient(
		time.Duration(dsSettings.Timeout)*time.Second,
		policy,
	)
	return inst, nil
}

// getInstance returns the cached *ArcInstanceSettings for this PluginContext.
// Settings are parsed and the HTTP client is built exactly once per revision
// (see newArcInstance). Errors here have either failed validation in the
// factory or come from a corrupted/replaced settings blob.
func (d *ArcDatasource) getInstance(ctx context.Context, pluginCtx backend.PluginContext) (*ArcInstanceSettings, error) {
	raw, err := d.im.Get(ctx, pluginCtx)
	if err != nil {
		return nil, err
	}
	inst, ok := raw.(*ArcInstanceSettings)
	if !ok {
		return nil, fmt.Errorf("instance manager returned unexpected type %T", raw)
	}
	return inst, nil
}

// QueryData handles query requests. RefIds within a single batch run
// concurrently; total in-flight HTTP requests are bounded by the shared
// semaphore on ArcInstanceSettings (R2-CR1 — refId × chunk fan-outs no
// longer multiply). Each refId is wrapped in a recover so a panic in one
// query fails only that query, not the whole batch (C1).
//
// The errgroup is wired with ctx (R2-HI4 / gemini 3244629509): when Grafana
// cancels the parent QueryDataRequest, the dispatch loop notices via
// gctx.Done() and stops spawning new refId goroutines rather than queueing
// MaxConcurrency more HTTP round-trips behind the SetLimit gate.
func (d *ArcDatasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	response := backend.NewQueryDataResponse()

	settings, err := d.getInstance(ctx, req.PluginContext)
	if err != nil {
		return nil, err
	}

	if len(req.Queries) <= 1 {
		for _, q := range req.Queries {
			response.Responses[q.RefID] = d.queryWithRecover(ctx, settings, q)
		}
		return response, nil
	}

	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(settings.settings.MaxConcurrency)
	for _, q := range req.Queries {
		select {
		case <-gctx.Done():
			// Parent cancelled — stop dispatching, fall through to Wait so
			// already-running refIds get to write their responses.
		default:
		}
		q := q
		g.Go(func() error {
			res := d.queryWithRecover(gctx, settings, q)
			mu.Lock()
			response.Responses[q.RefID] = res
			mu.Unlock()
			return nil
		})
	}
	// queryWithRecover never returns a non-nil error — every failure is
	// captured into the per-refId DataResponse — so g.Wait() is just a join.
	_ = g.Wait()
	return response, nil
}

// queryWithRecover wraps d.query in a recover so a panic in one refId fails
// only that refId rather than the entire batch. The full panic value plus
// stack is logged; the user-facing error is sanitized.
func (d *ArcDatasource) queryWithRecover(ctx context.Context, settings *ArcInstanceSettings, q backend.DataQuery) (resp backend.DataResponse) {
	defer func() {
		if r := recover(); r != nil {
			log.DefaultLogger.Error("panic in query handler",
				"refId", q.RefID,
				"panic", fmt.Sprintf("%v", r),
				"stack", string(debug.Stack()),
			)
			resp = backend.ErrDataResponse(backend.StatusInternal, "Query failed (internal error; see server logs).")
		}
	}()
	return d.query(ctx, settings, q)
}

// autoSplitDuration picks a split chunk size based on the query time range.
//   - < 3h  → no split (overhead not worth it)
//   - 3h–24h → 1h
//   - 1d–7d  → 6h
//   - 7d–30d → 1d
//   - > 30d  → 7d
func autoSplitDuration(tr backend.TimeRange) (time.Duration, bool) {
	span := tr.To.Sub(tr.From)
	switch {
	case span < 3*time.Hour:
		return 0, false
	case span < 24*time.Hour:
		return time.Hour, true
	case span < 7*24*time.Hour:
		return 6 * time.Hour, true
	case span < 30*24*time.Hour:
		return 24 * time.Hour, true
	default:
		return 7 * 24 * time.Hour, true
	}
}

// parseSplitDuration converts a split duration string to time.Duration.
// "auto" or "" uses autoSplitDuration; "off" disables splitting.
func parseSplitDuration(s string, tr backend.TimeRange) (time.Duration, bool) {
	if s == "off" {
		return 0, false
	}
	if s == "" || s == "auto" {
		return autoSplitDuration(tr)
	}

	switch s {
	case "1h":
		return time.Hour, true
	case "6h":
		return 6 * time.Hour, true
	case "12h":
		return 12 * time.Hour, true
	case "1d":
		return 24 * time.Hour, true
	case "3d":
		return 3 * 24 * time.Hour, true
	case "7d":
		return 7 * 24 * time.Hour, true
	default:
		return 0, false
	}
}

// splitTimeRange divides a time range into chunks aligned to epoch boundaries.
// Alignment ensures common aggregation intervals (1h, 10m, etc.) never span a
// chunk boundary, which would produce incorrect partial aggregations.
// Example with 6h chunks, range 14:30–02:30:
//   [14:30, 18:00), [18:00, 00:00), [00:00, 02:30)
// All internal boundaries land on 6h multiples from epoch.
func splitTimeRange(from, to time.Time, chunkSize time.Duration) []backend.TimeRange {
	// Truncates to whole seconds — sub-second chunk sizes are not supported,
	// but all valid split durations (1h, 6h, 1d, etc.) are well above that.
	chunkSecs := int64(chunkSize.Seconds())
	if chunkSecs <= 0 {
		return []backend.TimeRange{{From: from, To: to}}
	}

	// Find the next epoch-aligned boundary after 'from'
	fromEpoch := from.Unix()
	nextBoundary := ((fromEpoch / chunkSecs) + 1) * chunkSecs
	firstEnd := time.Unix(nextBoundary, 0).UTC()

	// If the entire range fits before the first boundary, no splitting needed
	if !firstEnd.Before(to) {
		return []backend.TimeRange{{From: from, To: to}}
	}

	var chunks []backend.TimeRange

	// First chunk: from -> first aligned boundary
	chunks = append(chunks, backend.TimeRange{From: from, To: firstEnd})

	// Middle chunks: all fully aligned
	current := firstEnd
	for {
		end := current.Add(chunkSize)
		if !end.Before(to) {
			break
		}
		chunks = append(chunks, backend.TimeRange{From: current, To: end})
		current = end
	}

	// Last chunk: last aligned boundary -> to
	if current.Before(to) {
		chunks = append(chunks, backend.TimeRange{From: current, To: to})
	}

	return chunks
}

// executeChunk runs a single query chunk against Arc
func (d *ArcDatasource) executeChunk(ctx context.Context, settings *ArcInstanceSettings, rawSQL string, chunk backend.TimeRange, originalRange backend.TimeRange) (*data.Frame, error) {
	// Apply macros with the chunk's time range for time filtering,
	// but keep the original range for $__interval calculation
	sql := ApplyMacrosWithSplit(rawSQL, chunk, originalRange)

	if *settings.settings.UseArrow {
		return queryArrow(ctx, settings, sql)
	}
	return queryJSON(ctx, settings, sql)
}

// frameSchemaCompatible returns true when `f` can be safely appended into
// `merged`: same field count AND same field type per slot. The previous
// check only compared counts, so a JSON-inference flip (chunk A typed col 2
// as float64, chunk B typed it as string) silently passed the gate and
// panicked inside the SDK's reflective Set (R2-HI2). The mismatch is now
// reported via log and the chunk is skipped.
func frameSchemaCompatible(merged, f *data.Frame) bool {
	if f == nil || len(f.Fields) != len(merged.Fields) {
		return false
	}
	for i, dst := range merged.Fields {
		if f.Fields[i].Type() != dst.Type() {
			return false
		}
	}
	return true
}

// mergeFrames appends rows from all chunk frames into a single frame.
// Skips frames with incompatible schemas (different field count OR different
// field types per slot — R2-HI2) and logs the skip so the operator can see
// the result is partial.
// Pre-allocates capacity to avoid O(n²) re-allocation from row-by-row appends.
func mergeFrames(frames []*data.Frame) *data.Frame {
	if len(frames) == 0 {
		return nil
	}
	if len(frames) == 1 {
		return frames[0]
	}

	// Find the first non-empty frame to use as the base
	var merged *data.Frame
	var startIdx int
	for i, f := range frames {
		if f != nil && len(f.Fields) > 0 {
			merged = f
			startIdx = i + 1
			break
		}
	}
	if merged == nil {
		return frames[0]
	}

	skipped := 0

	// Count total rows to add so we can pre-allocate.
	additionalRows := 0
	for _, f := range frames[startIdx:] {
		if !frameSchemaCompatible(merged, f) {
			if f != nil {
				skipped++
			}
			continue
		}
		rowLen, err := f.RowLen()
		if err != nil {
			continue
		}
		additionalRows += rowLen
	}

	if skipped > 0 {
		log.DefaultLogger.Warn("mergeFrames skipped chunks with incompatible schema",
			"skipped", skipped, "kept", len(frames)-skipped)
	}

	if additionalRows == 0 {
		return merged
	}

	// Pre-extend all fields to avoid repeated re-allocation.
	baseRows := merged.Rows()
	for _, field := range merged.Fields {
		field.Extend(additionalRows)
	}

	// Copy data using Set (single allocation, no per-row realloc).
	writeIdx := baseRows
	for _, f := range frames[startIdx:] {
		if !frameSchemaCompatible(merged, f) {
			continue
		}
		rowLen, err := f.RowLen()
		if err != nil {
			continue
		}
		for i := 0; i < rowLen; i++ {
			for fieldIdx := 0; fieldIdx < len(merged.Fields); fieldIdx++ {
				merged.Fields[fieldIdx].Set(writeIdx, f.Fields[fieldIdx].CopyAt(i))
			}
			writeIdx++
		}
	}
	return merged
}

// query executes a single query, with optional time-range splitting for large ranges
func (d *ArcDatasource) query(ctx context.Context, settings *ArcInstanceSettings, query backend.DataQuery) backend.DataResponse {
	var response backend.DataResponse

	var qm ArcQuery
	if err := json.Unmarshal(query.JSON, &qm); err != nil {
		// Sanitize: raw json error can include byte offsets and snippets of
		// the user-supplied JSON (R2-HI3).
		return backend.ErrDataResponse(backend.StatusBadRequest, sanitizeUserError(query.RefID, err))
	}

	qm.RefID = query.RefID

	// Migrate rawSql from Postgres/MySQL/MSSQL/ClickHouse datasources.
	if qm.SQL == "" && qm.RawSQL != "" {
		qm.SQL = qm.RawSQL
	}

	// Per-query database override (R2-HI6 — confused-deputy guard):
	// permitted only when the admin has opted in via AllowDatabaseOverride.
	// Otherwise a dashboard editor could switch databases on a datasource
	// the admin configured for a single tenant. The shallow-copy preserves
	// the cached *http.Client and apiKey while scoping the change to this
	// one query.
	if qm.Database != "" && qm.Database != settings.settings.Database {
		if !settings.settings.AllowDatabaseOverride {
			log.DefaultLogger.Warn("per-query database override rejected — not enabled in datasource settings",
				"refId", qm.RefID, "requested", qm.Database, "configured", settings.settings.Database)
			return backend.ErrDataResponse(backend.StatusBadRequest,
				"per-query database override is not enabled — toggle 'Allow Database Override' in datasource settings")
		}
		if err := validateDatabaseName(qm.Database); err != nil {
			// Sanitize via the user-error helper rather than echoing the raw
			// validator error (which embeds %q of the offending name) (R2-HI3).
			return backend.ErrDataResponse(backend.StatusBadRequest, sanitizeUserError(qm.RefID, err))
		}
		overridden := *settings
		overridden.settings.Database = qm.Database
		settings = &overridden
	}

	// Check if query splitting is enabled
	chunkSize, splitting := parseSplitDuration(qm.SplitDuration, query.TimeRange)

	// Compute the stripped-and-uppercased view of the SQL once and reuse it
	// across every splitting heuristic. Without this each heuristic re-ran
	// stripStringLiterals + ToUpper independently — three full-string passes
	// per query.
	stripped := newStrippedSQL(qm.SQL)

	switch {
	case splitting && !hasTimeFilterMacro(stripped):
		// No time macros (or all commented out) → nothing to split along.
		log.DefaultLogger.Debug("Skipping split for query without time filter", "refId", qm.RefID)
		splitting = false
	case splitting && containsLIMIT(stripped):
		// LIMIT applies per-chunk and would return N×chunks rows.
		log.DefaultLogger.Debug("Skipping split for query with LIMIT", "refId", qm.RefID)
		splitting = false
	case splitting && containsUnion(stripped):
		// Macro expansion in multi-statement queries produces mangled SQL.
		log.DefaultLogger.Debug("Skipping split for UNION query", "refId", qm.RefID)
		splitting = false
	case splitting && containsAggregationWithoutTimeGroup(stripped):
		// Aggregations without time bucketing span the full range; each chunk
		// aggregating independently produces wrong results (COUNT duplicated,
		// DISTINCT inflated, bare COUNT(*) returning N rows instead of 1).
		log.DefaultLogger.Debug("Skipping split for aggregation without $__timeGroup", "refId", qm.RefID)
		splitting = false
	}

	// Auto-add ORDER BY time ASC is disabled until the substring-match bug is fixed
	// (rewrites queries containing 'lifetime', 'runtime', 'timestamp' columns and
	// injects ORDER BY against a column named 'time' that may not exist).
	// Re-enable after C5 fix lands. See docs/progress/2026-05-14-signing-readiness.md.

	if !splitting {
		// No splitting — execute as before
		return d.querySingle(ctx, settings, query, qm)
	}

	// Split the time range into chunks
	chunks := splitTimeRange(query.TimeRange.From, query.TimeRange.To, chunkSize)

	log.DefaultLogger.Info("Splitting query into chunks",
		"refId", qm.RefID,
		"splitDuration", qm.SplitDuration,
		"chunks", len(chunks),
		"from", query.TimeRange.From,
		"to", query.TimeRange.To,
	)

	// Fan out chunks via errgroup.WithContext so the first error cancels
	// in-flight siblings (P9). SetLimit bounds goroutine creation rather than
	// relying on a semaphore that blocked inside already-spawned goroutines
	// (P8). With cancellation propagated through ctx, the per-chunk HTTP
	// requests see context.Canceled and unwind without finishing.
	frames := make([]*data.Frame, len(chunks))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(settings.settings.MaxConcurrency)

	for i, chunk := range chunks {
		i, chunk := i, chunk
		g.Go(func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					// Mirror queryWithRecover: log the full stack trace
					// server-side so an operator has a diagnostic trail; the
					// returned error stays brief and goes through sanitizer
					// before reaching the user (R2-HI1).
					log.DefaultLogger.Error("panic in chunk goroutine",
						"refId", qm.RefID,
						"chunk_from", chunk.From.Format("2006-01-02 15:04"),
						"chunk_to", chunk.To.Format("2006-01-02 15:04"),
						"panic", fmt.Sprintf("%v", r),
						"stack", string(debug.Stack()),
					)
					err = fmt.Errorf("[chunk %s to %s] panic: %v",
						chunk.From.Format("2006-01-02 15:04"),
						chunk.To.Format("2006-01-02 15:04"), r)
				}
			}()
			frame, runErr := d.executeChunk(gctx, settings, qm.SQL, chunk, query.TimeRange)
			if runErr != nil {
				return fmt.Errorf("[chunk %s to %s] %w",
					chunk.From.Format("2006-01-02 15:04"),
					chunk.To.Format("2006-01-02 15:04"), runErr)
			}
			frames[i] = frame
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, sanitizeUserError(qm.RefID, err))
	}

	orderedFrames := make([]*data.Frame, 0, len(chunks))
	for _, f := range frames {
		if f != nil {
			orderedFrames = append(orderedFrames, f)
		}
	}

	merged := mergeFrames(orderedFrames)
	if merged == nil {
		log.DefaultLogger.Warn("No data from split query", "refId", qm.RefID)
		return response
	}

	merged.Meta = &data.FrameMeta{
		ExecutedQueryString: qm.SQL,
		Custom: map[string]interface{}{
			"splitChunks": len(chunks),
		},
	}

	// Prepare frames (long-to-wide conversion, etc.)
	prepareStart := time.Now()
	processedFrames := prepareFrames(merged, qm)
	prepareDuration := time.Since(prepareStart)

	if len(processedFrames) == 0 {
		log.DefaultLogger.Warn("No frames after prepare", "refId", qm.RefID)
		return response
	}

	response.Frames = append(response.Frames, processedFrames...)

	log.DefaultLogger.Info("Split query completed",
		"refId", qm.RefID,
		"chunks", len(chunks),
		"totalRows", processedFrames[0].Rows(),
		"prepareDuration_ms", prepareDuration.Milliseconds(),
	)

	return response
}

// querySingle executes a query without splitting (original behavior)
func (d *ArcDatasource) querySingle(ctx context.Context, settings *ArcInstanceSettings, query backend.DataQuery, qm ArcQuery) backend.DataResponse {
	var response backend.DataResponse

	// Apply time range macros
	sql := ApplyMacros(qm.SQL, query.TimeRange)

	log.DefaultLogger.Debug("Executing Arc query",
		"refId", qm.RefID,
		"sql", sql,
		"format", qm.Format,
		"useArrow", *settings.settings.UseArrow,
	)

	var frame *data.Frame
	var err error

	if *settings.settings.UseArrow {
		frame, err = queryArrow(ctx, settings, sql)
	} else {
		frame, err = queryJSON(ctx, settings, sql)
	}

	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, sanitizeUserError(qm.RefID, err))
	}

	// Time the frame preparation (conversion)
	prepareStart := time.Now()
	processedFrames := prepareFrames(frame, qm)
	prepareDuration := time.Since(prepareStart)

	if len(processedFrames) == 0 {
		log.DefaultLogger.Warn("No frames returned from query", "refId", qm.RefID)
		return response
	}

	response.Frames = append(response.Frames, processedFrames...)

	log.DefaultLogger.Debug("Returning query response",
		"refId", qm.RefID,
		"frames", len(processedFrames),
		"rows", processedFrames[0].Rows(),
		"fields", len(processedFrames[0].Fields),
		"prepareDuration_ms", prepareDuration.Milliseconds(),
	)

	return response
}

// CheckHealth validates the datasource connection
func (d *ArcDatasource) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	var status = backend.HealthStatusOk
	var message = "Arc datasource is working"

	settings, err := d.getInstance(ctx, req.PluginContext)
	if err != nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: fmt.Sprintf("failed to get settings: %v", err),
		}, nil
	}

	// Test connection with a simple query against the production decode path,
	// so a CheckHealth pass actually proves the path real queries use.
	_, err = queryArrow(ctx, settings, "SHOW DATABASES")

	if err != nil {
		status = backend.HealthStatusError
		message = "Failed to connect to Arc: " + sanitizeUserError("health", err)
	} else {
		log.DefaultLogger.Info("Health check passed",
			"url", settings.settings.URL,
			"database", settings.settings.Database,
		)
	}

	return &backend.CheckHealthResult{
		Status:  status,
		Message: message,
	}, nil
}

func prepareFrames(frame *data.Frame, qm ArcQuery) data.Frames {
	if frame == nil {
		return nil
	}

	frame.Name = qm.RefID
	frame.RefID = qm.RefID

	if frame.Meta == nil {
		frame.Meta = &data.FrameMeta{}
	}

	switch qm.Format {
	case "table":
		frame.Meta.PreferredVisualization = data.VisTypeTable
		frame.Meta.Type = data.FrameTypeTable
		return data.Frames{frame}
	default:
		// Default to time series visualization
		frame.Meta.PreferredVisualization = data.VisTypeGraph
	}

	schema := frame.TimeSeriesSchema()

	// Handle wide format time series (already optimized, no conversion needed)
	if schema.Type == data.TimeSeriesTypeWide {
		frame.Meta.Type = data.FrameTypeTimeSeriesWide
		frame.Meta.PreferredVisualization = data.VisTypeGraph
		log.DefaultLogger.Debug("Detected wide format time series (no conversion needed)",
			"rows", frame.Rows(),
			"fields", len(frame.Fields),
		)
		return data.Frames{frame}
	}

	// Handle long format time series — convert to wide for compatibility with all
	// Grafana versions (including < v8) and existing dashboards/alerts.
	if schema.Type == data.TimeSeriesTypeLong {
		frame.Meta.Type = data.FrameTypeTimeSeriesLong

		log.DefaultLogger.Debug("Detected long format time series",
			"rows", frame.Rows(),
			"fields", len(frame.Fields),
		)

		longFrame := ensureAscendingTimes(frame, schema.TimeIndex)

		// Convert long to wide WITHOUT fill. Passing nil avoids the FillModeNull bug
		// that expanded hourly data into per-second null-filled rows (604K rows / 59MB).
		// Use $__timeGroup macro for proper time bucketing instead of date_trunc.
		wideFrame, err := data.LongToWide(longFrame, nil)
		if err != nil {
			log.DefaultLogger.Warn("LongToWide conversion failed, returning long format",
				"error", err,
			)
			longFrame.Meta.PreferredVisualization = data.VisTypeGraph
			longFrame.RefID = qm.RefID
			return data.Frames{longFrame}
		}

		log.DefaultLogger.Debug("Converted to wide format",
			"inputRows", longFrame.Rows(),
			"wideRows", wideFrame.Rows(),
			"wideFields", len(wideFrame.Fields),
		)

		if wideFrame.Meta == nil {
			wideFrame.Meta = &data.FrameMeta{}
		}
		wideFrame.Meta.PreferredVisualization = data.VisTypeGraph
		wideFrame.Meta.Type = data.FrameTypeTimeSeriesWide
		wideFrame.RefID = qm.RefID
		return data.Frames{wideFrame}
	}

	// Unknown format - return as-is
	frame.Meta.Type = data.FrameTypeUnknown

	return data.Frames{frame}
}

// ensureAscendingTimes sorts frame rows by time if needed.
// Performance: O(n) check + O(n log n) sort if unsorted (vs previous O(n²) bubble sort)
func ensureAscendingTimes(frame *data.Frame, timeIdx int) *data.Frame {
	rowLen, err := frame.RowLen()
	if err != nil || rowLen < 2 {
		return frame
	}

	// Check if data is sorted - O(n) early exit for already sorted data
	needsSorting := false
	var prevTime time.Time

	for i := 0; i < rowLen; i++ {
		currTime, ok := toTime(frame.CopyAt(timeIdx, i))
		if !ok {
			// Can't sort if we have invalid times
			return frame
		}

		if i > 0 && currTime.Before(prevTime) {
			needsSorting = true
			break
		}
		prevTime = currTime
	}

	if !needsSorting {
		return frame
	}

	log.DefaultLogger.Debug("Sorting frame by time", "rows", rowLen)

	// Create sorted frame by collecting all rows with their timestamps
	type rowWithTime struct {
		time time.Time
		data []interface{}
	}

	rows := make([]rowWithTime, rowLen)
	for i := 0; i < rowLen; i++ {
		t, _ := toTime(frame.CopyAt(timeIdx, i))
		rows[i] = rowWithTime{
			time: t,
			data: frame.RowCopy(i),
		}
	}

	// Sort by time ascending using efficient O(n log n) algorithm
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].time.Before(rows[j].time)
	})

	// Build sorted frame
	sorted := frame.EmptyCopy()
	sorted.Meta = frame.Meta
	sorted.Name = frame.Name
	sorted.RefID = frame.RefID

	for _, row := range rows {
		sorted.AppendRow(row.data...)
	}

	return sorted
}

func toTime(val interface{}) (time.Time, bool) {
	switch v := val.(type) {
	case time.Time:
		return v, true
	case *time.Time:
		if v == nil {
			return time.Time{}, false
		}
		return *v, true
	default:
		return time.Time{}, false
	}
}
