package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// ArcDataSourceSettings contains Arc connection settings
type ArcDataSourceSettings struct {
	URL      string `json:"url"`
	Database string `json:"database"`
	Timeout  int    `json:"timeout"` // seconds
	UseArrow bool   `json:"useArrow"`
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

// ArcInstanceSettings holds per-instance settings
type ArcInstanceSettings struct {
	settings ArcDataSourceSettings
	apiKey   string
}

// ArcDatasource implements the Grafana datasource interface
type ArcDatasource struct{}

// NewArcDatasource creates a new datasource
func NewArcDatasource() *ArcDatasource {
	return &ArcDatasource{}
}

// getSettings extracts settings from plugin context
func getSettings(ctx context.Context, pluginCtx backend.PluginContext) (*ArcInstanceSettings, error) {
	var dsSettings ArcDataSourceSettings

	// Parse settings
	if err := json.Unmarshal(pluginCtx.DataSourceInstanceSettings.JSONData, &dsSettings); err != nil {
		return nil, fmt.Errorf("failed to unmarshal settings: %w", err)
	}

	// Get API key from secure settings
	apiKey := pluginCtx.DataSourceInstanceSettings.DecryptedSecureJSONData["apiKey"]
	if apiKey == "" {
		return nil, fmt.Errorf("API key is required")
	}

	// Default values
	if dsSettings.Timeout == 0 {
		dsSettings.Timeout = 30
	}
	if dsSettings.Database == "" {
		dsSettings.Database = "default"
	}
	// Note: UseArrow defaults to false in Go struct initialization
	// The frontend defaults to true in the UI (ConfigEditor.tsx line 145)
	// This ensures the toggle actually works - if explicitly set to false, respect that choice

	return &ArcInstanceSettings{
		settings: dsSettings,
		apiKey:   apiKey,
	}, nil
}

// QueryData handles query requests
func (d *ArcDatasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	response := backend.NewQueryDataResponse()

	// Get settings
	settings, err := getSettings(ctx, req.PluginContext)
	if err != nil {
		return nil, err
	}

	// Process each query
	for _, q := range req.Queries {
		res := d.query(ctx, settings, q)
		response.Responses[q.RefID] = res
	}

	return response, nil
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

	if settings.settings.UseArrow {
		return QueryArrowFlightSQLStyle(ctx, settings, sql, chunk)
	}
	return QueryJSON(ctx, settings, sql, chunk)
}

// mergeFrames appends rows from all chunk frames into a single frame.
// Skips frames with incompatible schemas (different field count) to avoid panics.
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

	expectedFields := len(merged.Fields)

	// Count total rows to add so we can pre-allocate
	additionalRows := 0
	for _, f := range frames[startIdx:] {
		if f == nil || len(f.Fields) != expectedFields {
			continue
		}
		rowLen, err := f.RowLen()
		if err != nil {
			continue
		}
		additionalRows += rowLen
	}

	if additionalRows == 0 {
		return merged
	}

	// Pre-extend all fields to avoid repeated re-allocation
	baseRows := merged.Rows()
	for _, field := range merged.Fields {
		field.Extend(additionalRows)
	}

	// Copy data using Set (single allocation, no per-row realloc)
	writeIdx := baseRows
	for _, f := range frames[startIdx:] {
		if f == nil || len(f.Fields) != expectedFields {
			continue
		}
		rowLen, err := f.RowLen()
		if err != nil {
			continue
		}
		for i := 0; i < rowLen; i++ {
			for fieldIdx := 0; fieldIdx < expectedFields; fieldIdx++ {
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

	// Parse query model
	var qm ArcQuery
	if err := json.Unmarshal(query.JSON, &qm); err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("failed to unmarshal query: %v", err))
	}

	qm.RefID = query.RefID

	// Migrate rawSql from Postgres/MySQL/MSSQL/ClickHouse datasources
	if qm.SQL == "" && qm.RawSQL != "" {
		qm.SQL = qm.RawSQL
	}

	// Per-query database override
	if qm.Database != "" {
		overridden := *settings
		overridden.settings.Database = qm.Database
		settings = &overridden
	}

	// Check if query splitting is enabled
	chunkSize, splitting := parseSplitDuration(qm.SplitDuration, query.TimeRange)

	// Skip splitting for queries with LIMIT — LIMIT applies per-chunk and would
	// return N×chunks rows instead of N rows.
	if splitting && containsLIMIT(qm.SQL) {
		log.DefaultLogger.Debug("Skipping split for query with LIMIT", "refId", qm.RefID)
		splitting = false
	}

	// Skip splitting for queries with aggregation but no $__timeGroup — aggregations
	// without time bucketing span the full range and produce wrong results when
	// each chunk aggregates independently (e.g. COUNT per status gets duplicated,
	// DISTINCT returns duplicates, bare COUNT(*) returns N rows instead of 1).
	if splitting && containsAggregationWithoutTimeGroup(qm.SQL) {
		log.DefaultLogger.Debug("Skipping split for aggregation without $__timeGroup", "refId", qm.RefID)
		splitting = false
	}

	// Skip splitting for queries without $__timeFilter — the query doesn't use
	// the time range at all, so splitting would just run it N times.
	if splitting && !strings.Contains(qm.SQL, "$__timeFilter") && !strings.Contains(qm.SQL, "$__timeFrom") {
		log.DefaultLogger.Debug("Skipping split for query without time filter", "refId", qm.RefID)
		splitting = false
	}

	// Skip splitting for UNION queries — macro expansion in multi-statement
	// queries produces mangled SQL.
	if splitting {
		upper := strings.ToUpper(qm.SQL)
		if strings.Contains(upper, " UNION ") {
			log.DefaultLogger.Debug("Skipping split for UNION query", "refId", qm.RefID)
			splitting = false
		}
	}

	// Auto-add ORDER BY time ASC for time series queries without one
	if qm.Format == "time_series" {
		qm.SQL = OptimizeTimeSeriesQuery(qm.SQL)
	}

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

	// Execute chunks in parallel
	type chunkResult struct {
		index int
		frame *data.Frame
		err   error
	}

	results := make([]chunkResult, len(chunks))
	var wg sync.WaitGroup

	// Limit concurrency to avoid overwhelming Arc under multi-user load
	semaphore := make(chan struct{}, 4)

	for i, chunk := range chunks {
		wg.Add(1)
		go func(idx int, ch backend.TimeRange) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					results[idx] = chunkResult{
						index: idx,
						err:   fmt.Errorf("[chunk %s to %s] panic: %v", ch.From.Format("2006-01-02 15:04"), ch.To.Format("2006-01-02 15:04"), r),
					}
				}
			}()
			select {
			case semaphore <- struct{}{}:
			case <-ctx.Done():
				results[idx] = chunkResult{index: idx, err: ctx.Err()}
				return
			}
			defer func() { <-semaphore }()

			frame, err := d.executeChunk(ctx, settings, qm.SQL, ch, query.TimeRange)
			if err != nil {
				err = fmt.Errorf("[chunk %s to %s] %w",
					ch.From.Format("2006-01-02 15:04"),
					ch.To.Format("2006-01-02 15:04"),
					err)
			}
			results[idx] = chunkResult{index: idx, frame: frame, err: err}
		}(i, chunk)
	}

	wg.Wait()

	// Collect frames in order, fail on first error
	orderedFrames := make([]*data.Frame, 0, len(chunks))
	for _, r := range results {
		if r.err != nil {
			return backend.ErrDataResponse(backend.StatusInternal, r.err.Error())
		}
		if r.frame != nil {
			orderedFrames = append(orderedFrames, r.frame)
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
		"useArrow", settings.settings.UseArrow,
	)

	// Execute query based on protocol
	var frame *data.Frame
	var err error

	if settings.settings.UseArrow {
		frame, err = QueryArrowFlightSQLStyle(ctx, settings, sql, query.TimeRange)
	} else {
		frame, err = QueryJSON(ctx, settings, sql, query.TimeRange)
	}

	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, err.Error())
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

	// Get settings
	settings, err := getSettings(ctx, req.PluginContext)
	if err != nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: fmt.Sprintf("failed to get settings: %v", err),
		}, nil
	}

	// Test connection with simple query
	testSQL := "SHOW DATABASES"
	_, err = QueryArrow(ctx, settings, testSQL, backend.TimeRange{
		From: time.Now().Add(-1 * time.Hour),
		To:   time.Now(),
	})

	if err != nil {
		status = backend.HealthStatusError
		message = fmt.Sprintf("Failed to connect to Arc: %v", err)
		log.DefaultLogger.Error("Health check failed", "error", err)
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

// containsLIMIT checks if SQL contains a LIMIT clause (case-insensitive).
func containsLIMIT(sql string) bool {
	return strings.Contains(strings.ToUpper(sql), " LIMIT ")
}

// containsAggregationWithoutTimeGroup returns true if the SQL has aggregation
// (GROUP BY, DISTINCT, or aggregate functions) but no $__timeGroup macro.
// Such queries aggregate across the full time range and would produce wrong
// results if split into chunks (duplicated groups, inflated counts, etc.).
// Note: this is a best-effort heuristic — it can false-positive on keywords
// inside string literals or comments, but errs on the safe side (skipping
// splitting is always correct, just slower).
func containsAggregationWithoutTimeGroup(sql string) bool {
	if strings.Contains(sql, "$__timeGroup") {
		return false
	}
	upper := strings.ToUpper(sql)
	if strings.Contains(upper, "GROUP BY") {
		return true
	}
	// Match "DISTINCT " (with trailing space) or "DISTINCT(" to catch both
	// SELECT DISTINCT col and functions like APPROX_COUNT_DISTINCT(col).
	// Avoids false positives on values like 'DISTINCT_VALUE' in string literals.
	if strings.Contains(upper, "DISTINCT ") || strings.Contains(upper, "DISTINCT(") || strings.HasSuffix(upper, "DISTINCT") {
		return true
	}
	// Standard SQL + DuckDB aggregate functions (complete list from DuckDB docs)
	for _, fn := range []string{
		// General aggregates
		"SUM(", "FSUM(", "COUNT(", "COUNTIF(", "AVG(", "FAVG(",
		"MIN(", "MAX(", "ANY_VALUE(",
		"ARG_MIN(", "ARG_MIN_NULL(", "ARG_MAX(", "ARG_MAX_NULL(",
		"FIRST(", "LAST(", "PRODUCT(",
		"STRING_AGG(", "LIST(", "ARRAY_AGG(",
		"BOOL_AND(", "BOOL_OR(",
		"BIT_AND(", "BIT_OR(", "BIT_XOR(", "BITSTRING_AGG(",
		"GEOMETRIC_MEAN(", "WEIGHTED_AVG(",
		// Statistical
		"MEDIAN(", "MODE(", "MAD(",
		"STDDEV(", "STDDEV_POP(", "STDDEV_SAMP(",
		"VARIANCE(", "VAR_POP(", "VAR_SAMP(",
		"SKEWNESS(", "SKEWNESS_POP(",
		"KURTOSIS(", "KURTOSIS_POP(",
		"ENTROPY(", "CORR(",
		"COVAR_POP(", "COVAR_SAMP(",
		"QUANTILE(", "QUANTILE_CONT(", "QUANTILE_DISC(",
		"HISTOGRAM(", "HISTOGRAM_EXACT(", "HISTOGRAM_VALUES(",
		// Approximate
		"APPROX_COUNT_DISTINCT(", "APPROX_QUANTILE(", "APPROX_TOP_K(",
		"RESERVOIR_QUANTILE(",
		// Regression
		"REGR_AVGX(", "REGR_AVGY(", "REGR_COUNT(",
		"REGR_INTERCEPT(", "REGR_R2(", "REGR_SLOPE(",
		"REGR_SXX(", "REGR_SXY(", "REGR_SYY(",
	} {
		if strings.Contains(upper, fn) {
			return true
		}
	}
	// Window functions — each chunk restarts the window, producing wrong results
	if strings.Contains(upper, " OVER(") || strings.Contains(upper, " OVER (") {
		return true
	}
	return false
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
