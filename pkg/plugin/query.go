package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// parseArcError extracts a human-readable error from Arc's JSON error response.
// Arc returns errors as: {"error": "message"} or plain text.
func parseArcError(statusCode int, body []byte) string {
	var parsed struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &parsed) == nil && parsed.Error != "" {
		return fmt.Sprintf("Arc error (HTTP %d): %s", statusCode, parsed.Error)
	}
	text := strings.TrimSpace(string(body))
	if len(text) > 500 {
		text = text[:500] + "..."
	}
	if text == "" {
		return fmt.Sprintf("Arc returned HTTP %d with no error message", statusCode)
	}
	return fmt.Sprintf("Arc error (HTTP %d): %s", statusCode, text)
}

// formatRequestError converts Go HTTP client errors into user-friendly messages
// while preserving the original error chain for programmatic inspection via errors.Is/As.
func formatRequestError(err error) error {
	msg := err.Error()
	var friendly string
	switch {
	case strings.Contains(msg, "context deadline exceeded") || strings.Contains(msg, "Client.Timeout"):
		friendly = "Query timed out — try reducing the time range, increasing the timeout in datasource settings, or enabling query splitting"
	case strings.Contains(msg, "connection refused"):
		friendly = "Cannot connect to Arc — connection refused. Check that Arc is running and the URL is correct"
	case strings.Contains(msg, "no such host"):
		friendly = "Cannot connect to Arc — hostname not found. Check the URL in datasource settings"
	case strings.Contains(msg, "EOF"):
		friendly = "Arc closed the connection unexpectedly — the query may be too large. Try enabling query splitting or reducing the time range"
	default:
		friendly = "Request to Arc failed"
	}
	return fmt.Errorf("%s: %w", friendly, err)
}

// queryJSON executes a query using Arc's JSON endpoint (fallback path used
// when the user has disabled Arrow). Returns a decoded Grafana DataFrame.
func queryJSON(ctx context.Context, settings *ArcInstanceSettings, sql string) (*data.Frame, error) {
	// Build request
	url := fmt.Sprintf("%s/api/v1/query", settings.settings.URL)

	reqBody := map[string]interface{}{
		"sql": sql,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", settings.apiKey))

	// Set database if specified
	if settings.settings.Database != "" {
		req.Header.Set("X-Arc-Database", settings.settings.Database)
	}

	client := newHTTPClient(
		time.Duration(settings.settings.Timeout)*time.Second,
		allowPrivateForSettings(settings),
	)

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return nil, formatRequestError(err)
	}
	defer resp.Body.Close()

	body := http.MaxBytesReader(nil, resp.Body, MaxResponseBytes)

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(body)
		return nil, errors.New(parseArcError(resp.StatusCode, raw))
	}

	// Parse JSON response
	var result map[string]interface{}
	if err := json.NewDecoder(body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode Arc JSON response: %w", err)
	}

	duration := time.Since(start)
	log.DefaultLogger.Debug("JSON query completed",
		"duration_ms", duration.Milliseconds(),
	)

	// Convert to DataFrame
	frame, err := JSONToDataFrame(result)
	if err != nil {
		return nil, fmt.Errorf("failed to convert response to DataFrame: %w", err)
	}

	// Add metadata
	frame.Meta = &data.FrameMeta{
		ExecutedQueryString: sql,
		Custom: map[string]interface{}{
			"executionTime": duration.Milliseconds(),
		},
	}

	return frame, nil
}

// JSONToDataFrame converts Arc JSON response to Grafana DataFrame
func JSONToDataFrame(result map[string]interface{}) (*data.Frame, error) {
	// Extract column names from Arc response
	// Arc returns: {"columns": ["col1", "col2", ...], "data": [[row1], [row2], ...], "rows": N}
	columnsInterface, ok := result["columns"]
	if !ok {
		return nil, fmt.Errorf("missing 'columns' field in response")
	}

	columnsSlice, ok := columnsInterface.([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid columns format")
	}

	columnNames := make([]string, len(columnsSlice))
	for i, col := range columnsSlice {
		name, ok := col.(string)
		if !ok {
			return nil, fmt.Errorf("invalid column name at index %d: expected string, got %T", i, col)
		}
		columnNames[i] = name
	}

	// Extract data from Arc response
	dataInterface, ok := result["data"]
	if !ok {
		return nil, fmt.Errorf("missing 'data' field in response")
	}

	// Convert to slices
	dataRows, ok := dataInterface.([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid data format")
	}

	if len(dataRows) == 0 {
		return data.NewFrame(""), nil
	}

	// Get number of columns from first row
	firstRow, ok := dataRows[0].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid row format")
	}

	numCols := len(firstRow)
	numRows := len(dataRows)

	log.DefaultLogger.Debug("Parsing JSON response",
		"numColumns", numCols,
		"numRows", numRows,
		"columns", columnNames,
	)

	// Create fields for each column

	fields := make([]*data.Field, numCols)

	for colIdx := 0; colIdx < numCols; colIdx++ {
		colName := columnNames[colIdx]

		// Infer type from first non-null value
		var fieldType data.FieldType
		var sample interface{}

		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			row, ok := dataRows[rowIdx].([]interface{})
			if !ok {
				return nil, fmt.Errorf("invalid row at index %d: expected array, got %T", rowIdx, dataRows[rowIdx])
			}
			if colIdx >= len(row) {
				return nil, fmt.Errorf("row %d has %d columns, expected at least %d", rowIdx, len(row), colIdx+1)
			}
			if row[colIdx] != nil {
				sample = row[colIdx]
				break
			}
		}

		// Determine field type
		switch v := sample.(type) {
		case float64:
			fieldType = data.FieldTypeNullableFloat64
		case string:
			// Check if it's a timestamp (try multiple formats)
			// Arc sends: "2025-10-28T16:03:25.431000"
			if colName == "time" || colName == "timestamp" || colName == "_time" {
				fieldType = data.FieldTypeNullableTime
			} else if _, err := time.Parse(time.RFC3339, v); err == nil {
				fieldType = data.FieldTypeNullableTime
			} else if _, err := time.Parse("2006-01-02T15:04:05.000000", v); err == nil {
				fieldType = data.FieldTypeNullableTime
			} else {
				fieldType = data.FieldTypeNullableString
			}
		case bool:
			fieldType = data.FieldTypeNullableBool
		default:
			fieldType = data.FieldTypeNullableString
		}

		// Create field based on type
		switch fieldType {
		case data.FieldTypeNullableFloat64:
			values := make([]*float64, numRows)
			var typeMismatches int
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				row, ok := dataRows[rowIdx].([]interface{})
				if !ok || colIdx >= len(row) || row[colIdx] == nil {
					continue
				}
				v, ok := row[colIdx].(float64)
				if !ok {
					typeMismatches++
					continue
				}
				val := v
				values[rowIdx] = &val
			}
			if typeMismatches > 0 {
				log.DefaultLogger.Warn("numeric column had non-float64 rows",
					"col", colName, "mismatches", typeMismatches, "total", numRows)
			}
			fields[colIdx] = data.NewField(colName, nil, values)

		case data.FieldTypeNullableTime:
			// Detect the string format once on the first sample so we don't
			// retry up to three time.Parse layouts per row on big result sets.
			detectedLayout := ""
			if sampleStr, ok := sample.(string); ok {
				for _, layout := range timestampLayouts {
					if _, err := time.Parse(layout, sampleStr); err == nil {
						detectedLayout = layout
						break
					}
				}
			}
			values := make([]*time.Time, numRows)
			var parseFailures int
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				row, ok := dataRows[rowIdx].([]interface{})
				if !ok || colIdx >= len(row) || row[colIdx] == nil {
					continue
				}
				t, ok := parseJSONTimestamp(row[colIdx], detectedLayout)
				if !ok {
					parseFailures++
					continue
				}
				timeCopy := t
				values[rowIdx] = &timeCopy
			}
			if parseFailures > 0 {
				// Summary log (one line per column) instead of one-line-per-row
				// spam. A 100k-row response with a corrupted column previously
				// emitted 100k warn lines.
				log.DefaultLogger.Warn("timestamp column had unparseable rows",
					"col", colName, "failures", parseFailures, "total", numRows)
			}
			fields[colIdx] = data.NewField(colName, nil, values)

		case data.FieldTypeNullableString:
			values := make([]*string, numRows)
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				row, ok := dataRows[rowIdx].([]interface{})
				if !ok || colIdx >= len(row) || row[colIdx] == nil {
					continue
				}
				// Type-assert before falling back to Sprintf — the inferred
				// column type is string, so the common case avoids reflection.
				if s, ok := row[colIdx].(string); ok {
					strCopy := s
					values[rowIdx] = &strCopy
					continue
				}
				str := fmt.Sprintf("%v", row[colIdx])
				values[rowIdx] = &str
			}
			fields[colIdx] = data.NewField(colName, nil, values)

		case data.FieldTypeNullableBool:
			values := make([]*bool, numRows)
			var typeMismatches int
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				row, ok := dataRows[rowIdx].([]interface{})
				if !ok || colIdx >= len(row) || row[colIdx] == nil {
					continue
				}
				v, ok := row[colIdx].(bool)
				if !ok {
					typeMismatches++
					continue
				}
				val := v
				values[rowIdx] = &val
			}
			if typeMismatches > 0 {
				log.DefaultLogger.Warn("boolean column had non-bool rows",
					"col", colName, "mismatches", typeMismatches, "total", numRows)
			}
			fields[colIdx] = data.NewField(colName, nil, values)
		}
	}

	frame := data.NewFrame("", fields...)

	// Identify which fields are labels (string fields that are not "time")
	// This helps Grafana understand wide vs long format for time series
	for _, field := range frame.Fields {
		if field.Type() == data.FieldTypeNullableString && field.Name != "time" && field.Name != "timestamp" {
			// Mark string fields (except time) as labels
			if field.Labels == nil {
				field.Labels = data.Labels{}
			}
		}
	}

	log.DefaultLogger.Debug("Created frame from JSON",
		"fields", len(frame.Fields),
		"rows", frame.Rows(),
		"fieldNames", func() []string {
			names := make([]string, len(frame.Fields))
			for i, f := range frame.Fields {
				names[i] = f.Name
			}
			return names
		}(),
	)

	// Log first row for debugging
	if frame.Rows() > 0 {
		firstRow := make([]interface{}, len(frame.Fields))
		for i, field := range frame.Fields {
			firstRow[i] = field.At(0)
		}
		log.DefaultLogger.Debug("First row of data", "values", firstRow)
	}

	return frame, nil
}

// calculateInterval picks an appropriate aggregation interval for the given duration.
func calculateInterval(duration time.Duration) string {
	switch {
	case duration > 7*24*time.Hour:
		return "1 hour"
	case duration > 24*time.Hour:
		return "10 minutes"
	case duration > 6*time.Hour:
		return "1 minute"
	default:
		return "10 seconds"
	}
}

// replaceMacroOccurrences walks `sql` once and rewrites every occurrence of
// `macro` that lives outside string literals and comments. For each in-scope
// occurrence the inner argument (between the macro's opening paren and the
// matching closing paren, respecting nested parens) is passed to `rewrite`.
// If rewrite returns ok=false the original macro text is preserved verbatim.
//
// The single-pass approach (O(N) over `sql`, with `strings.Builder` output)
// replaces the previous repeated slice-splice loop that was O(N·L) per
// macro. The literal-and-comment awareness also fixes the C4 issue where
// `WHERE message = 'count of $__timeFilter(time)'` would have its literal
// content rewritten.
func replaceMacroOccurrences(sql, macro string, rewrite func(arg string) (string, bool)) string {
	var out strings.Builder
	out.Grow(len(sql))
	i := 0
	for i < len(sql) {
		// Skip over '...' string literals (preserve verbatim).
		if sql[i] == '\'' {
			out.WriteByte(sql[i])
			i++
			for i < len(sql) {
				out.WriteByte(sql[i])
				if sql[i] == '\'' {
					// Escaped quote ''
					if i+1 < len(sql) && sql[i+1] == '\'' {
						out.WriteByte(sql[i+1])
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}
		// Skip over -- line comments.
		if sql[i] == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			end := strings.IndexByte(sql[i:], '\n')
			if end < 0 {
				out.WriteString(sql[i:])
				return out.String()
			}
			out.WriteString(sql[i : i+end])
			i += end
			continue
		}
		// Skip over /* block comments */.
		if sql[i] == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				out.WriteString(sql[i:])
				return out.String()
			}
			out.WriteString(sql[i : i+2+end+2])
			i += 2 + end + 2
			continue
		}
		// Macro at this position?
		if i+len(macro) <= len(sql) && sql[i:i+len(macro)] == macro {
			closeIdx := findMatchingParen(sql, i+len(macro)-1)
			if closeIdx < 0 {
				// Unmatched paren — leave the rest of the SQL untouched.
				out.WriteString(sql[i:])
				return out.String()
			}
			arg := sql[i+len(macro) : closeIdx]
			if rewritten, ok := rewrite(arg); ok {
				out.WriteString(rewritten)
			} else {
				// Caller declined the rewrite — preserve the original macro
				// text so Arc surfaces a clear error rather than producing
				// silently-mangled SQL.
				out.WriteString(sql[i : closeIdx+1])
			}
			i = closeIdx + 1
			continue
		}
		out.WriteByte(sql[i])
		i++
	}
	return out.String()
}

// findMatchingParen scans forward from `openIdx` (which must point at '(')
// and returns the index of the matching ')', respecting nested parens and
// string literals inside the arg. Returns -1 if no match is found.
func findMatchingParen(sql string, openIdx int) int {
	if openIdx >= len(sql) || sql[openIdx] != '(' {
		return -1
	}
	depth := 1
	i := openIdx + 1
	for i < len(sql) {
		c := sql[i]
		switch c {
		case '\'':
			// Skip string literal.
			i++
			for i < len(sql) {
				if sql[i] == '\'' {
					if i+1 < len(sql) && sql[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
		case '(':
			depth++
			i++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
			i++
		default:
			i++
		}
	}
	return -1
}

// expandTimeFilter replaces $__timeFilter(column) with column >= 'from' AND column < 'to'.
// Column arguments are validated against columnNameRe — anything else is left
// un-expanded so Arc surfaces a clear error rather than the macro silently
// injecting attacker-controlled SQL. Macros inside string literals or comments
// are not expanded.
func expandTimeFilter(sql string, from, to time.Time) string {
	fromStr := from.Format(time.RFC3339)
	toStr := to.Format(time.RFC3339)
	return replaceMacroOccurrences(sql, "$__timeFilter(", func(arg string) (string, bool) {
		column := strings.TrimSpace(arg)
		if column == "" {
			log.DefaultLogger.Warn("$__timeFilter macro has empty column argument, defaulting to 'time'")
			column = "time"
		}
		if err := validateColumnArg(column); err != nil {
			log.DefaultLogger.Warn("$__timeFilter rejected unsafe column argument", "column", column, "error", err.Error())
			return "", false
		}
		return fmt.Sprintf("%s >= '%s' AND %s < '%s'", column, fromStr, column, toStr), true
	})
}

// ApplyMacros replaces Grafana macros in SQL query
func ApplyMacros(sql string, timeRange backend.TimeRange) string {
	// $__timeFilter(column) -> column >= 'start' AND column < 'end'
	sql = expandTimeFilter(sql, timeRange.From, timeRange.To)

	// $__timeFrom() -> start time
	sql = strings.ReplaceAll(sql, "$__timeFrom()", fmt.Sprintf("'%s'", timeRange.From.Format(time.RFC3339)))

	// $__timeTo() -> end time
	sql = strings.ReplaceAll(sql, "$__timeTo()", fmt.Sprintf("'%s'", timeRange.To.Format(time.RFC3339)))

	// $__interval -> calculate interval based on time range
	sql = strings.ReplaceAll(sql, "$__interval", calculateInterval(timeRange.To.Sub(timeRange.From)))

	// $__timeGroup(column, interval) -> epoch-based bucketing
	// DuckDB's date_trunc/time_bucket retains nanosecond residuals on TIMESTAMP_NS columns,
	// causing GROUP BY to produce per-second rows instead of proper hourly buckets.
	// Epoch math avoids this entirely.
	sql = expandTimeGroup(sql)

	return sql
}

// ApplyMacrosWithSplit replaces macros using the chunk's time range for filtering
// but the original full range for $__interval calculation (so bucket sizes stay consistent)
func ApplyMacrosWithSplit(sql string, chunk backend.TimeRange, originalRange backend.TimeRange) string {
	// $__timeFilter uses chunk boundaries
	sql = expandTimeFilter(sql, chunk.From, chunk.To)

	// $__timeFrom/$__timeTo use chunk boundaries
	sql = strings.ReplaceAll(sql, "$__timeFrom()", fmt.Sprintf("'%s'", chunk.From.Format(time.RFC3339)))
	sql = strings.ReplaceAll(sql, "$__timeTo()", fmt.Sprintf("'%s'", chunk.To.Format(time.RFC3339)))

	// $__interval uses the ORIGINAL range so bucket sizes are consistent across all chunks
	sql = strings.ReplaceAll(sql, "$__interval", calculateInterval(originalRange.To.Sub(originalRange.From)))

	sql = expandTimeGroup(sql)

	return sql
}

// timestampLayouts is the ordered list of Go time layouts the JSON decoder
// will try when inferring a timestamp column's string format. The first
// matching layout for the first non-null sample is cached and used for
// every subsequent row — eliminating up to 3 time.Parse attempts per row.
var timestampLayouts = []string{
	time.RFC3339,
	"2006-01-02T15:04:05.000000", // Arc-emitted microsecond precision
	"2006-01-02T15:04:05",        // No timezone
}

// parseJSONTimestamp converts a JSON-decoded value to time.Time using the
// detectedLayout for strings (or trying every layout if detection failed for
// this column). Numeric values are interpreted as seconds when small and
// milliseconds when large — the 1e12 threshold sits at year 2001 in seconds
// and would be year 33000 in milliseconds.
func parseJSONTimestamp(v interface{}, detectedLayout string) (time.Time, bool) {
	switch x := v.(type) {
	case string:
		if detectedLayout != "" {
			if t, err := time.Parse(detectedLayout, x); err == nil {
				return t, true
			}
		}
		// Fallback path when detection didn't latch (mixed-format column).
		for _, layout := range timestampLayouts {
			if t, err := time.Parse(layout, x); err == nil {
				return t, true
			}
		}
		return time.Time{}, false
	case float64:
		if x > 1e12 {
			return time.Unix(0, int64(x)*int64(time.Millisecond)), true
		}
		return time.Unix(int64(x), 0), true
	case int64:
		if x > 1e12 {
			return time.Unix(0, x*int64(time.Millisecond)), true
		}
		return time.Unix(x, 0), true
	default:
		return time.Time{}, false
	}
}

// intervalSecondsTable maps DuckDB-compatible interval strings to seconds.
// Package-level so the lookup is O(1) per macro call instead of a 13-arm
// switch. Both short and long forms are accepted ("1m" and "1 minute").
var intervalSecondsTable = map[string]int{
	"1s": 1, "1 second": 1,
	"5s": 5, "5 seconds": 5,
	"10s": 10, "10 seconds": 10,
	"30s": 30, "30 seconds": 30,
	"1m": 60, "1 minute": 60,
	"5m": 300, "5 minutes": 300,
	"10m": 600, "10 minutes": 600,
	"15m": 900, "15 minutes": 900,
	"30m": 1800, "30 minutes": 1800,
	"1h": 3600, "1 hour": 3600,
	"6h": 21600, "6 hours": 21600,
	"12h": 43200, "12 hours": 43200,
	"1d": 86400, "1 day": 86400,
}

// intervalToSeconds converts a DuckDB interval string to seconds. Returns
// (seconds, true) on a hit and (0, false) on an unknown interval — caller
// is responsible for deciding fallback behavior. Before this signature the
// function silently defaulted unknown input to 3600s, masking typos like
// '1minutes' as a one-hour bucket.
func intervalToSeconds(interval string) (int, bool) {
	if secs, ok := intervalSecondsTable[strings.TrimSpace(interval)]; ok {
		return secs, true
	}
	return 0, false
}

// expandTimeGroup replaces $__timeGroup(column, interval) with epoch-based bucketing SQL.
// DuckDB's date_trunc/time_bucket retains nanosecond residuals on TIMESTAMP_NS columns,
// causing GROUP BY to produce per-second rows. Epoch math avoids this.
// Column argument is validated against columnNameRe; unknown intervals and
// arg-count mismatches are rejected (macro left un-expanded so Arc surfaces a
// clear error) rather than silently defaulting.
func expandTimeGroup(sql string) string {
	return replaceMacroOccurrences(sql, "$__timeGroup(", func(arg string) (string, bool) {
		parts := strings.Split(arg, ",")
		if len(parts) < 2 {
			log.DefaultLogger.Warn("$__timeGroup requires two arguments: $__timeGroup(column, interval)", "found", arg)
			return "", false
		}
		if len(parts) > 2 {
			// Extra args silently ignored before; now warn loudly.
			log.DefaultLogger.Warn("$__timeGroup ignored extra arguments — expected $__timeGroup(column, interval)",
				"found", arg, "extra_count", len(parts)-2)
			return "", false
		}
		column := strings.TrimSpace(parts[0])
		if err := validateColumnArg(column); err != nil {
			log.DefaultLogger.Warn("$__timeGroup rejected unsafe column argument", "column", column, "error", err.Error())
			return "", false
		}
		interval := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
		secs, ok := intervalToSeconds(interval)
		if !ok {
			log.DefaultLogger.Warn("$__timeGroup rejected unknown interval — expected '1s', '10s', '1m', '5m', '1h', '1d', etc.",
				"interval", interval)
			return "", false
		}
		// Use epoch_ns() (BIGINT) with // (integer division) instead of epoch() (DOUBLE)
		// to avoid floating-point precision loss that causes timestamps near hour
		// boundaries (e.g. 05:59:59.999) to round up to the next bucket (06:00:00).
		// DuckDB's / operator returns DOUBLE; // returns BIGINT.
		return fmt.Sprintf("to_timestamp((epoch_ns(%s) // 1000000000 // %d) * %d)", column, secs, secs), true
	})
}

// OptimizeTimeSeriesQuery adds ORDER BY time ASC if missing for better performance
// This eliminates the need for in-memory sorting, reducing query overhead significantly
// Inserts ORDER BY before LIMIT/OFFSET clauses to maintain valid SQL syntax
func OptimizeTimeSeriesQuery(sql string) string {
	sqlLower := strings.ToLower(strings.TrimSpace(sql))

	// Check if ORDER BY is already present
	if strings.Contains(sqlLower, "order by") {
		return sql
	}

	// Check if this looks like a time series query (contains 'time' column)
	if !strings.Contains(sqlLower, "time") {
		return sql
	}

	// Find LIMIT or OFFSET clause position
	sql = strings.TrimRight(sql, " \t\n\r;")

	// Find the position where we should insert ORDER BY
	// ORDER BY must come before LIMIT/OFFSET
	limitPos := strings.LastIndex(sqlLower, " limit ")
	offsetPos := strings.LastIndex(sqlLower, " offset ")

	insertPos := len(sql) // Default: end of query

	if limitPos != -1 && (offsetPos == -1 || limitPos < offsetPos) {
		insertPos = limitPos
	} else if offsetPos != -1 {
		insertPos = offsetPos
	}

	// Insert ORDER BY at the correct position
	if insertPos < len(sql) {
		return sql[:insertPos] + " ORDER BY time ASC" + sql[insertPos:]
	}

	// No LIMIT/OFFSET, add at end
	return sql + " ORDER BY time ASC"
}
