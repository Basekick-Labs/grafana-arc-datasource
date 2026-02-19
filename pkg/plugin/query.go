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

// formatRequestError converts Go HTTP client errors into user-friendly messages.
func formatRequestError(err error) string {
	msg := err.Error()
	if strings.Contains(msg, "context deadline exceeded") || strings.Contains(msg, "Client.Timeout") {
		return "Query timed out — try reducing the time range, increasing the timeout in datasource settings, or enabling query splitting"
	}
	if strings.Contains(msg, "connection refused") {
		return "Cannot connect to Arc — connection refused. Check that Arc is running and the URL is correct"
	}
	if strings.Contains(msg, "no such host") {
		return "Cannot connect to Arc — hostname not found. Check the URL in datasource settings"
	}
	if strings.Contains(msg, "EOF") {
		return "Arc closed the connection unexpectedly — the query may be too large. Try enabling query splitting or reducing the time range"
	}
	return fmt.Sprintf("Request to Arc failed: %s", msg)
}

// QueryJSON executes a query using Arc's JSON endpoint (fallback)
func QueryJSON(ctx context.Context, settings *ArcInstanceSettings, sql string, timeRange backend.TimeRange) (*data.Frame, error) {
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

	// Execute request
	client := &http.Client{
		Timeout: time.Duration(settings.settings.Timeout) * time.Second,
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New(formatRequestError(err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, errors.New(parseArcError(resp.StatusCode, body))
	}

	// Parse JSON response
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
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
		columnNames[i] = col.(string)
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
			row := dataRows[rowIdx].([]interface{})
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
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				row := dataRows[rowIdx].([]interface{})
				if row[colIdx] != nil {
					val := new(float64)
					*val = row[colIdx].(float64)
					values[rowIdx] = val
				}
			}
			fields[colIdx] = data.NewField(colName, nil, values)

		case data.FieldTypeNullableTime:
			values := make([]*time.Time, numRows)
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				row := dataRows[rowIdx].([]interface{})
				if row[colIdx] != nil {
					var t time.Time
					var err error

					// Handle different timestamp formats from Arc
					switch v := row[colIdx].(type) {
					case string:
						// Try RFC3339 first
						t, err = time.Parse(time.RFC3339, v)
						if err != nil {
							// Try Arc's format with microseconds
							t, err = time.Parse("2006-01-02T15:04:05.000000", v)
						}
						if err != nil {
							// Try without timezone
							t, err = time.Parse("2006-01-02T15:04:05", v)
						}
					case float64:
						// Unix timestamp in seconds or milliseconds
						if v > 1e12 {
							// Milliseconds
							t = time.Unix(0, int64(v)*int64(time.Millisecond))
						} else {
							// Seconds
							t = time.Unix(int64(v), 0)
						}
						err = nil
					case int64:
						// Unix timestamp
						if v > 1e12 {
							// Milliseconds
							t = time.Unix(0, v*int64(time.Millisecond))
						} else {
							// Seconds
							t = time.Unix(v, 0)
						}
						err = nil
					default:
						log.DefaultLogger.Warn("Unknown timestamp type",
							"type", fmt.Sprintf("%T", v),
							"value", v,
							"row", rowIdx,
							"col", colName,
						)
					}

					if err == nil {
						timeCopy := t
						values[rowIdx] = &timeCopy
					} else {
						log.DefaultLogger.Warn("Failed to parse timestamp",
							"error", err,
							"value", row[colIdx],
							"row", rowIdx,
							"col", colName,
						)
					}
				}
			}
			fields[colIdx] = data.NewField(colName, nil, values)

		case data.FieldTypeNullableString:
			values := make([]*string, numRows)
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				row := dataRows[rowIdx].([]interface{})
				if row[colIdx] != nil {
					str := fmt.Sprintf("%v", row[colIdx])
					values[rowIdx] = &str
				}
			}
			fields[colIdx] = data.NewField(colName, nil, values)

		case data.FieldTypeNullableBool:
			values := make([]*bool, numRows)
			for rowIdx := 0; rowIdx < numRows; rowIdx++ {
				row := dataRows[rowIdx].([]interface{})
				if row[colIdx] != nil {
					val := new(bool)
					*val = row[colIdx].(bool)
					values[rowIdx] = val
				}
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

// expandTimeFilter replaces $__timeFilter(column) with column >= 'from' AND column < 'to'.
// Extracts the column name from the macro argument instead of hardcoding 'time'.
func expandTimeFilter(sql string, from, to time.Time) string {
	for {
		idx := strings.Index(sql, "$__timeFilter(")
		if idx == -1 {
			return sql
		}

		end := strings.Index(sql[idx:], ")")
		if end == -1 {
			return sql
		}
		end += idx

		column := strings.TrimSpace(sql[idx+len("$__timeFilter(") : end])
		if column == "" {
			log.DefaultLogger.Warn("$__timeFilter macro has empty column argument, defaulting to 'time'")
			column = "time"
		}

		replacement := fmt.Sprintf("%s >= '%s' AND %s < '%s'",
			column,
			from.Format(time.RFC3339),
			column,
			to.Format(time.RFC3339),
		)
		sql = sql[:idx] + replacement + sql[end+1:]
	}
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

// intervalToSeconds converts an interval string to seconds.
func intervalToSeconds(interval string) int {
	interval = strings.TrimSpace(interval)
	switch interval {
	case "1s", "1 second":
		return 1
	case "5s", "5 seconds":
		return 5
	case "10s", "10 seconds":
		return 10
	case "30s", "30 seconds":
		return 30
	case "1m", "1 minute":
		return 60
	case "5m", "5 minutes":
		return 300
	case "10m", "10 minutes":
		return 600
	case "15m", "15 minutes":
		return 900
	case "30m", "30 minutes":
		return 1800
	case "1h", "1 hour":
		return 3600
	case "6h", "6 hours":
		return 21600
	case "12h", "12 hours":
		return 43200
	case "1d", "1 day":
		return 86400
	default:
		return 3600
	}
}

// expandTimeGroup replaces $__timeGroup(column, interval) with epoch-based bucketing SQL.
// DuckDB's date_trunc/time_bucket retains nanosecond residuals on TIMESTAMP_NS columns,
// causing GROUP BY to produce per-second rows. Epoch math avoids this.
func expandTimeGroup(sql string) string {
	for {
		idx := strings.Index(sql, "$__timeGroup(")
		if idx == -1 {
			return sql
		}

		// Find the closing paren
		end := strings.Index(sql[idx:], ")")
		if end == -1 {
			return sql
		}
		end += idx

		// Extract arguments: $__timeGroup(column, 'interval')
		inner := sql[idx+len("$__timeGroup(") : end]
		parts := strings.SplitN(inner, ",", 2)
		if len(parts) != 2 {
			log.DefaultLogger.Warn("$__timeGroup requires two arguments: $__timeGroup(column, interval)",
				"found", inner)
			return sql
		}

		column := strings.TrimSpace(parts[0])
		interval := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
		secs := intervalToSeconds(interval)

		// Use epoch_ns() (BIGINT) with // (integer division) instead of epoch() (DOUBLE)
		// to avoid floating-point precision loss that causes timestamps near hour
		// boundaries (e.g. 05:59:59.999) to round up to the next bucket (06:00:00).
		// DuckDB's / operator returns DOUBLE; // returns BIGINT.
		replacement := fmt.Sprintf("to_timestamp((epoch_ns(%s) // 1000000000 // %d) * %d)", column, secs, secs)
		sql = sql[:idx] + replacement + sql[end+1:]
	}
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
