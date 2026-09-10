package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/vmihailenco/msgpack/v5"
)

// parseArcError extracts a human-readable error from Arc's error response.
// Arc returns errors as JSON `{"error": "message"}` on most paths, as a
// MessagePack map with the same `error` key on the msgpack query endpoint,
// or as plain text. Body is truncated to maxErrorBodyBytes, backing off to
// the previous rune boundary so the result is always valid UTF-8 even if the
// body byte-cap fell inside a multi-byte sequence (L8 fix).
func parseArcError(statusCode int, body []byte) string {
	var parsed struct {
		Error string `json:"error" msgpack:"error"`
	}
	if json.Unmarshal(body, &parsed) == nil && parsed.Error != "" {
		return fmt.Sprintf("Arc error (HTTP %d): %s", statusCode, truncateForLog(parsed.Error))
	}
	// The msgpack query endpoint encodes its own errors (400/429/500/504) as
	// msgpack; auth-middleware and cluster-gate errors stay JSON even on that
	// route, which the branch above already handled.
	if msgpack.Unmarshal(body, &parsed) == nil && parsed.Error != "" {
		return fmt.Sprintf("Arc error (HTTP %d): %s", statusCode, truncateForLog(parsed.Error))
	}
	text := strings.TrimSpace(string(body))
	text = truncateForLog(text)
	if text == "" {
		return fmt.Sprintf("Arc returned HTTP %d with no error message", statusCode)
	}
	return fmt.Sprintf("Arc error (HTTP %d): %s", statusCode, text)
}

const maxErrorBodyBytes = 500

// truncateForLog caps s at maxErrorBodyBytes, backing off to the last
// complete UTF-8 rune boundary so the returned string is always valid UTF-8.
func truncateForLog(s string) string {
	if len(s) <= maxErrorBodyBytes {
		return s
	}
	cut := s[:maxErrorBodyBytes]
	// Drop any trailing partial rune.
	for len(cut) > 0 {
		r, size := utf8.DecodeLastRuneInString(cut)
		if r != utf8.RuneError || size > 1 {
			break
		}
		cut = cut[:len(cut)-1]
	}
	return cut + "..."
}

// formatRequestError converts Go HTTP client errors into user-friendly
// messages while preserving the original error chain for programmatic
// inspection via errors.Is / errors.As. Uses typed error matching where
// possible (context.DeadlineExceeded, net.OpError, dnsError, net.ErrClosed)
// instead of substring-matching err.Error() strings, which can change
// across Go releases (L7).
func formatRequestError(err error) error {
	friendly := "Request to Arc failed"
	switch {
	case errors.Is(err, context.Canceled):
		// User-initiated cancellation (Grafana panel edit, dashboard close,
		// click-cancel). Grafana surfaces "Query canceled" already; a verbose
		// "try reducing the time range" message confuses users who weren't
		// timing out. Keep the error chain (so errors.Is still detects it)
		// but use a neutral phrase.
		friendly = "Query canceled"
	case errors.Is(err, context.DeadlineExceeded):
		friendly = "Query timed out — try reducing the time range, increasing the timeout in datasource settings, or enabling query splitting"
	case errors.Is(err, errBlockedAddr):
		friendly = "Arc URL resolves to a blocked address (private/loopback). Update the datasource URL or enable Allow Private IPs."
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		friendly = "Arc closed the connection unexpectedly — the query may be too large. Try enabling query splitting or reducing the time range"
	default:
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) {
			friendly = "Cannot connect to Arc — hostname not found. Check the URL in datasource settings"
			break
		}
		var opErr *net.OpError
		if errors.As(err, &opErr) {
			// connection refused / network unreachable / TCP reset all surface as OpError.
			friendly = "Cannot connect to Arc — " + opErr.Op + " failed. Check that Arc is running and the URL is correct"
			break
		}
		// Last-resort substring check for `http.Client.Timeout`-style errors that
		// don't satisfy errors.Is(context.DeadlineExceeded) (older SDK versions).
		if strings.Contains(err.Error(), "Client.Timeout") {
			friendly = "Query timed out — try reducing the time range, increasing the timeout in datasource settings, or enabling query splitting"
		}
	}
	return fmt.Errorf("%s: %w", friendly, err)
}

// queryJSON executes a query using Arc's JSON endpoint (fallback path used
// when the user has disabled Arrow). Returns a decoded Grafana DataFrame.
func queryJSON(ctx context.Context, settings *ArcInstanceSettings, sql string) (*data.Frame, error) {
	start := time.Now()
	body, err := settings.doRequest(ctx, "/api/v1/query", map[string]any{"sql": sql})
	if err != nil {
		return nil, err
	}
	defer func() { _ = body.Close() }()

	var result map[string]interface{}
	if err := json.NewDecoder(body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode Arc JSON response: %w", err)
	}

	duration := time.Since(start)
	log.DefaultLogger.Debug("JSON query completed", "duration_ms", duration.Milliseconds())

	frame, err := JSONToDataFrame(result)
	if err != nil {
		return nil, fmt.Errorf("failed to convert response to DataFrame: %w", err)
	}

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

// intervalBuckets maps a time-range ceiling to the aggregation bucket used for
// ranges at or below it, ordered smallest first. One table, so the text form
// ($__interval) and the millisecond form ($__interval_ms) cannot drift apart.
var intervalBuckets = []struct {
	upTo time.Duration
	text string
	size time.Duration
}{
	{upTo: 6 * time.Hour, text: "10 seconds", size: 10 * time.Second},
	{upTo: 24 * time.Hour, text: "1 minute", size: time.Minute},
	{upTo: 7 * 24 * time.Hour, text: "10 minutes", size: 10 * time.Minute},
}

// widestBucket applies to any range larger than the last intervalBuckets entry.
var widestBucket = struct {
	text string
	size time.Duration
}{text: "1 hour", size: time.Hour}

// intervalBucket picks the aggregation bucket for a time range, in both the
// text form DuckDB accepts and the duration behind it.
func intervalBucket(duration time.Duration) (string, time.Duration) {
	for _, b := range intervalBuckets {
		if duration <= b.upTo {
			return b.text, b.size
		}
	}
	return widestBucket.text, widestBucket.size
}

// calculateInterval picks an appropriate aggregation interval for the given duration.
func calculateInterval(duration time.Duration) string {
	text, _ := intervalBucket(duration)
	return text
}

// intervalMilliseconds is calculateInterval's bucket in milliseconds, for
// `$__interval_ms`.
func intervalMilliseconds(duration time.Duration) int64 {
	_, size := intervalBucket(duration)
	return size.Milliseconds()
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

// isMacroWordByte reports whether b can be part of a macro token's name.
// Used to require a word boundary after a token so `$__interval` does not
// match the prefix of `$__interval_ms` (or any future `$__interval*` macro).
func isMacroWordByte(b byte) bool {
	return b == '_' ||
		(b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9')
}

// replaceIntervalToken replaces every whole-word occurrence of `token` with
// `replacement`, skipping SQL comments but NOT string literals.
//
// It is the deliberate counterpart to replaceLiteralAwareTokens, for the one
// macro family whose documented use is inside quotes:
//
//	time_bucket('$__interval', t)   INTERVAL '$__interval'
//
// Skipping literals there would leave the token intact and DuckDB fails with
// "Could not convert string '$__interval' to INTERVAL". Comments are still
// skipped, so `-- bucket $__interval` is preserved and the SQL shown in
// Grafana's inspector still matches what the author wrote.
//
// The word-boundary check is what makes the replacement order irrelevant:
// `$__interval` cannot consume the prefix of `$__interval_ms`, `$__intervalx`,
// or any macro Grafana adds to this family later.
func replaceIntervalToken(sql, token, replacement string) string {
	if !strings.Contains(sql, token) {
		return sql
	}
	var out strings.Builder
	out.Grow(len(sql))
	i := 0
	for i < len(sql) {
		// Line comment: -- ... \n
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
		// Block comment: /* ... */ (nested, as DuckDB and Postgres allow).
		if sql[i] == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			depth, j := 1, i+2
			for j < len(sql)-1 {
				if sql[j] == '/' && sql[j+1] == '*' {
					depth++
					j += 2
					continue
				}
				if sql[j] == '*' && sql[j+1] == '/' {
					depth--
					j += 2
					if depth == 0 {
						break
					}
					continue
				}
				j++
			}
			if depth != 0 {
				out.WriteString(sql[i:]) // unterminated; copy the rest verbatim
				return out.String()
			}
			out.WriteString(sql[i:j])
			i = j
			continue
		}
		if strings.HasPrefix(sql[i:], token) {
			next := i + len(token)
			if next >= len(sql) || !isMacroWordByte(sql[next]) {
				out.WriteString(replacement)
				i = next
				continue
			}
		}
		out.WriteByte(sql[i])
		i++
	}
	return out.String()
}

// replaceLiteralAwareTokens replaces every occurrence of `token` (a fixed
// string with no argument list, e.g. "$__interval" or "$__timeFrom()") with
// `replacement` — skipping occurrences inside string literals and SQL
// comments. This is the zero-arg sibling of `replaceMacroOccurrences` and
// fixes R2-CR5: the previous `strings.ReplaceAll` rewrote macros inside
// string literals (`WHERE msg = 'see $__timeFrom()'` mangled the literal).
func replaceLiteralAwareTokens(sql, token, replacement string) string {
	if !strings.Contains(sql, token) {
		return sql
	}
	var out strings.Builder
	out.Grow(len(sql))
	i := 0
	for i < len(sql) {
		// Skip '...' string literals (preserve verbatim, including any tokens inside).
		if sql[i] == '\'' {
			out.WriteByte(sql[i])
			i++
			for i < len(sql) {
				out.WriteByte(sql[i])
				if sql[i] == '\'' {
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
		// Skip -- line comments.
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
		// Skip /* block comments */.
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
		// Token match?
		if i+len(token) <= len(sql) && sql[i:i+len(token)] == token {
			out.WriteString(replacement)
			i += len(token)
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
// Column arguments are checked by validateColumnArg — anything unsafe is left
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
		// Parenthesised on both sides: validateColumnArg already rejects an
		// unbalanced argument, and wrapping makes a break-out structurally
		// impossible rather than merely rejected. Costs nothing semantically.
		return fmt.Sprintf("(%s) >= '%s' AND (%s) < '%s'", column, fromStr, column, toStr), true
	})
}

// ApplyMacros replaces Grafana macros in SQL query
func ApplyMacros(sql string, timeRange backend.TimeRange) string {
	return applyMacrosWith(sql, timeRange.From, timeRange.To, timeRange.To.Sub(timeRange.From))
}

// ApplyMacrosWithSplit replaces macros using the chunk's time range for
// `$__timeFilter`/`$__timeFrom`/`$__timeTo`, but the ORIGINAL range for
// `$__interval` so bucket sizes stay consistent across chunks.
func ApplyMacrosWithSplit(sql string, chunk backend.TimeRange, originalRange backend.TimeRange) string {
	return applyMacrosWith(sql, chunk.From, chunk.To, originalRange.To.Sub(originalRange.From))
}

// applyMacrosWith routes EVERY macro through literal-and-comment-aware
// walkers (R2-CR5): the previous implementation used `strings.ReplaceAll`
// for `$__timeFrom()`, `$__timeTo()`, and `$__interval`, which rewrote macro
// text inside string literals (`WHERE msg = 'see $__timeFrom()'` mangled the
// literal). All five Grafana macros now share the same safety.
func applyMacrosWith(sql string, filterFrom, filterTo time.Time, intervalDuration time.Duration) string {
	sql = expandTimeFilter(sql, filterFrom, filterTo)
	sql = replaceLiteralAwareTokens(sql, "$__timeFrom()", fmt.Sprintf("'%s'", filterFrom.Format(time.RFC3339)))
	sql = replaceLiteralAwareTokens(sql, "$__timeTo()", fmt.Sprintf("'%s'", filterTo.Format(time.RFC3339)))
	// The interval macros expand inside string literals, unlike the
	// parenthesised macros above -- see replaceIntervalToken for why, and for
	// why the order of these two lines does not matter. Only backend-only
	// paths (alerting, recorded queries) see either token unexpanded; the
	// frontend substitutes both before a panel query reaches us.
	sql = replaceIntervalToken(sql, "$__interval_ms",
		strconv.FormatInt(intervalMilliseconds(intervalDuration), 10))
	sql = replaceIntervalToken(sql, "$__interval", calculateInterval(intervalDuration))
	// $__timeGroup(column, interval) -> epoch-based bucketing
	// DuckDB's date_trunc/time_bucket retains nanosecond residuals on TIMESTAMP_NS columns,
	// causing GROUP BY to produce per-second rows. Epoch math avoids this.
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

// intervalUnitSeconds maps an interval unit, and its accepted spellings, to
// its length in seconds. Months and years are deliberately absent: they are
// not fixed-length, so epoch division cannot express them.
var intervalUnitSeconds = map[string]int{
	"s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
	"m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
	"h": 3600, "hr": 3600, "hrs": 3600, "hour": 3600, "hours": 3600,
	"d": 86400, "day": 86400, "days": 86400,
	"w": 604800, "week": 604800, "weeks": 604800,
}

// intervalPattern matches "<n><unit>" and "<n> <unit>", e.g. 30s, 2 minutes,
// 500ms. Anchored, so anything with trailing junk is rejected rather than
// silently truncated.
var intervalPattern = regexp.MustCompile(`^(\d+)\s*([a-zA-Z]+)$`)

// intervalToSeconds converts a DuckDB interval string to whole seconds.
// Returns (seconds, true) on success and (0, false) when the input is not an
// interval this plugin can bucket by.
//
// Parses the general `<n><unit>` grammar rather than consulting a fixed table.
// The table form (added in 1.3.2) accepted only 13 literal strings, which
// excluded most of what Grafana's own `$__interval` produces — 20s, 2m, 2h,
// 3h, 2d are all routine — and an unlisted value left the whole
// `$__timeGroup` macro unexpanded, so Arc received a literal `$` and failed to
// parse. 1.2.0 accepted anything by silently defaulting to one hour, which
// hid typos; parsing gets the coverage without the silence.
//
// Sub-second intervals (500ms) parse to 0 seconds and are rejected: epoch
// division by zero is meaningless, and a sub-second bucket is not something
// the epoch-seconds path can express.
func intervalToSeconds(interval string) (int, bool) {
	return parseIntervalGrammar(strings.TrimSpace(interval))
}

// parseIntervalGrammar parses the `<n><unit>` / `<n> <unit>` interval grammar.
func parseIntervalGrammar(interval string) (int, bool) {
	m := intervalPattern.FindStringSubmatch(interval)
	if m == nil {
		return 0, false
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil || n <= 0 {
		return 0, false
	}
	unit, ok := intervalUnitSeconds[strings.ToLower(m[2])]
	if !ok {
		return 0, false
	}
	// Bound before multiplying, so the product cannot overflow. A bucket wider
	// than a month is never a useful time-series aggregation, and an absurd
	// one (`9223372036854775807s`) would otherwise collapse every row into a
	// single bucket at the epoch — a silently meaningless chart rather than an
	// error.
	const maxIntervalSeconds = 31 * 86400
	if n > int64(maxIntervalSeconds) {
		return 0, false
	}
	secs := int(n) * unit
	if secs <= 0 || secs > maxIntervalSeconds {
		return 0, false
	}
	return secs, true
}

// expandTimeGroup replaces $__timeGroup(column, interval) with epoch-based bucketing SQL.
// DuckDB's date_trunc/time_bucket retains nanosecond residuals on TIMESTAMP_NS columns,
// causing GROUP BY to produce per-second rows. Epoch math avoids this.
// Column argument is checked by validateColumnArg; unknown intervals and
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
			// Postgres and Timescale accept a third "fill" argument
			// ($__timeGroup(time, '5m', 0)), so dashboards migrated from those
			// datasources carry it. This plugin does not fill gaps — Grafana's
			// own "Connect null values" / "Fill" panel options do — so the
			// argument is ignored rather than rejected. Rejecting it (1.3.2)
			// left the whole macro unexpanded and broke every migrated panel.
			log.DefaultLogger.Debug("$__timeGroup ignoring extra arguments — this plugin does not fill gaps; use the panel's fill option",
				"found", arg, "extra_count", len(parts)-2)
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

// timeColumnRe matches a bare `time` column reference: the word "time" not
// glued to another identifier character and not inside a double-quoted
// identifier. `lifetime`, `runtime`, `timestamp` and `time_bucket` therefore
// do NOT match, which is what made the previous substring check unusable — it
// appended `ORDER BY time ASC` to queries whose only "time" was part of
// another column name, sorting by a column that need not exist.
var timeColumnRe = regexp.MustCompile(`(?i)(^|[^A-Za-z0-9_."])time($|[^A-Za-z0-9_."])`)

// orderByRe matches an ORDER BY the query already has, allowing any run of
// whitespace between the two words.
var orderByRe = regexp.MustCompile(`(?i)\border\s+by\b`)

// tailClauseRe matches a row-limiting clause an appended ORDER BY must
// precede. FETCH is included so `FETCH FIRST n ROWS ONLY` is recognised and
// the query declined, rather than having ORDER BY appended after it (a hard
// parse error).
//
// A real clause is followed by its argument — a number, a parameter, or (for
// FETCH) the word FIRST/NEXT. Requiring that excludes `SELECT time, limit FROM
// t` and `SELECT time, "limit" FROM t`, where the word is a column name:
// DuckDB accepts both, and treating either as a clause boundary splices the
// ORDER BY into the middle of the select list. A LIMIT whose argument is
// something else entirely (an expression, a template variable) simply does not
// match, and the query is then declined rather than rewritten — the safe
// direction.
var tailClauseRe = regexp.MustCompile(`(?i)(^|[^A-Za-z0-9_."])(limit|offset)\s+(\d+|\$\w+|\?|:\w+)|(^|[^A-Za-z0-9_."])(fetch)\s+(first|next)\b`)

// OptimizeTimeSeriesQuery appends `ORDER BY time ASC` to a time-series query
// that does not already order its rows, so Grafana receives points in
// chronological order without sorting them in memory.
//
// Only for `format: time_series` — a table panel must keep the row order the
// author asked for (CLAUDE.md gotcha 6).
//
// Deliberately conservative, because this rewrites SQL the user wrote. A
// missing sort renders a zig-zag line, which is merely ugly; a misplaced
// ORDER BY either fails the query or, worse, silently changes WHICH ROWS a
// LIMIT selects. This feature was disabled during the 1.3.2 hardening for
// exactly that class of defect, so every ambiguous shape is declined:
//
//   - an existing ORDER BY, or no bare `time` column;
//   - any parenthesis — a subquery or CTE can bind a trailing clause to the
//     wrong SELECT, and inserting before a nested LIMIT changes which rows
//     that LIMIT returns (a silent wrong answer, the worst outcome here);
//   - a set operation or a multi-statement input;
//   - `FETCH`, or more than one LIMIT/OFFSET token, or `limit`/`offset` used
//     as an identifier — all cases where the insertion point is ambiguous;
//   - a last line that opens a `--` comment, which would swallow the clause.
func OptimizeTimeSeriesQuery(sql string) string {
	trimmed := strings.TrimRight(sql, " \t\n\r;")
	if trimmed == "" {
		return sql
	}
	return optimizeTimeSeriesQuery(trimmed, newStrippedSQL(trimmed), sql)
}

// optimizeTimeSeriesQuery is the implementation, taking a strippedSQL the
// caller may already have computed. `original` is returned unchanged whenever
// the rewrite is declined, so the caller's exact input survives.
//
// Every keyword test is gated behind a cheap `strings.Contains` on the
// uppercased view: a `(?i)` regex cannot use Go's literal-prefix scan, so a
// MISS costs a full NFA walk of the query (~40µs on 4KB) — and a miss is the
// common case here, since most queries have neither ORDER BY nor LIMIT.
func optimizeTimeSeriesQuery(trimmed string, stripped strippedSQL, original string) string {
	if strings.Contains(stripped.upper, "ORDER") && orderByRe.MatchString(stripped.stripped) {
		return original
	}
	if !strings.Contains(stripped.upper, "TIME") || !timeColumnRe.MatchString(stripped.stripped) {
		return original
	}
	// Blunt on purpose: distinguishing a safe parenthesis from one that nests
	// a LIMIT is more machinery than this optimisation is worth.
	if strings.ContainsAny(stripped.stripped, "();") || containsUnion(stripped) {
		return original
	}

	var matches [][]int
	if strings.Contains(stripped.upper, "LIMIT") ||
		strings.Contains(stripped.upper, "OFFSET") ||
		strings.Contains(stripped.upper, "FETCH") {
		matches = tailClauseRe.FindAllStringIndex(stripped.stripped, -1)
	}
	switch {
	case len(matches) > 1:
		return original
	case len(matches) == 1 && strings.Contains(strings.ToUpper(stripped.stripped[matches[0][0]:matches[0][1]]), "FETCH"):
		return original
	case len(matches) == 0:
		if endsInLineComment(trimmed) {
			return original
		}
		return trimmed + " ORDER BY time ASC"
	}

	// Exactly one LIMIT/OFFSET. Stripping removes literal bodies, so stripped
	// offsets do not map onto the original — find it again there, and require
	// the same single unambiguous match.
	origMatches := tailClauseRe.FindAllStringIndex(trimmed, -1)
	if len(origMatches) != 1 {
		return original
	}
	// The match may include a leading separator character; cut at the keyword
	// itself so the separator stays with the head.
	at := origMatches[0][0]
	for at < len(trimmed) && !isASCIILetter(trimmed[at]) {
		at++
	}
	head := strings.TrimRight(trimmed[:at], " \t\n\r")
	if endsInLineComment(head) {
		return original
	}
	return head + " ORDER BY time ASC " + strings.TrimSpace(trimmed[at:])
}

// isASCIILetter reports whether b is an unaccented ASCII letter.
func isASCIILetter(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

// endsInLineComment reports whether the last line of sql opens a `--` comment
// with no newline after it, so appended text would be commented out.
func endsInLineComment(sql string) bool {
	lastLine := sql[strings.LastIndexByte(sql, '\n')+1:]
	inLiteral := false
	for i := 0; i < len(lastLine); i++ {
		switch {
		case lastLine[i] == '\'':
			if inLiteral && i+1 < len(lastLine) && lastLine[i+1] == '\'' {
				i++ // escaped quote inside a literal
				continue
			}
			inLiteral = !inLiteral
		case !inLiteral && lastLine[i] == '-' && i+1 < len(lastLine) && lastLine[i+1] == '-':
			return true
		}
	}
	return false
}
