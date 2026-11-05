package plugin

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// Arc reports two ways a result can be shorter than what the query matched
// (Arc#726), and they mean opposite things:
//
//   - truncated: the stream FAILED part way. The rows that arrived are not the
//     result and must not be drawn as one, so this is a query error.
//   - row cap: an Enterprise governance row cap stopped the query. The rows are
//     a complete, valid result up to the cap, so the panel renders them with a
//     warning notice. Treating a capped result as an error invents an outage.
//
// JSON and MessagePack carry them in the envelope (truncated +
// truncation_reason, rows_capped + row_cap); Arrow carries them as HTTP
// trailers, because Arc only knows either once the last batch is written.
const (
	arcStreamTruncatedTrailer = "Arc-Stream-Truncated"
	// Non-empty means capped; the value is the cap. Arc before 27.01.1 sent
	// it empty on every response, so empty means not capped.
	arcRowsCappedTrailer = "Arc-Rows-Capped"
)

// truncatedError is the query error for a result Arc reports as truncated.
func truncatedError(reason string) error {
	if strings.TrimSpace(reason) == "" {
		return errors.New("arc query response was truncated")
	}
	return fmt.Errorf("arc query response was truncated: %s", truncateForLog(reason))
}

// rowCapNotice is the panel notice for a result Arc stopped at its row cap.
// rowCap is 0 when the server did not say what the cap is.
func rowCapNotice(rowCap int64) data.Notice {
	text := "Arc's row cap stopped this query: the rows shown are correct, but rows past the cap are missing. Narrow the time range or the query to see them."
	if rowCap > 0 {
		text = fmt.Sprintf("Arc's row cap stopped this query at %d rows: the rows shown are correct, but rows past the cap are missing. Narrow the time range or the query to see them.", rowCap)
	}
	return data.Notice{Severity: data.NoticeSeverityWarning, Text: text}
}

// addNotice attaches n to frame once. Split queries run one request per chunk,
// and every capped chunk reports the same cap.
func addNotice(frame *data.Frame, n data.Notice) {
	if frame.Meta == nil {
		frame.Meta = &data.FrameMeta{}
	}
	for _, have := range frame.Meta.Notices {
		if have.Severity == n.Severity && have.Text == n.Text {
			return
		}
	}
	frame.Meta.Notices = append(frame.Meta.Notices, n)
}

// setQueryMeta records the executed query on frame without discarding the
// notices decoding attached to it.
func setQueryMeta(frame *data.Frame, sql string, executionTimeMs int64) {
	if frame.Meta == nil {
		frame.Meta = &data.FrameMeta{}
	}
	frame.Meta.ExecutedQueryString = sql
	frame.Meta.Custom = map[string]interface{}{
		"executionTime": executionTimeMs,
	}
}

// jsonCompleteness reads Arc's completeness fields from a decoded JSON
// envelope. A truncated result is an error; a capped one reports its cap.
// Responses from servers that predate the fields omit them and are complete.
func jsonCompleteness(result map[string]interface{}) (capped bool, rowCap int64, err error) {
	if raw, ok := result["truncated"]; ok {
		truncated, ok := raw.(bool)
		if !ok {
			return false, 0, fmt.Errorf("invalid 'truncated' field in Arc response: expected boolean, got %T", raw)
		}
		if truncated {
			reason, _ := result["truncation_reason"].(string)
			return false, 0, truncatedError(reason)
		}
	}
	raw, ok := result["rows_capped"]
	if !ok {
		return false, 0, nil
	}
	if capped, ok = raw.(bool); !ok {
		return false, 0, fmt.Errorf("invalid 'rows_capped' field in Arc response: expected boolean, got %T", raw)
	}
	if !capped {
		return false, 0, nil
	}
	// The cap only words the notice; a missing or odd value still caps.
	if n, ok := result["row_cap"].(float64); ok && n > 0 && n <= math.MaxInt64 {
		rowCap = int64(n)
	}
	return true, rowCap, nil
}

// arrowCompleteness reads Arc's completeness trailers, which the HTTP client
// fills in only once the body has been read to EOF.
func arrowCompleteness(trailer http.Header) (capped bool, rowCap int64, err error) {
	if reason := trailer.Get(arcStreamTruncatedTrailer); reason != "" {
		return false, 0, truncatedError(reason)
	}
	v := strings.TrimSpace(trailer.Get(arcRowsCappedTrailer))
	if v == "" {
		return false, 0, nil
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
		rowCap = n
	}
	return true, rowCap, nil
}
