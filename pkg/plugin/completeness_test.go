package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sync/semaphore"
)

// Arc#726: a truncated result is an error, a capped one renders with a
// warning notice, and a response without either field is complete.

func queryJSONTestSettings(server *httptest.Server) *ArcInstanceSettings {
	return &ArcInstanceSettings{
		settings:         ArcDataSourceSettings{URL: server.URL},
		client:           server.Client(),
		sem:              semaphore.NewWeighted(1),
		maxResponseBytes: 1024,
	}
}

func jsonServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" {
			t.Errorf("request path = %q, want /api/v1/query", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// wantCapNotice checks frame carries exactly one row-cap warning, naming the
// cap when it is known.
func wantCapNotice(t *testing.T, frame *data.Frame, rowCap string) {
	t.Helper()
	if frame == nil || frame.Meta == nil {
		t.Fatal("capped result has no frame meta")
	}
	n := 0
	for _, notice := range frame.Meta.Notices {
		if !strings.Contains(notice.Text, "row cap") {
			continue
		}
		n++
		if notice.Severity != data.NoticeSeverityWarning {
			t.Errorf("row cap notice severity = %v, want warning", notice.Severity)
		}
		if rowCap != "" && !strings.Contains(notice.Text, " "+rowCap+" rows") {
			t.Errorf("row cap notice %q does not name the cap %s", notice.Text, rowCap)
		}
	}
	if n != 1 {
		t.Fatalf("row cap notices = %d, want 1 (notices: %+v)", n, frame.Meta.Notices)
	}
}

func wantNoNotices(t *testing.T, frame *data.Frame) {
	t.Helper()
	if frame.Meta != nil && len(frame.Meta.Notices) > 0 {
		t.Fatalf("complete result has notices: %+v", frame.Meta.Notices)
	}
}

func TestQueryJSONRejectsTruncatedResults(t *testing.T) {
	server := jsonServer(t, `{"columns":["value"],"data":[[42]],"truncated":true,"truncation_reason":"context canceled"}`)

	frame, err := queryJSON(context.Background(), queryJSONTestSettings(server), "SELECT 42 AS value")
	if err == nil {
		t.Fatal("queryJSON returned a partial frame without an error for a truncated result")
	}
	if frame != nil {
		t.Fatalf("queryJSON returned a partial frame for a truncated result: %#v", frame)
	}
	if !strings.Contains(err.Error(), "truncated") || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("queryJSON error = %q, want truncation reason", err)
	}
}

func TestQueryJSONAcceptsCompleteLegacyResults(t *testing.T) {
	server := jsonServer(t, `{"columns":["value"],"data":[[42]]}`)

	frame, err := queryJSON(context.Background(), queryJSONTestSettings(server), "SELECT 42 AS value")
	if err != nil {
		t.Fatalf("queryJSON returned an error for a complete legacy response: %v", err)
	}
	if frame == nil || len(frame.Fields) != 1 {
		t.Fatalf("queryJSON frame = %#v, want one value field containing 42", frame)
	}
	value, ok := frame.Fields[0].At(0).(*float64)
	if !ok || value == nil || *value != 42 {
		t.Fatalf("queryJSON field value = %#v, want a numeric value of 42", frame.Fields[0].At(0))
	}
	wantNoNotices(t, frame)
}

func TestQueryJSONCappedResultRendersWithNotice(t *testing.T) {
	server := jsonServer(t, `{"columns":["value"],"data":[[1],[2]],"rows_capped":true,"row_cap":2}`)

	frame, err := queryJSON(context.Background(), queryJSONTestSettings(server), "SELECT value FROM t")
	if err != nil {
		t.Fatalf("a capped result is valid up to the cap, got error: %v", err)
	}
	if frame.Rows() != 2 {
		t.Fatalf("rows = %d, want the 2 capped rows", frame.Rows())
	}
	wantCapNotice(t, frame, "2")
	if frame.Meta.ExecutedQueryString != "SELECT value FROM t" {
		t.Errorf("executed query = %q: setting the query meta dropped or replaced it", frame.Meta.ExecutedQueryString)
	}
}

func TestQueryJSONUncappedResultHasNoNotice(t *testing.T) {
	server := jsonServer(t, `{"columns":["value"],"data":[[1]],"rows_capped":false,"truncated":false}`)

	frame, err := queryJSON(context.Background(), queryJSONTestSettings(server), "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	wantNoNotices(t, frame)
}

func TestQueryJSONTruncationWinsOverCap(t *testing.T) {
	server := jsonServer(t, `{"columns":["value"],"data":[[1]],"rows_capped":true,"row_cap":1,"truncated":true,"truncation_reason":"file disappeared"}`)

	if _, err := queryJSON(context.Background(), queryJSONTestSettings(server), "SELECT 1"); err == nil || !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("a failed stream is an error even when capped, got %v", err)
	}
}

func TestQueryJSONMalformedCompletenessRejected(t *testing.T) {
	for _, body := range []string{
		`{"columns":["value"],"data":[[1]],"truncated":"yes"}`,
		`{"columns":["value"],"data":[[1]],"rows_capped":"yes"}`,
	} {
		server := jsonServer(t, body)
		if _, err := queryJSON(context.Background(), queryJSONTestSettings(server), "SELECT 1"); err == nil {
			t.Errorf("%s: malformed completeness field accepted", body)
		}
	}
}

// encodeEnvelopeWith is encodeEnvelope's envelope plus extra trailing keys,
// as Arc appends rows_capped / row_cap after the fixed seven.
func encodeEnvelopeWith(t *testing.T, cols []mpColumn, extra ...interface{}) []byte {
	t.Helper()
	base := encodeEnvelope(t, cols)
	// Rewrite the fixmap header (0x80 | 7) for the extra pairs and append them.
	if base[0] != 0x87 {
		t.Fatalf("unexpected envelope header %#x", base[0])
	}
	var buf bytes.Buffer
	buf.WriteByte(0x80 | byte(7+len(extra)/2))
	buf.Write(base[1:])
	enc := msgpack.NewEncoder(&buf)
	for _, v := range extra {
		if err := enc.Encode(v); err != nil {
			t.Fatalf("encode: %v", err)
		}
	}
	return buf.Bytes()
}

func msgpackServer(t *testing.T, body []byte) *ArcInstanceSettings {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/msgpack")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return newTestInstance(t, srv.URL, ProtocolMsgpack)
}

var oneValueColumn = []mpColumn{{name: "value", typ: "float64", values: []interface{}{1.0, 2.0}}}

func TestQueryMsgpackCappedResultRendersWithNotice(t *testing.T) {
	inst := msgpackServer(t, encodeEnvelopeWith(t, oneValueColumn, "rows_capped", true, "row_cap", uint64(2)))

	frame, err := queryMsgpack(context.Background(), inst, "SELECT value FROM t")
	if err != nil {
		t.Fatalf("a capped result is valid up to the cap, got error: %v", err)
	}
	if frame.Rows() != 2 {
		t.Fatalf("rows = %d, want 2", frame.Rows())
	}
	wantCapNotice(t, frame, "2")
}

func TestQueryMsgpackCompleteResultHasNoNotice(t *testing.T) {
	inst := msgpackServer(t, encodeEnvelope(t, oneValueColumn))

	frame, err := queryMsgpack(context.Background(), inst, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	wantNoNotices(t, frame)
}

func TestQueryMsgpackTruncatedResultRejected(t *testing.T) {
	inst := msgpackServer(t, encodeEnvelopeWith(t, oneValueColumn, "truncated", true, "truncation_reason", "context canceled"))

	if _, err := queryMsgpack(context.Background(), inst, "SELECT 1"); err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("truncated msgpack result: got %v, want an error with the reason", err)
	}
}

// arrowServer streams one float64 column of n rows and sets the trailers
// after the body, as Arc does. Every completeness trailer is announced.
func arrowServer(t *testing.T, n int, trailers map[string]string) *ArcInstanceSettings {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Trailer", arcStreamTruncatedTrailer+", "+arcRowsCappedTrailer)
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
		schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Float64}}, nil)
		b := array.NewFloat64Builder(memory.DefaultAllocator)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(float64(i))
		}
		col := b.NewArray()
		defer col.Release()
		rec := array.NewRecord(schema, []arrow.Array{col}, int64(n))
		defer rec.Release()
		wr := ipc.NewWriter(w, ipc.WithSchema(schema))
		if err := wr.Write(rec); err != nil {
			t.Errorf("write record: %v", err)
		}
		if err := wr.Close(); err != nil {
			t.Errorf("close writer: %v", err)
		}
		// A real stream's last batch reaches the client well before the
		// chunked terminator and trailers; without this pause the whole
		// response is buffered at once and the client sees EOF early.
		w.(http.Flusher).Flush()
		time.Sleep(50 * time.Millisecond)
		for k, v := range trailers {
			w.Header().Set(k, v)
		}
	}))
	t.Cleanup(srv.Close)
	return newTestInstance(t, srv.URL, ProtocolArrow)
}

func TestQueryArrowCappedResultRendersWithNotice(t *testing.T) {
	inst := arrowServer(t, 2, map[string]string{arcRowsCappedTrailer: "2"})

	frame, err := queryArrow(context.Background(), inst, "SELECT value FROM t")
	if err != nil {
		t.Fatalf("a capped result is valid up to the cap, got error: %v", err)
	}
	if frame.Rows() != 2 {
		t.Fatalf("rows = %d, want 2", frame.Rows())
	}
	wantCapNotice(t, frame, "2")
}

// Arc before 27.01.1 sent the announced cap trailer empty on every response.
func TestQueryArrowEmptyCapTrailerIsNotCapped(t *testing.T) {
	inst := arrowServer(t, 2, map[string]string{arcRowsCappedTrailer: ""})

	frame, err := queryArrow(context.Background(), inst, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	wantNoNotices(t, frame)
}

func TestQueryArrowTruncatedTrailerRejected(t *testing.T) {
	inst := arrowServer(t, 2, map[string]string{arcStreamTruncatedTrailer: "context canceled"})

	if _, err := queryArrow(context.Background(), inst, "SELECT 1"); err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("truncated Arrow trailer: got %v, want an error with the reason", err)
	}
}

func TestMergeFramesKeepsEveryChunksNotices(t *testing.T) {
	v := 1.0
	base := data.NewFrame("", data.NewField("value", nil, []*float64{&v}))
	capped := data.NewFrame("", data.NewField("value", nil, []*float64{&v}))
	addNotice(capped, rowCapNotice(5))
	alsoCapped := data.NewFrame("", data.NewField("value", nil, []*float64{&v}))
	addNotice(alsoCapped, rowCapNotice(5))

	merged := mergeFrames([]*data.Frame{base, capped, alsoCapped})
	if merged.Rows() != 3 {
		t.Fatalf("rows = %d, want 3", merged.Rows())
	}
	wantCapNotice(t, merged, "5")
}

// A split query runs one request per chunk; a cap on any chunk must reach the
// panel through the merge and the split path's own query meta.
func TestSplitQueryKeepsChunkRowCapNotice(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if requests.Add(1) == 2 {
			_, _ = io.WriteString(w, `{"columns":["value"],"data":[[1]],"rows_capped":true,"row_cap":1}`)
			return
		}
		_, _ = io.WriteString(w, `{"columns":["value"],"data":[[1]]}`)
	}))
	defer srv.Close()
	inst := newTestInstance(t, srv.URL, ProtocolJSON)

	qJSON, _ := json.Marshal(map[string]any{
		"sql":           "SELECT value FROM t WHERE $__timeFilter(time)",
		"splitDuration": "1h",
		"format":        "table",
	})
	end := time.Date(2026, 11, 5, 3, 0, 0, 0, time.UTC)
	resp := (&ArcDatasource{}).query(context.Background(), inst, backend.DataQuery{
		RefID:     "A",
		JSON:      qJSON,
		TimeRange: backend.TimeRange{From: end.Add(-3 * time.Hour), To: end},
	})
	if resp.Error != nil {
		t.Fatalf("query: %v", resp.Error)
	}
	if got := requests.Load(); got < 2 {
		t.Fatalf("requests = %d: the query was not split", got)
	}
	if len(resp.Frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(resp.Frames))
	}
	wantCapNotice(t, resp.Frames[0], "1")
	if resp.Frames[0].Meta.ExecutedQueryString == "" {
		t.Error("split query lost its executed query string")
	}
}
