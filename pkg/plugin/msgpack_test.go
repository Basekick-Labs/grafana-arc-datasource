package plugin

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/vmihailenco/msgpack/v5"
)

// testMaxElems is the per-array element cap passed to MsgpackToDataFrame in
// tests; generous enough for every well-formed fixture.
const testMaxElems = 1 << 20

// mpColumn is one column of a test envelope. Values use nil for msgpack nil
// cells; time.Time values are encoded as the msgpack timestamp extension.
type mpColumn struct {
	name   string
	typ    string
	values []interface{}
}

// encodeEnvelope builds a wire-accurate Arc msgpack query envelope: the
// 7-key map with fixed key order and COLUMN-major data arrays, matching
// Arc's streamMsgPackFromBatches.
func encodeEnvelope(t *testing.T, cols []mpColumn) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)

	rowCount := 0
	if len(cols) > 0 {
		rowCount = len(cols[0].values)
	}

	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
	}

	check(enc.EncodeMapLen(7))
	check(enc.EncodeString("success"))
	check(enc.EncodeBool(true))
	check(enc.EncodeString("columns"))
	check(enc.EncodeArrayLen(len(cols)))
	for _, c := range cols {
		check(enc.EncodeString(c.name))
	}
	check(enc.EncodeString("types"))
	check(enc.EncodeArrayLen(len(cols)))
	for _, c := range cols {
		check(enc.EncodeString(c.typ))
	}
	check(enc.EncodeString("data"))
	check(enc.EncodeArrayLen(len(cols)))
	for _, c := range cols {
		check(enc.EncodeArrayLen(len(c.values)))
		for _, v := range c.values {
			if v == nil {
				check(enc.EncodeNil())
				continue
			}
			if ts, ok := v.(time.Time); ok {
				check(enc.EncodeTime(ts))
				continue
			}
			check(enc.Encode(v))
		}
	}
	check(enc.EncodeString("row_count"))
	check(enc.EncodeUint64(uint64(rowCount)))
	check(enc.EncodeString("execution_time_ms"))
	check(enc.EncodeUint64(3))
	check(enc.EncodeString("timestamp"))
	check(enc.EncodeString("2026-09-02T12:00:00Z"))

	return buf.Bytes()
}

func TestMsgpackToDataFrame_Basic(t *testing.T) {
	ts1 := time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC)
	ts2 := ts1.Add(time.Minute)
	body := encodeEnvelope(t, []mpColumn{
		{name: "time", typ: "timestamp[us]", values: []interface{}{ts1, ts2}},
		{name: "host", typ: "utf8", values: []interface{}{"a", "b"}},
		{name: "value", typ: "float64", values: []interface{}{1.5, nil}},
		{name: "count", typ: "int64", values: []interface{}{int64(7), int64(9)}},
	})

	frame, err := MsgpackToDataFrame(bytes.NewReader(body), testMaxElems)
	if err != nil {
		t.Fatalf("MsgpackToDataFrame: %v", err)
	}
	if got := len(frame.Fields); got != 4 {
		t.Fatalf("expected 4 fields, got %d", got)
	}
	if frame.Fields[0].Type() != data.FieldTypeNullableTime {
		t.Errorf("time field type = %v", frame.Fields[0].Type())
	}
	if got := *frame.Fields[0].At(1).(*time.Time); !got.Equal(ts2) {
		t.Errorf("time[1] = %v, want %v", got, ts2)
	}
	if got := *frame.Fields[1].At(0).(*string); got != "a" {
		t.Errorf("host[0] = %q", got)
	}
	if frame.Fields[2].At(1).(*float64) != nil {
		t.Error("value[1] should be nil (msgpack nil cell)")
	}
	if got := *frame.Fields[2].At(0).(*float64); got != 1.5 {
		t.Errorf("value[0] = %v", got)
	}
	// int64 promotes to *float64 exactly like the Arrow path.
	if frame.Fields[3].Type() != data.FieldTypeNullableFloat64 {
		t.Errorf("count field type = %v, want nullable float64 (int64 promotion)", frame.Fields[3].Type())
	}
	if got := *frame.Fields[3].At(1).(*float64); got != 9 {
		t.Errorf("count[1] = %v", got)
	}
}

func TestMsgpackToDataFrame_AllScalarTypes(t *testing.T) {
	body := encodeEnvelope(t, []mpColumn{
		{name: "i8", typ: "int8", values: []interface{}{int8(-8)}},
		{name: "i16", typ: "int16", values: []interface{}{int16(-16)}},
		{name: "i32", typ: "int32", values: []interface{}{int32(-32)}},
		{name: "u8", typ: "uint8", values: []interface{}{uint8(8)}},
		{name: "u16", typ: "uint16", values: []interface{}{uint16(16)}},
		{name: "u32", typ: "uint32", values: []interface{}{uint32(32)}},
		{name: "u64", typ: "uint64", values: []interface{}{uint64(64)}},
		{name: "f32", typ: "float32", values: []interface{}{float32(0.5)}},
		{name: "b", typ: "bool", values: []interface{}{true}},
		{name: "bin", typ: "binary", values: []interface{}{[]byte("raw")}},
		{name: "ls", typ: "large_utf8", values: []interface{}{"big"}},
		{name: "se", typ: "string_encoded", values: []interface{}{"2026-01-01"}},
		{name: "lst", typ: "list", values: []interface{}{"[1, 2]"}},
		{name: "unk", typ: "unknown:dictionary<...>", values: []interface{}{"enum_val"}},
	})

	frame, err := MsgpackToDataFrame(bytes.NewReader(body), testMaxElems)
	if err != nil {
		t.Fatalf("MsgpackToDataFrame: %v", err)
	}

	wantTypes := []data.FieldType{
		data.FieldTypeNullableInt8,
		data.FieldTypeNullableInt16,
		data.FieldTypeNullableInt32,
		data.FieldTypeNullableUint8,
		data.FieldTypeNullableUint16,
		data.FieldTypeNullableUint32,
		data.FieldTypeNullableFloat64, // uint64 promotion
		data.FieldTypeNullableFloat32,
		data.FieldTypeNullableBool,
		data.FieldTypeNullableString, // binary
		data.FieldTypeNullableString,
		data.FieldTypeNullableString,
		data.FieldTypeNullableString,
		data.FieldTypeNullableString,
	}
	for i, want := range wantTypes {
		if got := frame.Fields[i].Type(); got != want {
			t.Errorf("field %d (%s) type = %v, want %v", i, frame.Fields[i].Name, got, want)
		}
	}
	if got := *frame.Fields[6].At(0).(*float64); got != 64 {
		t.Errorf("u64[0] = %v", got)
	}
	if got := *frame.Fields[9].At(0).(*string); got != "cmF3" {
		t.Errorf("bin[0] = %q, want base64 of \"raw\"", got)
	}
	if got := *frame.Fields[13].At(0).(*string); got != "enum_val" {
		t.Errorf("unk[0] = %q", got)
	}
}

func TestMsgpackToDataFrame_NullsAcrossTypes(t *testing.T) {
	ts := time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC)
	body := encodeEnvelope(t, []mpColumn{
		{name: "t", typ: "timestamp[ns]", values: []interface{}{nil, ts}},
		{name: "s", typ: "utf8", values: []interface{}{nil, "x"}},
		{name: "n", typ: "int64", values: []interface{}{nil, int64(1)}},
		{name: "b", typ: "bool", values: []interface{}{nil, false}},
	})
	frame, err := MsgpackToDataFrame(bytes.NewReader(body), testMaxElems)
	if err != nil {
		t.Fatalf("MsgpackToDataFrame: %v", err)
	}
	for i, f := range frame.Fields {
		if f.At(0) != nil {
			// At returns a typed nil pointer wrapped in interface — compare
			// via the concrete accessor instead.
			switch v := f.At(0).(type) {
			case *time.Time:
				if v != nil {
					t.Errorf("field %d row 0 should be nil", i)
				}
			case *string:
				if v != nil {
					t.Errorf("field %d row 0 should be nil", i)
				}
			case *float64:
				if v != nil {
					t.Errorf("field %d row 0 should be nil", i)
				}
			case *bool:
				if v != nil {
					t.Errorf("field %d row 0 should be nil", i)
				}
			}
		}
	}
	if got := *frame.Fields[0].At(1).(*time.Time); !got.Equal(ts) {
		t.Errorf("t[1] = %v", got)
	}
}

func TestMsgpackToDataFrame_EmptyResult(t *testing.T) {
	// Arc's "no files found yet" envelope: columns=[] types=[] data=[].
	body := encodeEnvelope(t, nil)
	frame, err := MsgpackToDataFrame(bytes.NewReader(body), testMaxElems)
	if err != nil {
		t.Fatalf("MsgpackToDataFrame: %v", err)
	}
	if len(frame.Fields) != 0 {
		t.Errorf("expected empty frame, got %d fields", len(frame.Fields))
	}
}

func TestMsgpackToDataFrame_ErrorEnvelope(t *testing.T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	_ = enc.EncodeMapLen(4)
	_ = enc.EncodeString("success")
	_ = enc.EncodeBool(false)
	_ = enc.EncodeString("error")
	_ = enc.EncodeString("Binder Error: no such column")
	_ = enc.EncodeString("execution_time_ms")
	_ = enc.EncodeUint64(1)
	_ = enc.EncodeString("timestamp")
	_ = enc.EncodeString("2026-09-02T12:00:00Z")

	_, err := MsgpackToDataFrame(bytes.NewReader(buf.Bytes()), testMaxElems)
	if err == nil || !strings.Contains(err.Error(), "Binder Error") {
		t.Fatalf("expected the server error surfaced, got %v", err)
	}
}

func TestMsgpackToDataFrame_DataBeforeColumnsRejected(t *testing.T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	_ = enc.EncodeMapLen(1)
	_ = enc.EncodeString("data")
	_ = enc.EncodeArrayLen(0)

	_, err := MsgpackToDataFrame(bytes.NewReader(buf.Bytes()), testMaxElems)
	if err == nil || !strings.Contains(err.Error(), "data precedes") {
		t.Fatalf("expected malformed-envelope error, got %v", err)
	}
}

func TestMsgpackToDataFrame_ColumnCountMismatchRejected(t *testing.T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	_ = enc.EncodeMapLen(3)
	_ = enc.EncodeString("columns")
	_ = enc.EncodeArrayLen(2)
	_ = enc.EncodeString("a")
	_ = enc.EncodeString("b")
	_ = enc.EncodeString("types")
	_ = enc.EncodeArrayLen(2)
	_ = enc.EncodeString("utf8")
	_ = enc.EncodeString("utf8")
	_ = enc.EncodeString("data")
	_ = enc.EncodeArrayLen(1) // only one column array for two declared columns
	_ = enc.EncodeArrayLen(0)

	_, err := MsgpackToDataFrame(bytes.NewReader(buf.Bytes()), testMaxElems)
	if err == nil || !strings.Contains(err.Error(), "data columns") {
		t.Fatalf("expected column-count mismatch error, got %v", err)
	}
}

func TestMsgpackToDataFrame_RaggedRowsRejected(t *testing.T) {
	body := encodeEnvelope(t, []mpColumn{
		{name: "a", typ: "utf8", values: []interface{}{"x", "y"}},
		{name: "b", typ: "utf8", values: []interface{}{"only-one"}},
	})
	_, err := MsgpackToDataFrame(bytes.NewReader(body), testMaxElems)
	if err == nil || !strings.Contains(err.Error(), "rows") {
		t.Fatalf("expected ragged-row error, got %v", err)
	}
}

func TestMsgpackToDataFrame_MissingDataRejected(t *testing.T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	_ = enc.EncodeMapLen(1)
	_ = enc.EncodeString("success")
	_ = enc.EncodeBool(true)

	_, err := MsgpackToDataFrame(bytes.NewReader(buf.Bytes()), testMaxElems)
	if err == nil || !strings.Contains(err.Error(), "no data field") {
		t.Fatalf("expected missing-data error, got %v", err)
	}
}

// TestMsgpackToDataFrame_NilArraysRejected pins the H1 hardening: msgpack
// nil where an array is expected must be a decode error, not a panic
// (DecodeArrayLen returns -1 for nil, and make([]T, -1) panics).
func TestMsgpackToDataFrame_NilArraysRejected(t *testing.T) {
	cases := []struct {
		name   string
		encode func(enc *msgpack.Encoder)
	}{
		{name: "nil columns", encode: func(enc *msgpack.Encoder) {
			_ = enc.EncodeMapLen(1)
			_ = enc.EncodeString("columns")
			_ = enc.EncodeNil()
		}},
		{name: "nil column array in data", encode: func(enc *msgpack.Encoder) {
			_ = enc.EncodeMapLen(3)
			_ = enc.EncodeString("columns")
			_ = enc.EncodeArrayLen(1)
			_ = enc.EncodeString("a")
			_ = enc.EncodeString("types")
			_ = enc.EncodeArrayLen(1)
			_ = enc.EncodeString("utf8")
			_ = enc.EncodeString("data")
			_ = enc.EncodeArrayLen(1)
			_ = enc.EncodeNil()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			enc := msgpack.NewEncoder(&buf)
			tc.encode(enc)
			_, err := MsgpackToDataFrame(bytes.NewReader(buf.Bytes()), testMaxElems)
			if err == nil {
				t.Fatal("expected an error for nil in array position")
			}
		})
	}
}

// TestMsgpackToDataFrame_HostileLengthRejected pins the M1 hardening: a
// length header claiming billions of elements must be rejected before
// allocation, since it costs the attacker only 5 wire bytes.
func TestMsgpackToDataFrame_HostileLengthRejected(t *testing.T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	_ = enc.EncodeMapLen(1)
	_ = enc.EncodeString("columns")
	_ = enc.EncodeArrayLen(1 << 30) // claims 1B strings, no bytes behind it
	_, err := MsgpackToDataFrame(bytes.NewReader(buf.Bytes()), testMaxElems)
	if err == nil || !strings.Contains(err.Error(), "element limit") {
		t.Fatalf("expected element-limit rejection, got %v", err)
	}
}

// TestMsgpackToDataFrame_BinaryIsBase64 pins protocol parity for BINARY
// columns: the Arrow path renders base64 (array.Binary.ValueStr), so the
// msgpack path must too.
func TestMsgpackToDataFrame_BinaryIsBase64(t *testing.T) {
	body := encodeEnvelope(t, []mpColumn{
		{name: "bin", typ: "binary", values: []interface{}{[]byte{0xde, 0xad, 0xbe, 0xef}}},
	})
	frame, err := MsgpackToDataFrame(bytes.NewReader(body), testMaxElems)
	if err != nil {
		t.Fatalf("MsgpackToDataFrame: %v", err)
	}
	if got := *frame.Fields[0].At(0).(*string); got != "3q2+7w==" {
		t.Errorf("bin[0] = %q, want base64 \"3q2+7w==\"", got)
	}
}

func TestParseArcError_MsgpackBody(t *testing.T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	_ = enc.EncodeMapLen(2)
	_ = enc.EncodeString("success")
	_ = enc.EncodeBool(false)
	_ = enc.EncodeString("error")
	_ = enc.EncodeString("Query timed out")

	got := parseArcError(504, buf.Bytes())
	if !strings.Contains(got, "504") || !strings.Contains(got, "Query timed out") {
		t.Errorf("parseArcError = %q", got)
	}
}

func TestParseArcError_JSONStillWorks(t *testing.T) {
	got := parseArcError(403, []byte(`{"success":false,"error":"Permission denied: read required"}`))
	if !strings.Contains(got, "Permission denied") {
		t.Errorf("parseArcError = %q", got)
	}
}

// newTestInstance builds an ArcInstanceSettings against an httptest server
// via the real factory, so the SSRF dial policy, semaphore, and response cap
// all run in the test.
func newTestInstance(t *testing.T, serverURL, protocol string) *ArcInstanceSettings {
	t.Helper()
	jsonData, err := jsonMarshal(map[string]any{
		"url":      serverURL,
		"protocol": protocol,
	})
	if err != nil {
		t.Fatalf("marshal settings: %v", err)
	}
	inst, err := newArcInstance(t.Context(), backend.DataSourceInstanceSettings{
		JSONData:                jsonData,
		DecryptedSecureJSONData: map[string]string{"apiKey": "test-key"},
	})
	if err != nil {
		t.Fatalf("newArcInstance: %v", err)
	}
	return inst.(*ArcInstanceSettings)
}

func TestQueryMsgpack_EndToEnd(t *testing.T) {
	ts := time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC)
	var gotPath, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/msgpack")
		_, _ = w.Write(encodeEnvelope(t, []mpColumn{
			{name: "time", typ: "timestamp[us]", values: []interface{}{ts}},
			{name: "value", typ: "float64", values: []interface{}{42.0}},
		}))
	}))
	defer srv.Close()

	inst := newTestInstance(t, srv.URL, ProtocolMsgpack)
	frame, err := queryMsgpack(context.Background(), inst, "SELECT 1")
	if err != nil {
		t.Fatalf("queryMsgpack: %v", err)
	}
	if gotPath != "/api/v1/query/msgpack" {
		t.Errorf("request path = %q", gotPath)
	}
	if gotAuth != "Bearer test-key" {
		t.Errorf("auth header = %q", gotAuth)
	}
	if frame.Rows() != 1 || len(frame.Fields) != 2 {
		t.Fatalf("frame shape = %d rows × %d fields", frame.Rows(), len(frame.Fields))
	}
	if got := *frame.Fields[1].At(0).(*float64); got != 42.0 {
		t.Errorf("value = %v", got)
	}
	if frame.Meta == nil || frame.Meta.ExecutedQueryString != "SELECT 1" {
		t.Error("frame meta should carry the executed SQL")
	}
}

func TestQueryMsgpack_ServerErrorSurfaced(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		enc := msgpack.NewEncoder(&buf)
		_ = enc.EncodeMapLen(2)
		_ = enc.EncodeString("success")
		_ = enc.EncodeBool(false)
		_ = enc.EncodeString("error")
		_ = enc.EncodeString("Catalog Error: table missing")
		w.Header().Set("Content-Type", "application/msgpack")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write(buf.Bytes())
	}))
	defer srv.Close()

	inst := newTestInstance(t, srv.URL, ProtocolMsgpack)
	_, err := queryMsgpack(context.Background(), inst, "SELECT * FROM missing")
	if err == nil || !strings.Contains(err.Error(), "Catalog Error") {
		t.Fatalf("expected msgpack-decoded server error, got %v", err)
	}
}

func TestNewArcInstance_ProtocolResolution(t *testing.T) {
	cases := []struct {
		name     string
		settings map[string]any
		want     string
		wantErr  bool
	}{
		{name: "default is arrow", settings: map[string]any{}, want: ProtocolArrow},
		{name: "explicit msgpack", settings: map[string]any{"protocol": "msgpack"}, want: ProtocolMsgpack},
		{name: "explicit json", settings: map[string]any{"protocol": "json"}, want: ProtocolJSON},
		{name: "legacy useArrow false maps to json", settings: map[string]any{"useArrow": false}, want: ProtocolJSON},
		{name: "legacy useArrow true maps to arrow", settings: map[string]any{"useArrow": true}, want: ProtocolArrow},
		{name: "protocol wins over legacy toggle", settings: map[string]any{"protocol": "msgpack", "useArrow": false}, want: ProtocolMsgpack},
		{name: "unknown protocol rejected", settings: map[string]any{"protocol": "flatbuffers"}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.settings["url"] = "https://arc.example.com"
			jsonData, _ := jsonMarshal(tc.settings)
			inst, err := newArcInstance(t.Context(), backend.DataSourceInstanceSettings{
				JSONData:                jsonData,
				DecryptedSecureJSONData: map[string]string{"apiKey": "k"},
			})
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("newArcInstance: %v", err)
			}
			if got := inst.(*ArcInstanceSettings).settings.Protocol; got != tc.want {
				t.Errorf("protocol = %q, want %q", got, tc.want)
			}
		})
	}
}
