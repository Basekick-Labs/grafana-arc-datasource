package plugin

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// Live end-to-end tests against a real Arc server. Skipped unless both env
// vars are set:
//
//	ARC_E2E_URL=http://127.0.0.1:8299 ARC_E2E_TOKEN=<token> go test -run Live ./pkg/plugin/
//
// The server needs the duckdb_arrow build (any shipped Arc artifact) and at
// least one measurement written; the tests only assume a queryable server,
// not specific data.
func liveInstance(t *testing.T, protocol string) *ArcInstanceSettings {
	t.Helper()
	url := os.Getenv("ARC_E2E_URL")
	token := os.Getenv("ARC_E2E_TOKEN")
	if url == "" || token == "" {
		t.Skip("ARC_E2E_URL / ARC_E2E_TOKEN not set")
	}
	jsonData, _ := jsonMarshal(map[string]any{"url": url, "protocol": protocol})
	inst, err := newArcInstance(t.Context(), backend.DataSourceInstanceSettings{
		JSONData:                jsonData,
		DecryptedSecureJSONData: map[string]string{"apiKey": token},
	})
	if err != nil {
		t.Fatalf("newArcInstance: %v", err)
	}
	return inst.(*ArcInstanceSettings)
}

// TestLive_HealthProbeAllProtocols locks in the CheckHealth fix: SELECT 1
// must succeed on every protocol (SHOW DATABASES does not on Arrow).
func TestLive_HealthProbeAllProtocols(t *testing.T) {
	for _, proto := range []string{ProtocolArrow, ProtocolMsgpack, ProtocolJSON} {
		t.Run(proto, func(t *testing.T) {
			inst := liveInstance(t, proto)
			frame, err := executeProtocolQuery(context.Background(), inst, "SELECT 1")
			if err != nil {
				t.Fatalf("SELECT 1 via %s: %v", proto, err)
			}
			if frame.Rows() != 1 {
				t.Fatalf("expected 1 row, got %d", frame.Rows())
			}
		})
	}
}

// TestLive_MsgpackTypedColumns exercises the msgpack decoder against real
// DuckDB output: int64 promotion, float64, null, string, and a timestamp.
func TestLive_MsgpackTypedColumns(t *testing.T) {
	inst := liveInstance(t, ProtocolMsgpack)
	frame, err := queryMsgpack(context.Background(), inst,
		"SELECT CAST(7 AS BIGINT) AS big, 1.5 AS f, CAST(NULL AS DOUBLE) AS nul, 'txt' AS s, TIMESTAMP '2026-09-02 10:00:00' AS t")
	if err != nil {
		t.Fatalf("queryMsgpack: %v", err)
	}
	if frame.Rows() != 1 || len(frame.Fields) != 5 {
		t.Fatalf("frame shape = %d rows × %d fields", frame.Rows(), len(frame.Fields))
	}
	if frame.Fields[0].Type() != data.FieldTypeNullableFloat64 {
		t.Errorf("BIGINT column type = %v, want nullable float64 (promotion)", frame.Fields[0].Type())
	}
	if got := *frame.Fields[0].At(0).(*float64); got != 7 {
		t.Errorf("big = %v", got)
	}
	if frame.Fields[2].At(0).(*float64) != nil {
		t.Error("nul should decode as nil")
	}
	if got := *frame.Fields[3].At(0).(*string); got != "txt" {
		t.Errorf("s = %q", got)
	}
	want := time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC)
	if got := *frame.Fields[4].At(0).(*time.Time); !got.Equal(want) {
		t.Errorf("t = %v, want %v", got, want)
	}
}

// TestLive_MsgpackShowDatabases pins the one capability msgpack has over
// Arrow: SHOW statements work.
func TestLive_MsgpackShowDatabases(t *testing.T) {
	inst := liveInstance(t, ProtocolMsgpack)
	frame, err := queryMsgpack(context.Background(), inst, "SHOW DATABASES")
	if err != nil {
		t.Fatalf("SHOW DATABASES via msgpack: %v", err)
	}
	if len(frame.Fields) == 0 {
		t.Fatal("expected at least one column from SHOW DATABASES")
	}
}

// TestLive_MsgpackErrorSurfaced verifies the msgpack error envelope path: a
// bad query must come back as a readable Arc error, not a decode failure.
// A syntax error is used because a missing table is NOT an error in Arc — it
// returns the empty "no files found yet" envelope with HTTP 200.
func TestLive_MsgpackErrorSurfaced(t *testing.T) {
	inst := liveInstance(t, ProtocolMsgpack)
	_, err := queryMsgpack(context.Background(), inst, "SELECT * FROM")
	if err == nil {
		t.Fatal("expected an error for invalid SQL")
	}
	if got := err.Error(); !containsAny(got, "Arc error", "Parser Error", "syntax") {
		t.Errorf("error not surfaced readably: %v", got)
	}
}

// TestLive_MsgpackMissingTableIsEmpty pins Arc's empty-envelope semantics:
// an unknown measurement yields a 200 with columns=[] data=[], which must
// decode to an empty frame rather than an error.
func TestLive_MsgpackMissingTableIsEmpty(t *testing.T) {
	inst := liveInstance(t, ProtocolMsgpack)
	frame, err := queryMsgpack(context.Background(), inst, "SELECT * FROM this_table_does_not_exist_xyz")
	if err != nil {
		t.Fatalf("expected empty frame for unknown measurement, got %v", err)
	}
	if len(frame.Fields) != 0 {
		t.Errorf("expected 0 fields, got %d", len(frame.Fields))
	}
}

// TestLive_ProtocolParity runs the same query through all three protocols
// and requires identical field types. DOUBLE is cast explicitly because a
// bare 2.5 literal is a DuckDB DECIMAL: Arrow and msgpack normalize decimals
// to numeric on the server, but Arc's JSON encoder stringifies them, so
// DECIMAL columns are a known typing gap on the JSON protocol only.
func TestLive_ProtocolParity(t *testing.T) {
	const sql = "SELECT CAST(42 AS BIGINT) AS n, CAST(2.5 AS DOUBLE) AS f, 'x' AS s"
	frames := map[string]*data.Frame{}
	for _, proto := range []string{ProtocolArrow, ProtocolMsgpack, ProtocolJSON} {
		inst := liveInstance(t, proto)
		frame, err := executeProtocolQuery(context.Background(), inst, sql)
		if err != nil {
			t.Fatalf("%s: %v", proto, err)
		}
		frames[proto] = frame
	}
	ref := frames[ProtocolArrow]
	for proto, frame := range frames {
		if len(frame.Fields) != len(ref.Fields) {
			t.Errorf("%s: %d fields, arrow has %d", proto, len(frame.Fields), len(ref.Fields))
			continue
		}
		for i := range frame.Fields {
			if frame.Fields[i].Type() != ref.Fields[i].Type() {
				t.Errorf("%s field %d type %v != arrow %v", proto, i, frame.Fields[i].Type(), ref.Fields[i].Type())
			}
		}
	}
}

// TestLive_DateParity verifies DATE columns decode to time fields with the
// same value on both binary protocols (Arrow date32 vs msgpack timestamp
// extension for date32).
func TestLive_DateParity(t *testing.T) {
	const sql = "SELECT DATE '2026-09-02' AS d"
	want := time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)
	for _, proto := range []string{ProtocolArrow, ProtocolMsgpack} {
		t.Run(proto, func(t *testing.T) {
			inst := liveInstance(t, proto)
			frame, err := executeProtocolQuery(context.Background(), inst, sql)
			if err != nil {
				t.Fatalf("%s: %v", proto, err)
			}
			if frame.Fields[0].Type() != data.FieldTypeNullableTime {
				t.Fatalf("%s: DATE column type = %v, want nullable time", proto, frame.Fields[0].Type())
			}
			if got := *frame.Fields[0].At(0).(*time.Time); !got.Equal(want) {
				t.Errorf("%s: d = %v, want %v", proto, got, want)
			}
		})
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
