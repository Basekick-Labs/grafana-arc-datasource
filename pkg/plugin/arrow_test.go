package plugin

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// TestAppendRecordToDataFrame_Float64 exercises the bulk-slice fast path on
// the all-valid branch — values must land in the destination field in order.
func TestAppendRecordToDataFrame_Float64(t *testing.T) {
	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.Float64Builder).AppendValues([]float64{1.5, 2.5, 3.5}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	frame := newFrameFromArrowSchema(schema)
	if err := appendRecordToDataFrame(frame, rec); err != nil {
		t.Fatalf("appendRecordToDataFrame: %v", err)
	}
	if frame.Rows() != 3 {
		t.Fatalf("expected 3 rows, got %d", frame.Rows())
	}
	for i, want := range []float64{1.5, 2.5, 3.5} {
		got := frame.Fields[0].At(i).(*float64)
		if got == nil || *got != want {
			t.Errorf("row %d: expected %f, got %v", i, want, got)
		}
	}
}

// TestAppendRecordToDataFrame_Int64_PromotedToFloat64 locks in the
// Grafana-compatibility promotion: Arrow INT64 → data.Field float64.
func TestAppendRecordToDataFrame_Int64_PromotedToFloat64(t *testing.T) {
	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 20, 30}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	frame := newFrameFromArrowSchema(schema)
	if err := appendRecordToDataFrame(frame, rec); err != nil {
		t.Fatalf("appendRecordToDataFrame: %v", err)
	}
	// The destination field is *float64, not *int64 — that's the promotion.
	got := frame.Fields[0].At(0)
	if _, ok := got.(*float64); !ok {
		t.Fatalf("expected *float64 (promoted), got %T", got)
	}
	for i, want := range []float64{10, 20, 30} {
		v := frame.Fields[0].At(i).(*float64)
		if v == nil || *v != want {
			t.Errorf("row %d: expected %f, got %v", i, want, v)
		}
	}
}

// TestAppendRecordToDataFrame_WithNulls verifies the slow path (allValid=false).
func TestAppendRecordToDataFrame_WithNulls(t *testing.T) {
	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	// validity bitmap: [valid, null, valid]
	b.Field(0).(*array.Float64Builder).AppendValues([]float64{1.0, 0.0, 3.0}, []bool{true, false, true})
	rec := b.NewRecord()
	defer rec.Release()

	frame := newFrameFromArrowSchema(schema)
	if err := appendRecordToDataFrame(frame, rec); err != nil {
		t.Fatalf("appendRecordToDataFrame: %v", err)
	}
	if v := frame.Fields[0].At(0).(*float64); v == nil || *v != 1.0 {
		t.Errorf("row 0 should be 1.0, got %v", v)
	}
	if v := frame.Fields[0].At(1).(*float64); v != nil {
		t.Errorf("row 1 should be null, got %v", *v)
	}
	if v := frame.Fields[0].At(2).(*float64); v == nil || *v != 3.0 {
		t.Errorf("row 2 should be 3.0, got %v", v)
	}
}

// TestAppendRecordToDataFrame_Timestamp exercises the bulk timestamp accessor
// and locks in the millisecond unit ToTime conversion (Arc emits TIMESTAMP[ms]).
func TestAppendRecordToDataFrame_Timestamp(t *testing.T) {
	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: &arrow.TimestampType{Unit: arrow.Millisecond}, Nullable: true},
	}, nil)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	// 2026-05-14T12:00:00Z and 2026-05-14T13:00:00Z, in ms since epoch.
	t0 := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	b.Field(0).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{
		arrow.Timestamp(t0.UnixMilli()),
		arrow.Timestamp(t1.UnixMilli()),
	}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	frame := newFrameFromArrowSchema(schema)
	if err := appendRecordToDataFrame(frame, rec); err != nil {
		t.Fatalf("appendRecordToDataFrame: %v", err)
	}
	got0 := frame.Fields[0].At(0).(*time.Time)
	if got0 == nil || !got0.Equal(t0) {
		t.Errorf("row 0: expected %v, got %v", t0, got0)
	}
	got1 := frame.Fields[0].At(1).(*time.Time)
	if got1 == nil || !got1.Equal(t1) {
		t.Errorf("row 1: expected %v, got %v", t1, got1)
	}
}

// TestAppendRecordToDataFrame_MultiBatch covers the case where two record
// batches are appended in sequence — startIdx advancement matters.
func TestAppendRecordToDataFrame_MultiBatch(t *testing.T) {
	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	frame := newFrameFromArrowSchema(schema)

	for _, batch := range [][]int64{{1, 2}, {3, 4, 5}} {
		b := array.NewRecordBuilder(pool, schema)
		b.Field(0).(*array.Int64Builder).AppendValues(batch, nil)
		rec := b.NewRecord()
		if err := appendRecordToDataFrame(frame, rec); err != nil {
			t.Fatalf("batch %v: %v", batch, err)
		}
		rec.Release()
		b.Release()
	}
	if frame.Rows() != 5 {
		t.Fatalf("expected 5 rows after two batches, got %d", frame.Rows())
	}
	for i, want := range []float64{1, 2, 3, 4, 5} {
		v := frame.Fields[0].At(i).(*float64)
		if v == nil || *v != want {
			t.Errorf("row %d: expected %f, got %v", i, want, v)
		}
	}
}

// TestAppendRecordToDataFrame_EmptyRecord verifies the zero-row guard.
func TestAppendRecordToDataFrame_EmptyRecord(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	rec := b.NewRecord()
	defer rec.Release()

	frame := newFrameFromArrowSchema(schema)
	if err := appendRecordToDataFrame(frame, rec); err != nil {
		t.Fatalf("appendRecordToDataFrame: %v", err)
	}
	if frame.Rows() != 0 {
		t.Errorf("expected 0 rows, got %d", frame.Rows())
	}
}

// TestAppendRecordToDataFrame_ZeroFields locks in the zero-column-schema
// guard — frame.Fields[0] used to be accessed unconditionally and would have
// panicked on an Arrow record with no columns (gemini review fixup).
func TestAppendRecordToDataFrame_ZeroFields(t *testing.T) {
	// Build a frame with zero fields directly (no schema needed).
	frame := data.NewFrame("")
	// Build a record with one column so the for-loop has something — but the
	// frame has no fields so we should bail early without panicking on
	// frame.Fields[0].
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	rec := b.NewRecord()
	defer rec.Release()
	// Must not panic.
	if err := appendRecordToDataFrame(frame, rec); err != nil {
		t.Fatalf("expected nil error on zero-field frame, got %v", err)
	}
}

// TestAppendRecordToDataFrame_String verifies the string column path (no
// bulk accessor in Arrow — per-row Value(i)).
func TestAppendRecordToDataFrame_String(t *testing.T) {
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "host", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	frame := newFrameFromArrowSchema(schema)
	if err := appendRecordToDataFrame(frame, rec); err != nil {
		t.Fatalf("appendRecordToDataFrame: %v", err)
	}
	for i, want := range []string{"a", "b", "c"} {
		v := frame.Fields[0].At(i).(*string)
		if v == nil || *v != want {
			t.Errorf("row %d: expected %q, got %v", i, want, v)
		}
	}
}

// TestNewFrameFromArrowSchema_AllTypes locks in the schema-to-field type
// mapping, including the int64/uint64 → float64 promotion.
func TestNewFrameFromArrowSchema_AllTypes(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "b", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "t", Type: &arrow.TimestampType{Unit: arrow.Millisecond}, Nullable: true},
	}, nil)
	frame := newFrameFromArrowSchema(schema)

	for _, tc := range []struct {
		name string
		want data.FieldType
	}{
		{"i64", data.FieldTypeNullableFloat64}, // promoted
		{"u64", data.FieldTypeNullableFloat64}, // promoted
		{"f64", data.FieldTypeNullableFloat64},
		{"s", data.FieldTypeNullableString},
		{"b", data.FieldTypeNullableBool},
		{"t", data.FieldTypeNullableTime},
	} {
		f, _ := frame.FieldByName(tc.name)
		if f == nil {
			t.Fatalf("missing field %q", tc.name)
		}
		if f.Type() != tc.want {
			t.Errorf("field %q: expected type %s, got %s", tc.name, tc.want, f.Type())
		}
	}
}
