package plugin

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// queryArrow executes a query against Arc's /api/v1/query/arrow endpoint and
// returns the decoded Grafana DataFrame. Streams the Arrow IPC response
// record-by-record and decodes columns via bulk slice accessors where the
// Arrow library supports them.
func queryArrow(ctx context.Context, settings *ArcInstanceSettings, sql string) (*data.Frame, error) {
	start := time.Now()
	body, err := settings.doRequest(ctx, "/api/v1/query/arrow", map[string]any{"sql": sql})
	if err != nil {
		return nil, err
	}
	defer body.Close()

	reader, err := ipc.NewReader(body)
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow reader: %w", err)
	}
	defer reader.Release()

	frame, err := frameForRecords(reader)
	if err != nil {
		return nil, err
	}

	duration := time.Since(start)
	log.DefaultLogger.Debug("Arrow query completed",
		"duration_ms", duration.Milliseconds(),
		"rows", frame.Rows(),
		"fields", len(frame.Fields),
	)

	frame.Meta = &data.FrameMeta{
		ExecutedQueryString: sql,
		Custom: map[string]interface{}{
			"executionTime": duration.Milliseconds(),
		},
	}

	return frame, nil
}

// frameForRecords creates a data.Frame from a stream of arrow.Records
// This is the FlightSQL approach that we know works
func frameForRecords(reader *ipc.Reader) (*data.Frame, error) {
	// Wait for first record to get schema
	if !reader.Next() {
		if reader.Err() != nil && reader.Err() != io.EOF {
			return nil, fmt.Errorf("error reading Arrow stream: %w", reader.Err())
		}
		return data.NewFrame(""), nil
	}

	// Create frame from schema
	record := reader.Record()
	schema := record.Schema()
	frame := newFrameFromArrowSchema(schema)

	// Process first record
	if err := appendRecordToDataFrame(frame, record); err != nil {
		record.Release()
		return nil, err
	}
	record.Release()

	// Process remaining records
	for reader.Next() {
		record := reader.Record()
		if err := appendRecordToDataFrame(frame, record); err != nil {
			record.Release()
			return nil, err
		}
		record.Release()
	}

	if reader.Err() != nil && reader.Err() != io.EOF {
		return nil, fmt.Errorf("error reading Arrow stream: %w", reader.Err())
	}

	log.DefaultLogger.Debug("Built frame from Arrow records",
		"fields", len(frame.Fields),
		"rows", frame.Rows(),
	)

	return frame, nil
}

// newFrameFromArrowSchema creates a data.Frame with empty fields from Arrow schema
func newFrameFromArrowSchema(schema *arrow.Schema) *data.Frame {
	fields := make([]*data.Field, schema.NumFields())
	for i, arrowField := range schema.Fields() {
		fields[i] = createEmptyField(arrowField)
	}
	return data.NewFrame("", fields...)
}

// createEmptyField creates an empty data.Field from an Arrow field.
//
// Fields are ALWAYS created as nullable (pointer-element slices), regardless
// of the Arrow schema's `f.Nullable` flag. Arc's Arrow schemas advertise
// non-nullable for columns that are nullable in practice (e.g. aggregates
// with all-null groups), and Arrow's underlying buffer at null positions is
// undefined. Honoring the schema's non-nullable claim let stale buffer bytes
// surface as real values in the dashboard — see R2-CR2 in the
// signing-readiness punch list. Coercing to nullable + emitting nil at null
// positions is the only safe shape.
//
// INT64/UINT64 are promoted to *float64 so Grafana's Stat/TimeSeries panels
// treat them as numeric value fields (DuckDB aggregates return int64 after
// Arc's decimal normalization; Grafana auto-detection requires float64).
//
// Unknown Arrow types fall back to *string so the column is still rendered
// even if the writer path can't decode it. The writer path matches this
// fallback (R2-HI12).
func createEmptyField(f arrow.Field) *data.Field {
	switch f.Type.ID() {
	case arrow.STRING:
		return data.NewField(f.Name, nil, []*string{})
	case arrow.FLOAT32:
		return data.NewField(f.Name, nil, []*float32{})
	case arrow.FLOAT64:
		return data.NewField(f.Name, nil, []*float64{})
	case arrow.INT8:
		return data.NewField(f.Name, nil, []*int8{})
	case arrow.INT16:
		return data.NewField(f.Name, nil, []*int16{})
	case arrow.INT32:
		return data.NewField(f.Name, nil, []*int32{})
	case arrow.INT64:
		return data.NewField(f.Name, nil, []*float64{})
	case arrow.UINT8:
		return data.NewField(f.Name, nil, []*uint8{})
	case arrow.UINT16:
		return data.NewField(f.Name, nil, []*uint16{})
	case arrow.UINT32:
		return data.NewField(f.Name, nil, []*uint32{})
	case arrow.UINT64:
		return data.NewField(f.Name, nil, []*float64{})
	case arrow.BOOL:
		return data.NewField(f.Name, nil, []*bool{})
	case arrow.TIMESTAMP:
		return data.NewField(f.Name, nil, []*time.Time{})
	default:
		// Fallback to nullable string for unsupported types — the writer
		// path's default branch must match this (R2-HI12).
		return data.NewField(f.Name, nil, []*string{})
	}
}

// appendRecordToDataFrame appends every column of an Arrow record to its
// corresponding data.Frame field. Each field is pre-extended by the record's
// row count so the per-row writes don't trigger repeated reflective slice
// reallocations (M21/P2 fix).
func appendRecordToDataFrame(frame *data.Frame, record arrow.Record) error {
	if record.NumRows() == 0 || len(frame.Fields) == 0 {
		return nil
	}
	rows := int(record.NumRows())
	startIdx := frame.Fields[0].Len()
	for i, col := range record.Columns() {
		field := frame.Fields[i]
		field.Extend(rows)
		if err := writeArrowColumnIntoField(field, col, startIdx); err != nil {
			return fmt.Errorf("failed to append column %s: %w", field.Name, err)
		}
	}
	return nil
}

// writeArrowColumnIntoField writes every value of an Arrow column into the
// destination field starting at startIdx. The field is assumed to have been
// pre-extended by the caller (see appendRecordToDataFrame).
//
// Each type-cast uses comma-ok so a schema-vs-concrete-type drift (extension
// types, dictionary-encoded strings, lists) routes to the string fallback
// (matching createEmptyField's *string default) rather than panicking the
// goroutine.
//
// Numeric and timestamp columns use Arrow's bulk slice accessors
// (Int64Values/Float64Values/TimestampValues) and short-circuit the null check
// when col.NullN() == 0 — significantly faster than per-row Value(i) +
// IsNull(i) on large batches.
//
// All destination fields are nullable (R2-CR2): Arc's schemas can advertise
// non-nullable for columns that contain nulls in practice, and Arrow's
// underlying buffer at null positions is undefined. Writers ALWAYS check
// IsNull and emit a typed nil pointer there.
func writeArrowColumnIntoField(field *data.Field, col arrow.Array, startIdx int) error {
	allValid := col.NullN() == 0
	switch col.DataType().ID() {
	case arrow.TIMESTAMP:
		arr, ok := col.(*array.Timestamp)
		if !ok {
			return writeUnsupportedAsString(field, col, startIdx)
		}
		ts, ok := col.DataType().(*arrow.TimestampType)
		if !ok {
			return writeUnsupportedAsString(field, col, startIdx)
		}
		return writeTimestampColumn(field, arr, ts.Unit, startIdx, allValid)
	case arrow.STRING:
		arr, ok := col.(*array.String)
		if !ok {
			return writeUnsupportedAsString(field, col, startIdx)
		}
		return writeStringColumn(field, arr, startIdx, allValid)
	case arrow.BOOL:
		arr, ok := col.(*array.Boolean)
		if !ok {
			return writeUnsupportedAsString(field, col, startIdx)
		}
		return writeBoolColumn(field, arr, startIdx, allValid)
	case arrow.FLOAT32:
		arr, ok := col.(*array.Float32)
		if !ok {
			return writeUnsupportedAsString(field, col, startIdx)
		}
		return writeNumericColumn[float32](field, arr, arr.Float32Values(), startIdx, allValid)
	case arrow.FLOAT64:
		arr, ok := col.(*array.Float64)
		if !ok {
			return writeUnsupportedAsString(field, col, startIdx)
		}
		return writeNumericColumn[float64](field, arr, arr.Float64Values(), startIdx, allValid)
	case arrow.INT8:
		arr, ok := col.(*array.Int8)
		if !ok {
			return writeUnsupportedAsString(field, col, startIdx)
		}
		return writeNumericColumn[int8](field, arr, arr.Int8Values(), startIdx, allValid)
	case arrow.INT16:
		arr, ok := col.(*array.Int16)
		if !ok {
			return writeUnsupportedAsString(field, col, startIdx)
		}
		return writeNumericColumn[int16](field, arr, arr.Int16Values(), startIdx, allValid)
	case arrow.INT32:
		arr, ok := col.(*array.Int32)
		if !ok {
			return writeUnsupportedAsString(field, col, startIdx)
		}
		return writeNumericColumn[int32](field, arr, arr.Int32Values(), startIdx, allValid)
	case arrow.INT64:
		arr, ok := col.(*array.Int64)
		if !ok {
			return writeUnsupportedAsString(field, col, startIdx)
		}
		return writePromotedColumn[int64](field, arr, arr.Int64Values(), startIdx, allValid)
	case arrow.UINT8:
		arr, ok := col.(*array.Uint8)
		if !ok {
			return writeUnsupportedAsString(field, col, startIdx)
		}
		return writeNumericColumn[uint8](field, arr, arr.Uint8Values(), startIdx, allValid)
	case arrow.UINT16:
		arr, ok := col.(*array.Uint16)
		if !ok {
			return writeUnsupportedAsString(field, col, startIdx)
		}
		return writeNumericColumn[uint16](field, arr, arr.Uint16Values(), startIdx, allValid)
	case arrow.UINT32:
		arr, ok := col.(*array.Uint32)
		if !ok {
			return writeUnsupportedAsString(field, col, startIdx)
		}
		return writeNumericColumn[uint32](field, arr, arr.Uint32Values(), startIdx, allValid)
	case arrow.UINT64:
		arr, ok := col.(*array.Uint64)
		if !ok {
			return writeUnsupportedAsString(field, col, startIdx)
		}
		return writePromotedColumn[uint64](field, arr, arr.Uint64Values(), startIdx, allValid)
	default:
		// Unsupported Arrow type: render via String() so the column is still
		// visible (matches createEmptyField's *string fallback — R2-HI12).
		return writeUnsupportedAsString(field, col, startIdx)
	}
}

// writeUnsupportedAsString renders an Arrow column the writer can't decode
// natively as the column's per-row String() representation. Lets the panel
// still display data for extension types / dictionary-encoded strings / lists
// instead of failing the whole query — schema-build path matches via
// createEmptyField's *string fallback (R2-HI12).
func writeUnsupportedAsString(field *data.Field, col arrow.Array, startIdx int) error {
	n := col.Len()
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			var s *string
			field.Set(startIdx+i, s)
			continue
		}
		// arrow.Array's ValueStr renders the i-th element per the type's stringer.
		v := col.ValueStr(i)
		field.Set(startIdx+i, &v)
	}
	return nil
}

// nullable is an interface satisfied by every Arrow array. Used to keep the
// IsNull lookup polymorphic without a per-row type switch.
type nullableArrow interface {
	IsNull(int) bool
	Len() int
}

// writeNumericColumn copies a bulk Arrow numeric slice into the (nullable)
// destination field. When allValid is true the null bitmap is skipped.
// All destination fields are nullable — see createEmptyField comment.
func writeNumericColumn[T any](field *data.Field, arr nullableArrow, values []T, startIdx int, allValid bool) error {
	n := arr.Len()
	if allValid {
		for i := 0; i < n; i++ {
			v := values[i]
			field.Set(startIdx+i, &v)
		}
		return nil
	}
	for i := 0; i < n; i++ {
		if arr.IsNull(i) {
			var v *T
			field.Set(startIdx+i, v)
			continue
		}
		v := values[i]
		field.Set(startIdx+i, &v)
	}
	return nil
}

// writePromotedColumn copies int64/uint64 Arrow values into a float64 field
// (the Grafana-compatibility promotion).
func writePromotedColumn[T int64 | uint64](field *data.Field, arr nullableArrow, values []T, startIdx int, allValid bool) error {
	n := arr.Len()
	if allValid {
		for i := 0; i < n; i++ {
			v := float64(values[i])
			field.Set(startIdx+i, &v)
		}
		return nil
	}
	for i := 0; i < n; i++ {
		if arr.IsNull(i) {
			var v *float64
			field.Set(startIdx+i, v)
			continue
		}
		v := float64(values[i])
		field.Set(startIdx+i, &v)
	}
	return nil
}

// writeTimestampColumn uses Arrow's bulk TimestampValues slice and converts
// to time.Time using the column's declared unit (passed in to avoid an
// unchecked (*arrow.TimestampType) cast inside the hot loop — R2-CR4).
func writeTimestampColumn(field *data.Field, col *array.Timestamp, unit arrow.TimeUnit, startIdx int, allValid bool) error {
	values := col.TimestampValues()
	n := col.Len()
	if allValid {
		for i := 0; i < n; i++ {
			t := values[i].ToTime(unit)
			field.Set(startIdx+i, &t)
		}
		return nil
	}
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			var t *time.Time
			field.Set(startIdx+i, t)
			continue
		}
		t := values[i].ToTime(unit)
		field.Set(startIdx+i, &t)
	}
	return nil
}

// writeStringColumn writes Arrow string column values. Arrow's *array.String
// has no bulk slice accessor (variable-width data), so per-row Value(i) is
// the right shape here.
func writeStringColumn(field *data.Field, col *array.String, startIdx int, allValid bool) error {
	n := col.Len()
	if allValid {
		for i := 0; i < n; i++ {
			s := col.Value(i)
			field.Set(startIdx+i, &s)
		}
		return nil
	}
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			var s *string
			field.Set(startIdx+i, s)
			continue
		}
		s := col.Value(i)
		field.Set(startIdx+i, &s)
	}
	return nil
}

// writeBoolColumn writes Arrow boolean column values. *array.Boolean is
// bitmap-backed; per-row Value(i) is the public accessor.
func writeBoolColumn(field *data.Field, col *array.Boolean, startIdx int, allValid bool) error {
	n := col.Len()
	if allValid {
		for i := 0; i < n; i++ {
			b := col.Value(i)
			field.Set(startIdx+i, &b)
		}
		return nil
	}
	for i := 0; i < n; i++ {
		if col.IsNull(i) {
			var b *bool
			field.Set(startIdx+i, b)
			continue
		}
		b := col.Value(i)
		field.Set(startIdx+i, &b)
	}
	return nil
}
