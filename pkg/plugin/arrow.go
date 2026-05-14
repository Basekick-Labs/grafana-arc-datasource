package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// queryArrow executes a query against Arc's /api/v1/query/arrow endpoint and
// returns the decoded Grafana DataFrame. Streams the Arrow IPC response
// (decoded record-by-record) and decodes columns via bulk slice accessors
// where the Arrow library supports them.
func queryArrow(ctx context.Context, settings *ArcInstanceSettings, sql string) (*data.Frame, error) {
	url := fmt.Sprintf("%s/api/v1/query/arrow", settings.settings.URL)

	reqBody := map[string]interface{}{"sql": sql}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", settings.apiKey))
	if settings.settings.Database != "" {
		req.Header.Set("X-Arc-Database", settings.settings.Database)
	}

	client := newHTTPClient(
		time.Duration(settings.settings.Timeout)*time.Second,
		allowPrivateForSettings(settings),
	)

	start := time.Now()
	resp, err := client.Do(req)
	httpDuration := time.Since(start)
	if err != nil {
		return nil, formatRequestError(err)
	}
	defer resp.Body.Close()

	body := http.MaxBytesReader(nil, resp.Body, MaxResponseBytes)

	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(body)
		return nil, errors.New(parseArcError(resp.StatusCode, raw))
	}

	parseStart := time.Now()
	reader, err := ipc.NewReader(body)
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow reader: %w", err)
	}
	defer reader.Release()

	frame, err := frameForRecords(reader)
	parseDuration := time.Since(parseStart)
	if err != nil {
		return nil, err
	}

	totalDuration := time.Since(start)
	log.DefaultLogger.Debug("Arrow query completed",
		"total_ms", totalDuration.Milliseconds(),
		"http_ms", httpDuration.Milliseconds(),
		"parse_ms", parseDuration.Milliseconds(),
		"rows", frame.Rows(),
		"fields", len(frame.Fields),
	)

	frame.Meta = &data.FrameMeta{
		ExecutedQueryString: sql,
		Custom: map[string]interface{}{
			"executionTime": totalDuration.Milliseconds(),
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

// createEmptyField creates an empty data.Field from an Arrow field
func createEmptyField(f arrow.Field) *data.Field {
	switch f.Type.ID() {
	case arrow.STRING:
		if f.Nullable {
			return data.NewField(f.Name, nil, []*string{})
		}
		return data.NewField(f.Name, nil, []string{})
	case arrow.FLOAT32:
		if f.Nullable {
			return data.NewField(f.Name, nil, []*float32{})
		}
		return data.NewField(f.Name, nil, []float32{})
	case arrow.FLOAT64:
		if f.Nullable {
			return data.NewField(f.Name, nil, []*float64{})
		}
		return data.NewField(f.Name, nil, []float64{})
	case arrow.INT8:
		if f.Nullable {
			return data.NewField(f.Name, nil, []*int8{})
		}
		return data.NewField(f.Name, nil, []int8{})
	case arrow.INT16:
		if f.Nullable {
			return data.NewField(f.Name, nil, []*int16{})
		}
		return data.NewField(f.Name, nil, []int16{})
	case arrow.INT32:
		if f.Nullable {
			return data.NewField(f.Name, nil, []*int32{})
		}
		return data.NewField(f.Name, nil, []int32{})
	case arrow.INT64:
		// Promote to float64 so Grafana Stat/Time series panels treat it as a
		// numeric value field. DuckDB aggregates (SUM, COUNT) return int64 after
		// Arc's decimal normalization — Grafana auto-detection requires float64.
		if f.Nullable {
			return data.NewField(f.Name, nil, []*float64{})
		}
		return data.NewField(f.Name, nil, []float64{})
	case arrow.UINT8:
		if f.Nullable {
			return data.NewField(f.Name, nil, []*uint8{})
		}
		return data.NewField(f.Name, nil, []uint8{})
	case arrow.UINT16:
		if f.Nullable {
			return data.NewField(f.Name, nil, []*uint16{})
		}
		return data.NewField(f.Name, nil, []uint16{})
	case arrow.UINT32:
		if f.Nullable {
			return data.NewField(f.Name, nil, []*uint32{})
		}
		return data.NewField(f.Name, nil, []uint32{})
	case arrow.UINT64:
		// Promote to float64 for same reason as INT64.
		if f.Nullable {
			return data.NewField(f.Name, nil, []*float64{})
		}
		return data.NewField(f.Name, nil, []float64{})
	case arrow.BOOL:
		if f.Nullable {
			return data.NewField(f.Name, nil, []*bool{})
		}
		return data.NewField(f.Name, nil, []bool{})
	case arrow.TIMESTAMP:
		if f.Nullable {
			return data.NewField(f.Name, nil, []*time.Time{})
		}
		return data.NewField(f.Name, nil, []time.Time{})
	default:
		// Fallback to nullable string for unsupported types
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
// types, dictionary-encoded strings, lists) returns a clean error instead of
// panicking the goroutine.
//
// Numeric and timestamp columns use Arrow's bulk slice accessors
// (Int64Values/Float64Values/TimestampValues) and short-circuit the null check
// when col.NullN() == 0 — significantly faster than per-row Value(i) +
// IsNull(i) on large batches.
func writeArrowColumnIntoField(field *data.Field, col arrow.Array, startIdx int) error {
	mismatch := func() error {
		return fmt.Errorf("arrow column type mismatch: id=%s concrete=%T", col.DataType().String(), col)
	}
	nullable := field.Nullable()
	allValid := col.NullN() == 0
	switch col.DataType().ID() {
	case arrow.TIMESTAMP:
		arr, ok := col.(*array.Timestamp)
		if !ok {
			return mismatch()
		}
		return writeTimestampColumn(field, arr, startIdx, nullable, allValid)
	case arrow.STRING:
		arr, ok := col.(*array.String)
		if !ok {
			return mismatch()
		}
		return writeStringColumn(field, arr, startIdx, nullable, allValid)
	case arrow.BOOL:
		arr, ok := col.(*array.Boolean)
		if !ok {
			return mismatch()
		}
		return writeBoolColumn(field, arr, startIdx, nullable, allValid)
	case arrow.FLOAT32:
		arr, ok := col.(*array.Float32)
		if !ok {
			return mismatch()
		}
		return writeNumericColumn[float32](field, arr, arr.Float32Values(), startIdx, nullable, allValid)
	case arrow.FLOAT64:
		arr, ok := col.(*array.Float64)
		if !ok {
			return mismatch()
		}
		return writeNumericColumn[float64](field, arr, arr.Float64Values(), startIdx, nullable, allValid)
	case arrow.INT8:
		arr, ok := col.(*array.Int8)
		if !ok {
			return mismatch()
		}
		return writeNumericColumn[int8](field, arr, arr.Int8Values(), startIdx, nullable, allValid)
	case arrow.INT16:
		arr, ok := col.(*array.Int16)
		if !ok {
			return mismatch()
		}
		return writeNumericColumn[int16](field, arr, arr.Int16Values(), startIdx, nullable, allValid)
	case arrow.INT32:
		arr, ok := col.(*array.Int32)
		if !ok {
			return mismatch()
		}
		return writeNumericColumn[int32](field, arr, arr.Int32Values(), startIdx, nullable, allValid)
	case arrow.INT64:
		arr, ok := col.(*array.Int64)
		if !ok {
			return mismatch()
		}
		return writePromotedColumn[int64](field, arr, arr.Int64Values(), startIdx, nullable, allValid)
	case arrow.UINT8:
		arr, ok := col.(*array.Uint8)
		if !ok {
			return mismatch()
		}
		return writeNumericColumn[uint8](field, arr, arr.Uint8Values(), startIdx, nullable, allValid)
	case arrow.UINT16:
		arr, ok := col.(*array.Uint16)
		if !ok {
			return mismatch()
		}
		return writeNumericColumn[uint16](field, arr, arr.Uint16Values(), startIdx, nullable, allValid)
	case arrow.UINT32:
		arr, ok := col.(*array.Uint32)
		if !ok {
			return mismatch()
		}
		return writeNumericColumn[uint32](field, arr, arr.Uint32Values(), startIdx, nullable, allValid)
	case arrow.UINT64:
		arr, ok := col.(*array.Uint64)
		if !ok {
			return mismatch()
		}
		return writePromotedColumn[uint64](field, arr, arr.Uint64Values(), startIdx, nullable, allValid)
	default:
		return fmt.Errorf("unsupported Arrow type: %s", col.DataType().String())
	}
}

// nullable is an interface satisfied by every Arrow array. Used to keep the
// IsNull lookup polymorphic without a per-row type switch.
type nullableArrow interface {
	IsNull(int) bool
	Len() int
}

// writeNumericColumn copies a bulk Arrow numeric slice into the destination
// field. When allValid is true the null bitmap is skipped entirely.
func writeNumericColumn[T any](field *data.Field, arr nullableArrow, values []T, startIdx int, nullable, allValid bool) error {
	n := arr.Len()
	if nullable {
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
	for i := 0; i < n; i++ {
		field.Set(startIdx+i, values[i])
	}
	return nil
}

// writePromotedColumn copies int64/uint64 Arrow values into a float64 field
// (the Grafana-compatibility promotion).
func writePromotedColumn[T int64 | uint64](field *data.Field, arr nullableArrow, values []T, startIdx int, nullable, allValid bool) error {
	n := arr.Len()
	if nullable {
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
	for i := 0; i < n; i++ {
		field.Set(startIdx+i, float64(values[i]))
	}
	return nil
}

// writeTimestampColumn uses Arrow's bulk TimestampValues slice and converts
// to time.Time using the column's declared unit.
func writeTimestampColumn(field *data.Field, col *array.Timestamp, startIdx int, nullable, allValid bool) error {
	unit := col.DataType().(*arrow.TimestampType).Unit
	values := col.TimestampValues()
	n := col.Len()
	if nullable {
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
	for i := 0; i < n; i++ {
		field.Set(startIdx+i, values[i].ToTime(unit))
	}
	return nil
}

// writeStringColumn writes Arrow string column values. Arrow's *array.String
// has no bulk slice accessor (variable-width data), so per-row Value(i) is
// the right shape here.
func writeStringColumn(field *data.Field, col *array.String, startIdx int, nullable, allValid bool) error {
	n := col.Len()
	if nullable {
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
	for i := 0; i < n; i++ {
		field.Set(startIdx+i, col.Value(i))
	}
	return nil
}

// writeBoolColumn writes Arrow boolean column values. *array.Boolean is
// bitmap-backed; per-row Value(i) is the public accessor.
func writeBoolColumn(field *data.Field, col *array.Boolean, startIdx int, nullable, allValid bool) error {
	n := col.Len()
	if nullable {
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
	for i := 0; i < n; i++ {
		field.Set(startIdx+i, col.Value(i))
	}
	return nil
}
