package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// QueryArrowFlightSQLStyle executes a query using Arc's Arrow endpoint with FlightSQL-style frame building
func QueryArrowFlightSQLStyle(ctx context.Context, settings *ArcInstanceSettings, sql string, timeRange backend.TimeRange) (*data.Frame, error) {
	// Build request
	url := fmt.Sprintf("%s/api/v1/query/arrow", settings.settings.URL)

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
	httpDuration := time.Since(start)
	if err != nil {
		return nil, fmt.Errorf("%s", formatRequestError(err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%s", parseArcError(resp.StatusCode, body))
	}

	// Stream Arrow IPC directly from response body (no intermediate buffer)
	// This eliminates the ReadAll overhead and processes data as it arrives
	parseStart := time.Now()
	reader, err := ipc.NewReader(resp.Body)
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
	log.DefaultLogger.Debug("Arrow query completed (streaming)",
		"total_ms", totalDuration.Milliseconds(),
		"http_ms", httpDuration.Milliseconds(),
		"parse_ms", parseDuration.Milliseconds(),
		"rows", frame.Rows(),
		"fields", len(frame.Fields),
	)

	// Add metadata
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
		if f.Nullable {
			return data.NewField(f.Name, nil, []*int64{})
		}
		return data.NewField(f.Name, nil, []int64{})
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
		if f.Nullable {
			return data.NewField(f.Name, nil, []*uint64{})
		}
		return data.NewField(f.Name, nil, []uint64{})
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

// appendRecordToDataFrame appends an Arrow record to a data.Frame
func appendRecordToDataFrame(frame *data.Frame, record arrow.Record) error {
	for i, col := range record.Columns() {
		field := frame.Fields[i]
		if err := appendArrowColumnToField(field, col); err != nil {
			return fmt.Errorf("failed to append column %s: %w", field.Name, err)
		}
	}
	return nil
}

// appendArrowColumnToField appends an Arrow column to a Grafana field
func appendArrowColumnToField(field *data.Field, col arrow.Array) error {
	switch col.DataType().ID() {
	case arrow.TIMESTAMP:
		return appendTimestampColumn(field, col.(*array.Timestamp))
	case arrow.STRING:
		return appendTypedColumn[string](field, col.(*array.String))
	case arrow.FLOAT32:
		return appendTypedColumn[float32](field, col.(*array.Float32))
	case arrow.FLOAT64:
		return appendTypedColumn[float64](field, col.(*array.Float64))
	case arrow.INT8:
		return appendTypedColumn[int8](field, col.(*array.Int8))
	case arrow.INT16:
		return appendTypedColumn[int16](field, col.(*array.Int16))
	case arrow.INT32:
		return appendTypedColumn[int32](field, col.(*array.Int32))
	case arrow.INT64:
		return appendTypedColumn[int64](field, col.(*array.Int64))
	case arrow.UINT8:
		return appendTypedColumn[uint8](field, col.(*array.Uint8))
	case arrow.UINT16:
		return appendTypedColumn[uint16](field, col.(*array.Uint16))
	case arrow.UINT32:
		return appendTypedColumn[uint32](field, col.(*array.Uint32))
	case arrow.UINT64:
		return appendTypedColumn[uint64](field, col.(*array.Uint64))
	case arrow.BOOL:
		return appendTypedColumn[bool](field, col.(*array.Boolean))
	default:
		return fmt.Errorf("unsupported Arrow type: %s", col.DataType().String())
	}
}

// appendTimestampColumn handles timestamp columns with unit detection
func appendTimestampColumn(field *data.Field, col *array.Timestamp) error {
	timestampType := col.DataType().(*arrow.TimestampType)
	unit := timestampType.Unit

	for i := 0; i < col.Len(); i++ {
		if field.Nullable() {
			if col.IsNull(i) {
				var t *time.Time
				field.Append(t)
				continue
			}
			t := col.Value(i).ToTime(unit)
			field.Append(&t)
		} else {
			field.Append(col.Value(i).ToTime(unit))
		}
	}
	return nil
}

// arrowTypedArray is an interface for typed Arrow arrays
type arrowTypedArray[T any] interface {
	IsNull(int) bool
	Value(int) T
	Len() int
}

// appendTypedColumn appends a typed Arrow column to a field
func appendTypedColumn[T any, Array arrowTypedArray[T]](field *data.Field, arr Array) error {
	for i := 0; i < arr.Len(); i++ {
		if field.Nullable() {
			if arr.IsNull(i) {
				var v *T
				field.Append(v)
				continue
			}
			v := arr.Value(i)
			field.Append(&v)
		} else {
			field.Append(arr.Value(i))
		}
	}
	return nil
}
