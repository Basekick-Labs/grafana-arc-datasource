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
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// QueryArrow executes a query using Arc's Arrow endpoint
func QueryArrow(ctx context.Context, settings *ArcInstanceSettings, sql string, timeRange backend.TimeRange) (*data.Frame, error) {
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
	if err != nil {
		return nil, formatRequestError(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, errors.New(parseArcError(resp.StatusCode, body))
	}

	// Read Arrow IPC stream
	arrowData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	duration := time.Since(start)
	log.DefaultLogger.Debug("Arrow query completed",
		"duration_ms", duration.Milliseconds(),
		"bytes", len(arrowData),
	)

	// Parse Arrow IPC stream
	frame, err := ArrowToDataFrame(arrowData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Arrow data: %w", err)
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

// ArrowToDataFrame converts Arrow IPC bytes to Grafana DataFrame
// Using the pattern from grafana-flightsql-datasource for compatibility
func ArrowToDataFrame(arrowData []byte) (*data.Frame, error) {
	// Create Arrow IPC reader
	reader, err := ipc.NewReader(bytes.NewReader(arrowData))
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow reader: %w", err)
	}
	defer reader.Release()

	// Read first record to get schema
	if !reader.Next() {
		if reader.Err() != nil {
			return nil, fmt.Errorf("error reading Arrow stream: %w", reader.Err())
		}
		// No data
		return data.NewFrame(""), nil
	}

	// Get schema and create frame with empty fields
	schema := reader.Record().Schema()
	frame := newFrameFromSchema(schema)

	// Process first record
	record := reader.Record()
	if err := appendRecordToFrame(frame, record); err != nil {
		record.Release()
		return nil, err
	}
	record.Release()

	// Process remaining records
	for reader.Next() {
		record := reader.Record()
		if err := appendRecordToFrame(frame, record); err != nil {
			record.Release()
			return nil, err
		}
		record.Release()
	}

	if reader.Err() != nil {
		return nil, fmt.Errorf("error reading Arrow stream: %w", reader.Err())
	}

	log.DefaultLogger.Debug("Created DataFrame",
		"numFields", len(frame.Fields),
		"rows", frame.Rows(),
	)

	return frame, nil
}

// newFrameFromSchema creates an empty data frame with fields based on Arrow schema
func newFrameFromSchema(schema *arrow.Schema) *data.Frame {
	fields := make([]*data.Field, schema.NumFields())
	for i, arrowField := range schema.Fields() {
		fields[i] = newFieldFromArrowField(arrowField)
	}
	return data.NewFrame("", fields...)
}

// newFieldFromArrowField creates a Grafana field from an Arrow field
func newFieldFromArrowField(f arrow.Field) *data.Field {
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
	case arrow.DURATION:
		if f.Nullable {
			return data.NewField(f.Name, nil, []*int64{})
		}
		return data.NewField(f.Name, nil, []int64{})
	default:
		// For unsupported types, use nullable string
		log.DefaultLogger.Warn("Unsupported Arrow type, using string",
			"field", f.Name,
			"type", f.Type.String(),
		)
		return data.NewField(f.Name, nil, []*string{})
	}
}

// appendRecordToFrame appends data from an Arrow record to a data frame
func appendRecordToFrame(frame *data.Frame, record arrow.Record) error {
	for i, col := range record.Columns() {
		field := frame.Fields[i]
		if err := appendColumnToField(field, col); err != nil {
			return fmt.Errorf("failed to append column %s: %w", field.Name, err)
		}
	}
	return nil
}

// appendColumnToField appends data from an Arrow column to a Grafana field
func appendColumnToField(field *data.Field, col arrow.Array) error {
	switch col.DataType().ID() {
	case arrow.TIMESTAMP:
		return appendTimestamp(field, col.(*array.Timestamp))
	case arrow.STRING:
		return appendBasic[string](field, col.(*array.String))
	case arrow.UINT8:
		return appendBasic[uint8](field, col.(*array.Uint8))
	case arrow.UINT16:
		return appendBasic[uint16](field, col.(*array.Uint16))
	case arrow.UINT32:
		return appendBasic[uint32](field, col.(*array.Uint32))
	case arrow.UINT64:
		return appendBasic[uint64](field, col.(*array.Uint64))
	case arrow.INT8:
		return appendBasic[int8](field, col.(*array.Int8))
	case arrow.INT16:
		return appendBasic[int16](field, col.(*array.Int16))
	case arrow.INT32:
		return appendBasic[int32](field, col.(*array.Int32))
	case arrow.INT64:
		return appendBasic[int64](field, col.(*array.Int64))
	case arrow.FLOAT32:
		return appendBasic[float32](field, col.(*array.Float32))
	case arrow.FLOAT64:
		return appendBasic[float64](field, col.(*array.Float64))
	case arrow.BOOL:
		return appendBasic[bool](field, col.(*array.Boolean))
	case arrow.DURATION:
		// Duration needs special handling
		return appendDuration(field, col.(*array.Duration))
	default:
		// For unsupported types, convert to string
		return appendGenericAsString(field, col)
	}
}

// appendTimestamp appends timestamp data to a field
func appendTimestamp(field *data.Field, col *array.Timestamp) error {
	timestampType := col.DataType().(*arrow.TimestampType)

	log.DefaultLogger.Debug("Processing timestamp field",
		"field", field.Name,
		"unit", timestampType.Unit.String(),
		"rows", col.Len(),
	)

	// WORKAROUND: Arc appears to send timestamps as seconds but marks them as microseconds
	// Detect this by checking if the first value is too small to be microseconds
	// Microseconds since epoch for year 2020+ should be > 1.5e15
	// If value is < 1e12, it's likely seconds, not microseconds
	var actualUnit arrow.TimeUnit = timestampType.Unit
	if col.Len() > 0 && !col.IsNull(0) {
		firstValue := int64(col.Value(0))
		if timestampType.Unit == arrow.Microsecond && firstValue < 1e12 {
			log.DefaultLogger.Warn("Timestamp appears to be in seconds but marked as microseconds, converting",
				"field", field.Name,
				"firstValue", firstValue,
			)
			actualUnit = arrow.Second
		}
	}

	for i := 0; i < col.Len(); i++ {
		if field.Nullable() {
			if col.IsNull(i) {
				var t *time.Time
				field.Append(t)
				continue
			}
			t := col.Value(i).ToTime(actualUnit)
			field.Append(&t)
		} else {
			field.Append(col.Value(i).ToTime(actualUnit))
		}
	}
	return nil
}

// appendDuration appends duration data to a field (stored as int64)
func appendDuration(field *data.Field, col *array.Duration) error {
	for i := 0; i < col.Len(); i++ {
		if field.Nullable() {
			if col.IsNull(i) {
				var v *int64
				field.Append(v)
				continue
			}
			v := int64(col.Value(i))
			field.Append(&v)
		} else {
			field.Append(int64(col.Value(i)))
		}
	}
	return nil
}

// arrowArray is an interface for Arrow arrays that support basic operations
type arrowArray[T any] interface {
	IsNull(int) bool
	Value(int) T
	Len() int
}

// appendBasic appends basic type data to a field
func appendBasic[T any, Array arrowArray[T]](field *data.Field, arr Array) error {
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

// appendGenericAsString appends unsupported types as strings
func appendGenericAsString(field *data.Field, col arrow.Array) error {
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			var s *string
			field.Append(s)
			continue
		}
		str := fmt.Sprintf("%v", col.GetOneForMarshal(i))
		field.Append(&str)
	}
	return nil
}
