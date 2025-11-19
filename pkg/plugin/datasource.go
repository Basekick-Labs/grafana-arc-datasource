package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// ArcDataSourceSettings contains Arc connection settings
type ArcDataSourceSettings struct {
	URL      string `json:"url"`
	Database string `json:"database"`
	Timeout  int    `json:"timeout"` // seconds
	UseArrow bool   `json:"useArrow"`
}

// ArcQuery represents a query to Arc
type ArcQuery struct {
	RefID         string `json:"refId"`
	SQL           string `json:"sql"`
	Format        string `json:"format"` // "time_series" or "table"
	MaxDataPoints int64  `json:"maxDataPoints"`
}

// ArcInstanceSettings holds per-instance settings
type ArcInstanceSettings struct {
	settings ArcDataSourceSettings
	apiKey   string
}

// ArcDatasource implements the Grafana datasource interface
type ArcDatasource struct{}

// NewArcDatasource creates a new datasource
func NewArcDatasource() *ArcDatasource {
	return &ArcDatasource{}
}

// getSettings extracts settings from plugin context
func getSettings(ctx context.Context, pluginCtx backend.PluginContext) (*ArcInstanceSettings, error) {
	var dsSettings ArcDataSourceSettings

	// Parse settings
	if err := json.Unmarshal(pluginCtx.DataSourceInstanceSettings.JSONData, &dsSettings); err != nil {
		return nil, fmt.Errorf("failed to unmarshal settings: %w", err)
	}

	// Get API key from secure settings
	apiKey := pluginCtx.DataSourceInstanceSettings.DecryptedSecureJSONData["apiKey"]
	if apiKey == "" {
		return nil, fmt.Errorf("API key is required")
	}

	// Default values
	if dsSettings.Timeout == 0 {
		dsSettings.Timeout = 30
	}
	if dsSettings.Database == "" {
		dsSettings.Database = "default"
	}
	// Note: UseArrow defaults to false in Go struct initialization
	// The frontend defaults to true in the UI (ConfigEditor.tsx line 145)
	// This ensures the toggle actually works - if explicitly set to false, respect that choice

	return &ArcInstanceSettings{
		settings: dsSettings,
		apiKey:   apiKey,
	}, nil
}

// QueryData handles query requests
func (d *ArcDatasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	response := backend.NewQueryDataResponse()

	// Get settings
	settings, err := getSettings(ctx, req.PluginContext)
	if err != nil {
		return nil, err
	}

	// Process each query
	for _, q := range req.Queries {
		res := d.query(ctx, settings, q)
		response.Responses[q.RefID] = res
	}

	return response, nil
}

// query executes a single query
func (d *ArcDatasource) query(ctx context.Context, settings *ArcInstanceSettings, query backend.DataQuery) backend.DataResponse {
	var response backend.DataResponse

	// Parse query model
	var qm ArcQuery
	if err := json.Unmarshal(query.JSON, &qm); err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("failed to unmarshal query: %v", err))
	}

	qm.RefID = query.RefID

	// Apply time range macros
	sql := ApplyMacros(qm.SQL, query.TimeRange)

	// Note: Users should add "ORDER BY time ASC" to their queries for best performance
	// This prevents expensive in-memory sorting during long-to-wide conversion

	log.DefaultLogger.Debug("Executing Arc query",
		"refId", qm.RefID,
		"sql", sql,
		"format", qm.Format,
		"useArrow", settings.settings.UseArrow,
	)

	// Execute query based on protocol
	var frame *data.Frame
	var err error

	if settings.settings.UseArrow {
		// Use FlightSQL-style Arrow handling (proven to work)
		frame, err = QueryArrowFlightSQLStyle(ctx, settings, sql, query.TimeRange)
	} else {
		frame, err = QueryJSON(ctx, settings, sql, query.TimeRange)
	}

	if err != nil {
		return backend.ErrDataResponse(backend.StatusInternal, fmt.Sprintf("query failed: %v", err))
	}

	// Time the frame preparation (conversion)
	prepareStart := time.Now()
	processedFrames := prepareFrames(frame, qm)
	prepareDuration := time.Since(prepareStart)

	if len(processedFrames) == 0 {
		log.DefaultLogger.Warn("No frames returned from query", "refId", qm.RefID)
		return response
	}

	response.Frames = append(response.Frames, processedFrames...)

	log.DefaultLogger.Debug("Returning query response",
		"refId", qm.RefID,
		"frames", len(processedFrames),
		"rows", processedFrames[0].Rows(),
		"fields", len(processedFrames[0].Fields),
		"prepareDuration_ms", prepareDuration.Milliseconds(),
	)

	return response
}

// CheckHealth validates the datasource connection
func (d *ArcDatasource) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	var status = backend.HealthStatusOk
	var message = "Arc datasource is working"

	// Get settings
	settings, err := getSettings(ctx, req.PluginContext)
	if err != nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: fmt.Sprintf("failed to get settings: %v", err),
		}, nil
	}

	// Test connection with simple query
	testSQL := "SHOW DATABASES"
	_, err = QueryArrow(ctx, settings, testSQL, backend.TimeRange{
		From: time.Now().Add(-1 * time.Hour),
		To:   time.Now(),
	})

	if err != nil {
		status = backend.HealthStatusError
		message = fmt.Sprintf("Failed to connect to Arc: %v", err)
		log.DefaultLogger.Error("Health check failed", "error", err)
	} else {
		log.DefaultLogger.Info("Health check passed",
			"url", settings.settings.URL,
			"database", settings.settings.Database,
		)
	}

	return &backend.CheckHealthResult{
		Status:  status,
		Message: message,
	}, nil
}

func prepareFrames(frame *data.Frame, qm ArcQuery) data.Frames {
	if frame == nil {
		return nil
	}

	frame.Name = qm.RefID
	frame.RefID = qm.RefID

	if frame.Meta == nil {
		frame.Meta = &data.FrameMeta{}
	}

	switch qm.Format {
	case "table":
		frame.Meta.PreferredVisualization = data.VisTypeTable
		frame.Meta.Type = data.FrameTypeTable
		return data.Frames{frame}
	default:
		// Default to time series visualization
		frame.Meta.PreferredVisualization = data.VisTypeGraph
	}

	schema := frame.TimeSeriesSchema()

	// Handle wide format time series (already optimized, no conversion needed)
	if schema.Type == data.TimeSeriesTypeWide {
		frame.Meta.Type = data.FrameTypeTimeSeriesWide
		frame.Meta.PreferredVisualization = data.VisTypeGraph
		log.DefaultLogger.Debug("Detected wide format time series (no conversion needed)",
			"rows", frame.Rows(),
			"fields", len(frame.Fields),
		)
		return data.Frames{frame}
	}

	// Handle long format time series (needs conversion)
	if schema.Type == data.TimeSeriesTypeLong {
		frame.Meta.Type = data.FrameTypeTimeSeriesLong

		log.DefaultLogger.Debug("Detected long format time series",
			"timeIndex", schema.TimeIndex,
			"rows", frame.Rows(),
			"fields", len(frame.Fields),
			"fieldNames", func() []string {
				names := make([]string, len(frame.Fields))
				for i, f := range frame.Fields {
					names[i] = f.Name
				}
				return names
			}(),
		)

		longFrame := ensureAscendingTimes(frame, schema.TimeIndex)

		// Configure fill missing policy for long-to-wide conversion
		fillMissing := &data.FillMissing{
			Mode: data.FillModeNull, // Use null for missing values
		}

		wideFrame, err := data.LongToWide(longFrame, fillMissing)
		if err != nil {
			log.DefaultLogger.Warn("Failed to convert long series to wide format",
				"error", err,
				"schema", schema,
			)
			// Return the long frame as-is, let Grafana handle it
			longFrame.Meta.Type = data.FrameTypeTimeSeriesLong
			longFrame.Meta.PreferredVisualization = data.VisTypeGraph
			longFrame.RefID = qm.RefID
			return data.Frames{longFrame}
		}

		log.DefaultLogger.Debug("Converted to wide format",
			"wideRows", wideFrame.Rows(),
			"wideFields", len(wideFrame.Fields),
			"wideFieldNames", func() []string {
				names := make([]string, len(wideFrame.Fields))
				for i, f := range wideFrame.Fields {
					names[i] = f.Name
				}
				return names
			}(),
		)

		if wideFrame.Meta == nil {
			wideFrame.Meta = &data.FrameMeta{}
		}
		wideFrame.Meta.PreferredVisualization = data.VisTypeGraph
		wideFrame.Meta.Type = data.FrameTypeTimeSeriesWide
		wideFrame.RefID = qm.RefID
		return data.Frames{wideFrame}
	}

	// Unknown format - return as-is
	frame.Meta.Type = data.FrameTypeUnknown

	return data.Frames{frame}
}

// ensureAscendingTimes sorts frame rows by time if needed.
// Performance: O(n) check + O(n log n) sort if unsorted (vs previous O(n²) bubble sort)
func ensureAscendingTimes(frame *data.Frame, timeIdx int) *data.Frame {
	rowLen, err := frame.RowLen()
	if err != nil || rowLen < 2 {
		return frame
	}

	// Check if data is sorted - O(n) early exit for already sorted data
	needsSorting := false
	var prevTime time.Time

	for i := 0; i < rowLen; i++ {
		currTime, ok := toTime(frame.CopyAt(timeIdx, i))
		if !ok {
			// Can't sort if we have invalid times
			return frame
		}

		if i > 0 && currTime.Before(prevTime) {
			needsSorting = true
			break
		}
		prevTime = currTime
	}

	if !needsSorting {
		return frame
	}

	log.DefaultLogger.Debug("Sorting frame by time", "rows", rowLen)

	// Create sorted frame by collecting all rows with their timestamps
	type rowWithTime struct {
		time time.Time
		data []interface{}
	}

	rows := make([]rowWithTime, rowLen)
	for i := 0; i < rowLen; i++ {
		t, _ := toTime(frame.CopyAt(timeIdx, i))
		rows[i] = rowWithTime{
			time: t,
			data: frame.RowCopy(i),
		}
	}

	// Sort by time ascending using efficient O(n log n) algorithm
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].time.Before(rows[j].time)
	})

	// Build sorted frame
	sorted := frame.EmptyCopy()
	sorted.Meta = frame.Meta
	sorted.Name = frame.Name
	sorted.RefID = frame.RefID

	for _, row := range rows {
		sorted.AppendRow(row.data...)
	}

	return sorted
}

func toTime(val interface{}) (time.Time, bool) {
	switch v := val.(type) {
	case time.Time:
		return v, true
	case *time.Time:
		if v == nil {
			return time.Time{}, false
		}
		return *v, true
	default:
		return time.Time{}, false
	}
}
