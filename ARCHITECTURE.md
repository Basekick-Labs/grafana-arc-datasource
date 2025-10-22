# Arc Grafana Datasource - Architecture

## Overview

The Arc Grafana datasource plugin provides high-performance data visualization for Arc time-series database using Apache Arrow protocol for efficient data transfer.

## Architecture Components

```
┌─────────────────────────────────────────────────────────────┐
│                     Grafana Dashboard                       │
│  (User queries via SQL editor, visualizations render data)  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                   Frontend (TypeScript/React)                │
│  • QueryEditor.tsx - SQL editor UI                          │
│  • ConfigEditor.tsx - Datasource settings                   │
│  • datasource.ts - Query dispatcher                         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ↓ (GraphQL/HTTP)
┌─────────────────────────────────────────────────────────────┐
│                   Backend Plugin (Go)                        │
│  • datasource.go - Main datasource logic                    │
│  • arrow.go - Arrow protocol handler                        │
│  • query.go - Query execution & macros                      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ↓ (HTTP POST /api/v1/query/arrow)
┌─────────────────────────────────────────────────────────────┐
│                        Arc Database                          │
│  • DuckDB query engine                                      │
│  • Parquet storage (MinIO/S3)                               │
│  • Arrow IPC response                                       │
└─────────────────────────────────────────────────────────────┘
```

## Data Flow

### Query Execution Flow

1. **User Input**
   - User writes SQL in QueryEditor
   - Grafana macros (`$__timeFilter`, `$__interval`) are included
   - Query submitted via "Run Query" button

2. **Frontend Processing**
   - `QueryEditor.tsx` captures SQL and format settings
   - `datasource.ts` sends query to Grafana backend API
   - Time range from dashboard passed along

3. **Backend Plugin Processing**
   - `datasource.go` receives query request
   - `query.go` applies macro replacements:
     ```go
     $__timeFilter(time) → time >= '2025-10-22T00:00:00Z' AND time < '2025-10-22T23:59:59Z'
     $__interval → '1 minute' (calculated based on time range)
     ```
   - Routes to Arrow or JSON handler based on settings

4. **Arrow Protocol Handler** (`arrow.go`)
   ```go
   // Send HTTP request to Arc
   POST /api/v1/query/arrow
   Headers: x-api-key: <token>
   Body: {"sql": "SELECT ..."}

   // Receive Arrow IPC stream
   Response: <Arrow IPC bytes>

   // Parse with Apache Arrow Go library
   reader := ipc.NewReader(response.Body)
   records := reader.Record()

   // Convert to Grafana DataFrame
   frame := ArrowToDataFrame(records)
   ```

5. **Arc Processing**
   - DuckDB executes SQL against Parquet files
   - Results serialized to Arrow IPC format
   - Compressed columnar binary stream returned

6. **Response Conversion**
   - Arrow RecordBatch → Grafana DataFrame
   - Type mapping:
     - `arrow.INT64` → `*int64`
     - `arrow.FLOAT64` → `*float64`
     - `arrow.TIMESTAMP` → `*time.Time`
     - `arrow.STRING` → `*string`
     - `arrow.BOOL` → `*bool`

7. **Visualization**
   - Grafana renders DataFrame in panel
   - Time-series graph, table, or other visualization

## Performance Optimizations

### 1. Apache Arrow Protocol

**Why Arrow?**
- Columnar binary format (vs row-based JSON)
- Zero-copy deserialization
- Native compression
- Type-safe data transfer

**Benchmark** (from Arc docs):
- 7.36x faster for 100K+ rows
- 43% smaller payloads
- Millisecond query latency

**Implementation:**
```go
// Go backend - zero-copy Arrow parsing
reader := ipc.NewReader(arrowBytes)
record := reader.Record()  // No allocations!

// Direct field access
col := record.Column(0).(*array.Float64)
value := col.Value(i)  // Pointer to data, not copy
```

### 2. Backend Plugin Architecture

**Why Go backend?**
- Grafana plugins can be frontend-only or backend+frontend
- Backend plugin advantages:
  - Secure credential storage (API key never exposed to browser)
  - Connection pooling and caching
  - Binary protocol handling (Arrow)
  - Better error handling

**Connection Management:**
```go
type ArcInstanceSettings struct {
    settings ArcDataSourceSettings
    apiKey   string  // Encrypted at rest
}

// Reused across queries
client := &http.Client{
    Timeout: time.Duration(settings.Timeout) * time.Second,
}
```

### 3. Query Macros

**Purpose:** Dynamic SQL generation based on dashboard state

**Examples:**

```sql
-- User writes:
SELECT * FROM cpu WHERE $__timeFilter(time)

-- Backend expands to:
SELECT * FROM cpu WHERE time >= '2025-10-22T00:00:00Z' AND time < '2025-10-22T23:59:59Z'
```

```sql
-- User writes:
SELECT time_bucket(INTERVAL '$__interval', time) as time, AVG(usage)

-- Backend calculates interval based on range and expands:
SELECT time_bucket(INTERVAL '1 minute', time) as time, AVG(usage)
```

**Benefits:**
- Automatic time range filtering
- Dynamic aggregation granularity
- Reusable queries across different time ranges

## Configuration

### Datasource Settings

**Stored in Grafana database:**

```json
{
  "jsonData": {
    "url": "http://localhost:8000",
    "database": "production",
    "timeout": 30,
    "useArrow": true
  },
  "secureJsonData": {
    "apiKey": "<encrypted>"
  }
}
```

**Security:**
- API key encrypted using Grafana's secret management
- Never sent to browser
- Only accessible to backend plugin

### Query Model

**Stored in dashboard JSON:**

```json
{
  "refId": "A",
  "sql": "SELECT * FROM cpu WHERE $__timeFilter(time)",
  "format": "time_series"
}
```

## Frontend Components

### 1. ConfigEditor.tsx

**Purpose:** Datasource configuration UI

**Features:**
- URL input with validation
- Secure API key input (masked)
- Database name (optional)
- Timeout setting
- Arrow protocol toggle

**User Experience:**
```
┌─────────────────────────────────────────┐
│ Arc Connection                          │
├─────────────────────────────────────────┤
│ URL: [http://localhost:8000___________] │
│ API Key: [********************] [Reset] │
│ Database: [default___________________] │
│                                         │
│ Advanced Settings                       │
│ Timeout: [30] seconds                   │
│ Use Arrow: [✓] (recommended)           │
└─────────────────────────────────────────┘
```

### 2. QueryEditor.tsx

**Purpose:** SQL query builder

**Features:**
- Multi-line SQL text area
- Syntax hints for macros
- Format selector (time series vs table)
- Run on blur or explicit run

**User Experience:**
```
┌─────────────────────────────────────────┐
│ Format: ◉ Time series  ◯ Table         │
├─────────────────────────────────────────┤
│ SQL Query:                              │
│ ┌─────────────────────────────────────┐ │
│ │ SELECT                              │ │
│ │   time,                             │ │
│ │   host,                             │ │
│ │   AVG(usage_idle) as avg_cpu        │ │
│ │ FROM cpu                            │ │
│ │ WHERE $__timeFilter(time)           │ │
│ │ GROUP BY time, host                 │ │
│ │ ORDER BY time                       │ │
│ └─────────────────────────────────────┘ │
│                                         │
│ Macros: $__timeFilter(column),         │
│         $__timeFrom(), $__timeTo()     │
└─────────────────────────────────────────┘
```

### 3. VariableQueryEditor.tsx

**Purpose:** Template variable configuration

**Features:**
- SQL query for variable values
- First column used for variable options
- Supports SHOW commands

**Example Queries:**
```sql
-- Get list of hosts
SELECT DISTINCT host FROM cpu ORDER BY host

-- Get databases
SHOW DATABASES

-- Get tables
SHOW TABLES FROM production
```

**Result:**
- Creates dropdown variable in dashboard
- Users can select values dynamically
- Variables usable in panel queries: `WHERE host = '$host'`

## Backend Implementation Details

### Arrow Type Conversion

```go
func createFieldFromArrowColumn(name string, arrowType arrow.DataType, records []arrow.Record, colIdx int, totalRows int) (*data.Field, error) {
    switch arrowType.ID() {
    case arrow.INT64:
        return createInt64Field(name, records, colIdx, totalRows), nil

    case arrow.FLOAT64:
        return createFloat64Field(name, records, colIdx, totalRows), nil

    case arrow.STRING:
        return createStringField(name, records, colIdx, totalRows), nil

    case arrow.TIMESTAMP:
        // Special handling for timestamps
        return createTimestampField(name, records, colIdx, totalRows), nil

    case arrow.BOOL:
        return createBoolField(name, records, colIdx, totalRows), nil

    default:
        // Fallback to string
        return createStringField(name, records, colIdx, totalRows), nil
    }
}
```

### Timestamp Handling

Arc stores timestamps as `TIMESTAMP[ms]` (millisecond precision):

```go
func createTimestampField(name string, records []arrow.Record, colIdx int, totalRows int) *data.Field {
    values := make([]*time.Time, 0, totalRows)

    for _, record := range records {
        col := record.Column(colIdx).(*array.Timestamp)
        timestampType := col.DataType().(*arrow.TimestampType)

        for i := 0; i < col.Len(); i++ {
            val := col.Value(i)
            // Convert Arrow timestamp to Go time.Time
            t := timestampType.GetToTimeFunc()(val)
            values = append(values, &t)
        }
    }

    return data.NewField(name, nil, values)
}
```

### Health Check

```go
func (d *ArcDatasource) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
    // Test with simple query
    testSQL := "SHOW DATABASES"
    _, err := QueryArrow(ctx, settings, testSQL, timeRange)

    if err != nil {
        return &backend.CheckHealthResult{
            Status:  backend.HealthStatusError,
            Message: fmt.Sprintf("Failed to connect: %v", err),
        }, nil
    }

    return &backend.CheckHealthResult{
        Status:  backend.HealthStatusOk,
        Message: "Arc datasource is working",
    }, nil
}
```

## Error Handling

### Frontend Errors

- Connection errors: Show in datasource config
- Query errors: Display in panel with error message
- Timeout: Configurable, returns timeout error

### Backend Errors

```go
// HTTP errors from Arc
if resp.StatusCode != http.StatusOK {
    body, _ := io.ReadAll(resp.Body)
    return fmt.Errorf("Arc returned status %d: %s", resp.StatusCode, string(body))
}

// Arrow parsing errors
frame, err := ArrowToDataFrame(arrowData)
if err != nil {
    return backend.ErrDataResponse(
        backend.StatusInternal,
        fmt.Sprintf("failed to parse Arrow data: %v", err),
    )
}
```

## Testing Strategy

### Backend Tests (Go)

```bash
go test ./pkg/plugin/...
```

**Test Coverage:**
- Arrow parsing with various data types
- Macro expansion
- Health check
- Error handling
- Type conversion

### Frontend Tests (Jest)

```bash
npm run test
```

**Test Coverage:**
- Component rendering
- User interactions
- Query model validation
- Settings persistence

### Integration Tests

**Manual testing:**
1. Install plugin in Grafana
2. Configure Arc connection
3. Test "Save & Test"
4. Create dashboard
5. Write queries with macros
6. Verify visualizations

## Build Process

### Frontend Build

```bash
npm install
npm run build
```

**Output:** `dist/` directory with:
- `module.js` - Bundled plugin code
- `plugin.json` - Metadata
- `img/` - Assets

### Backend Build

```bash
# Install mage
go install github.com/magefile/mage@latest

# Build for current platform
mage build

# Build for all platforms
mage buildAll
```

**Output:**
- `gpx_arc` (or `gpx_arc.exe` on Windows)
- `dist/<platform>_<arch>/gpx_arc`

### Complete Build

```bash
# Build everything
npm run build && mage buildAll
```

## Deployment

### Local Development

```bash
# Start Grafana with plugin
cp -r dist /var/lib/grafana/plugins/grafana-arc-datasource
systemctl restart grafana-server
```

### Production

1. Package plugin: `npm run build && mage buildAll`
2. Create release archive
3. Install on Grafana server
4. Configure datasource in Grafana UI
5. Create dashboards

## Future Enhancements

### Planned Features

1. **Query Builder UI**
   - Visual query builder (no SQL needed)
   - Drag-and-drop metric selection
   - Auto-complete for tables/columns

2. **Streaming Queries**
   - Real-time data updates
   - WebSocket support
   - Live dashboards

3. **Query Caching**
   - Cache frequently-run queries
   - Reduce Arc load
   - Faster dashboard load times

4. **Advanced Macros**
   - Custom macro definitions
   - Mathematical operations
   - Time zone handling

5. **Alerting Integration**
   - Native Grafana alerts
   - Arc-specific alert conditions
   - Performance thresholds

## Troubleshooting

### Common Issues

**Plugin not appearing:**
- Check plugin directory permissions
- Verify `plugin.json` is valid
- Restart Grafana

**Connection fails:**
- Test Arc: `curl http://localhost:8000/health`
- Verify API key has read permissions
- Check network connectivity

**Queries timeout:**
- Increase timeout in datasource settings
- Optimize SQL (add LIMIT, indexes)
- Check Arc query performance

**No data in panels:**
- Verify SQL syntax
- Check time range
- Ensure macros are used correctly
- Test query in Arc CLI

## References

- [Grafana Plugin Development](https://grafana.com/docs/grafana/latest/developers/plugins/)
- [Apache Arrow Go](https://github.com/apache/arrow/tree/main/go)
- [Arc Documentation](https://github.com/basekick-labs/arc)
