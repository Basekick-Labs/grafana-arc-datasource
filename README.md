# Arc Datasource for Grafana

High-performance Grafana datasource plugin for Arc time-series database using Apache Arrow for efficient data transfer.

## Screenshots

### Dashboard with Real-time Monitoring
![System monitoring dashboard showing CPU, memory, disk, and network metrics](https://raw.githubusercontent.com/basekick-labs/grafana-arc-datasource/main/img/dashboard.png)

### Query Editor
![SQL query editor with Arc datasource](https://raw.githubusercontent.com/basekick-labs/grafana-arc-datasource/main/img/query-editor.png)

### Data Inspector
![Query results and data inspection](https://raw.githubusercontent.com/basekick-labs/grafana-arc-datasource/main/img/inspect.png)

### Variable Configuration
![Template variable configuration with dynamic host selection](https://raw.githubusercontent.com/basekick-labs/grafana-arc-datasource/main/img/variables.png)

### Datasource Configuration
![Arc datasource settings and connection setup](https://raw.githubusercontent.com/basekick-labs/grafana-arc-datasource/main/img/datasource.png)

### Alerting
![Alert rule configuration with Arc SQL queries](https://raw.githubusercontent.com/basekick-labs/grafana-arc-datasource/main/img/alerts.png)

## Features

- **Three wire protocols**: Apache Arrow IPC (default, fastest), columnar MessagePack (stable since Arc 26.09.1), and JSON (compatibility fallback)
- **High Performance**: Streaming deserialization straight into Grafana DataFrames
- **Query Splitting**: Large time ranges chunked and executed in parallel, with automatic skip for queries where splitting would change results
- **Alerting Support**: Full support for Grafana alerting and notifications
- **SQL Query Editor**: Direct DuckDB SQL with Grafana macro expansion
- **Template Variables**: Dynamic dashboard filters with variable support
- **Time-series Optimized**: Native support for DuckDB time functions
- **Multi-database**: Query across different Arc databases/schemas (opt-in per-query override)
- **Secure**: API key authentication with secure credential storage, SSRF-guarded outbound connections

## Performance

Real-world performance characteristics:
- **Data processing**: typically sub-millisecond per chunk for time-series conversion
- **Query execution**: dominated by Arc server (typically 100-500ms)
- **Columnar transfer**: Arrow IPC streamed from Arc, decoded into Grafana DataFrames
- **Optimized sorting**: O(n log n) time-series sorting when post-sort is needed

## Installation

### From Release

1. Download the latest release from GitHub releases
2. Extract to your Grafana plugins directory:
   ```bash
   unzip grafana-arc-datasource-<version>.zip -d /var/lib/grafana/plugins/
   ```
3. Restart Grafana:
   ```bash
   systemctl restart grafana-server
   ```

### From Source

```bash
# Clone repository
git clone https://github.com/basekick-labs/grafana-arc-datasource
cd grafana-arc-datasource

# Install dependencies
npm install

# Build plugin
npm run build

# Build backend
mage -v

# Install to Grafana
cp -r dist /var/lib/grafana/plugins/grafana-arc-datasource
systemctl restart grafana-server
```

## Configuration

1. In Grafana, go to **Configuration** → **Data sources**
2. Click **Add data source**
3. Search for and select **Arc**
4. Configure connection:
   - **URL**: Arc API endpoint (e.g., `http://localhost:8000`)
   - **API Key**: Your Arc authentication token
   - **Database** (optional): Default database/schema
5. Click **Save & Test**

### Configuration Options

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| URL | Arc API base URL | Yes | - |
| API Key | Authentication token | Yes | - |
| Database | Default database name | No | `default` |
| Timeout | Query timeout in seconds | No | `30` |
| Protocol | Query response wire format: `Arrow` (fastest, recommended), `MessagePack`, or `JSON` | No | `Arrow` |
| Max Concurrency | Maximum parallel Arc requests per datasource (query splitting fan-out) | No | `4` |
| Max Response MB | Per-response body size cap in MiB | No | `1024` |
| Allow Private IPs | Permit the Arc URL to resolve to private/RFC1918 addresses | No | off |
| Allow Database Override | Permit per-query `database` field to override the default | No | off |

### Choosing a protocol

- **Arrow** decodes Arc's Arrow IPC stream directly and is the fastest option; keep it unless you have a reason not to.
- **MessagePack** uses Arc's columnar `/api/v1/query/msgpack` endpoint (stable since Arc 26.09.1). It reaches roughly 78% of Arrow's decode throughput and additionally supports gzip response compression, which can help on constrained links.
- **JSON** is the slowest path and exists for compatibility and debugging.

## Usage

### Query Editor

The Arc datasource provides a SQL query editor with:
- Time range macros expanded server-side (safe against injection via literals/comments)
- Result format selection (time series or table)
- Query splitting controls and per-query database override

#### Example Queries

**Basic time-series query (CPU usage):**
```sql
SELECT
  $__timeGroup(time, '$__interval') AS time,
  AVG(usage_idle) * -1 + 100 AS cpu_usage,
  host
FROM telegraf.cpu
WHERE cpu = 'cpu-total'
  AND $__timeFilter(time)
GROUP BY 1, host
ORDER BY 1 ASC
```

**Memory usage:**
```sql
SELECT
  $__timeGroup(time, '$__interval') AS time,
  AVG(used_percent) AS memory_used,
  host
FROM telegraf.mem
WHERE $__timeFilter(time)
GROUP BY 1, host
ORDER BY 1 ASC
```

**Network traffic (bytes to bits):**
```sql
SELECT
  $__timeGroup(time, '$__interval') AS time,
  AVG(bytes_recv) * 8 AS bits_in,
  host,
  interface
FROM telegraf.net
WHERE $__timeFilter(time)
GROUP BY 1, host, interface
ORDER BY 1 ASC
```

> Prefer `$__timeGroup` over raw `time_bucket`/`date_trunc` for bucketing:
> DuckDB's `date_trunc` retains nanosecond residuals on `TIMESTAMP_NS`
> columns, which makes `GROUP BY` produce per-second rows. The macro expands
> to integer epoch math that avoids this.

### Macros

The datasource provides several macros for dynamic queries:

| Macro | Description | Example |
|-------|-------------|---------|
| `$__timeFilter(columnName)` | Complete time range filter | `WHERE $__timeFilter(time)` |
| `$__timeFrom()` | Start of time range | `time >= $__timeFrom()` |
| `$__timeTo()` | End of time range | `time < $__timeTo()` |
| `$__interval` | Grafana's calculated interval | `$__timeGroup(time, '$__interval')` |
| `$__timeGroup(columnName, interval)` | Epoch-based time bucketing | `$__timeGroup(time, '1m') AS time` |

### Variables

Create dashboard variables to make queries dynamic:

**Host variable:**
```sql
SELECT DISTINCT host FROM telegraf.cpu ORDER BY host
```

**Interface variable:**
```sql
SELECT DISTINCT interface FROM telegraf.net ORDER BY interface
```

Use variables in queries with `$variable` syntax:
```sql
SELECT
  time_bucket(INTERVAL '$__interval', time) as time,
  AVG(usage_idle) * -1 + 100 AS cpu_usage
FROM telegraf.cpu
WHERE host = '$server'
  AND cpu = 'cpu-total'
  AND $__timeFilter(time)
GROUP BY time_bucket(INTERVAL '$__interval', time)
ORDER BY time ASC
```

### Alerting

The datasource fully supports Grafana alerting. Create alert rules with Arc queries:

**Example alert query (CPU usage > 80%):**
```sql
SELECT
  time,
  100 - usage_idle AS cpu_usage,
  host
FROM telegraf.cpu
WHERE cpu = 'cpu-total'
  AND time >= NOW() - INTERVAL '5 minutes'
ORDER BY time ASC
```

Then set alert condition: `WHEN avg() OF query(A, 5m, now) IS ABOVE 80`

## Development

### Prerequisites

- Node.js 18+
- Go 1.25+
- Mage (Go build tool)
- Grafana 10.0+

### Setup

```bash
# Install dependencies
npm install

# Install Go dependencies
go mod download

# Full development build: frontend bundle + backend binary into dist/
mage dev

# Or iterate on the frontend with webpack watch
npm run dev
```

### Project Structure

```
grafana-arc-datasource/
├── src/
│   ├── datasource.ts       # Main datasource implementation
│   ├── ConfigEditor.tsx    # Configuration UI
│   ├── QueryEditor.tsx     # Query editor UI
│   ├── VariableQueryEditor.tsx
│   └── module.ts           # Plugin entry point
├── pkg/
│   ├── plugin/
│   │   ├── datasource.go   # Backend datasource
│   │   ├── query.go        # Query handling
│   │   └── arrow.go        # Arrow protocol implementation
│   └── main.go
├── plugin.json             # Plugin metadata
├── package.json
├── go.mod
└── README.md
```

### Testing

```bash
# Frontend tests
npm run test

# Backend tests
go test ./pkg/...

# E2E tests
npm run e2e
```

## Architecture

### Data Flow

```
Grafana Dashboard
  ↓
Query Request (SQL)
  ↓
Frontend (TypeScript)
  ↓
Backend (Go)
  ↓
Arc API (/api/v1/query/arrow, /api/v1/query/msgpack, or /api/v1/query)
  ↓
Columnar Response (Arrow IPC / MessagePack / JSON)
  ↓
Streaming Decoder (Go)
  ↓
Grafana DataFrame
  ↓
Visualization
```

### Wire Protocols

Regardless of protocol, the flow is the same:

1. **Query Submission**: SQL query sent to Arc with time range macros expanded
2. **Columnar Response**: Arc returns the result in the configured wire format
3. **Streaming Decode**: The backend decodes the response incrementally
4. **DataFrame Conversion**: Columns land in Grafana DataFrames with identical field types across all three protocols
5. **Rendering**: Grafana visualizes data

Benefits of the binary formats (Arrow, MessagePack) over JSON:
- No JSON serialization/deserialization overhead
- Columnar layout well-suited for time-series
- Typed data transfer, no string-based type inference

## Troubleshooting

### Connection Issues

**Error: "Failed to connect to Arc"**
- Verify Arc is running: `curl http://localhost:8000/health`
- Check URL in datasource configuration
- Verify network connectivity

**Error: "Authentication failed"**
- Verify API key is valid
- Check token hasn't expired
- Ensure token has read permissions

### Query Issues

**Error: "Table not found"**
- Run `SHOW TABLES` to list available tables
- Verify database name is correct
- Check Arc has data for the measurement

**Slow queries:**
- Add `LIMIT` clause to limit result size
- Use time range filters with `$__timeFilter()`
- Add appropriate indexes in Arc
- Check Arc query performance with `EXPLAIN`

### Plugin Issues

**Plugin not appearing in Grafana:**
- Check plugin directory permissions
- Verify `plugin.json` is valid
- Restart Grafana after installation
- Check Grafana logs: `/var/log/grafana/grafana.log`

**Backend plugin not working:**
- Ensure backend binary is compiled: `mage -v`
- Check binary has execute permissions
- Verify Go version compatibility

## Performance Tips

1. **Keep the Arrow protocol**: Arrow is the default and the fastest transfer format; MessagePack is a close second, JSON the slowest
2. **Optimize time ranges**: Smaller time ranges mean faster queries. Use Grafana's time picker to narrow down your analysis
3. **Leverage $__timeGroup**: Bucket with `$__timeGroup(time, '$__interval')` to avoid returning millions of points. Grafana automatically adjusts `$__interval` based on your dashboard width
4. **Index your time column**: Arc automatically indexes time columns, but ensure your queries filter by time first for optimal performance
5. **Enable caching**: Configure Grafana query caching for frequently accessed data

## Contributing

Contributions welcome! Please see [CONTRIBUTING.md](https://github.com/basekick-labs/grafana-arc-datasource/blob/main/CONTRIBUTING.md) for guidelines.

### Building a Release

```bash
# Build frontend, then backend binaries for all platforms into dist/
npm run build
mage -v buildAll
```

Releases are cut by pushing a `v*` tag; the GitHub Actions release workflow builds, packages, and validates the plugin archive.

## License

Apache License 2.0 - see [LICENSE](https://github.com/basekick-labs/grafana-arc-datasource/blob/main/LICENSE)

## Support

- GitHub Issues: https://github.com/basekick-labs/grafana-arc-datasource/issues
- Arc Documentation: https://docs.basekick.net/arc
- Grafana Plugin Development: https://grafana.com/docs/grafana/latest/developers/plugins/

## Related Projects

- [Arc](https://github.com/basekick-labs/arc) - High-performance time-series database
- [Telegraf Arc Output](https://github.com/basekick-labs/telegraf) - Telegraf output plugin for Arc
- [arc-superset-arrow](https://pypi.org/project/arc-superset-arrow/) - Apache Superset dialect for Arc

---

Built with ❤️ by [Basekick Labs](https://github.com/basekick-labs)
