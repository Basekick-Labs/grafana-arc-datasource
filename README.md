# Arc Datasource for Grafana

High-performance Grafana datasource plugin for Arc time-series database using Apache Arrow for efficient data transfer.

## Features

- **Apache Arrow Protocol**: Uses Arc's `/api/v1/query/arrow` endpoint for columnar data transfer
- **High Performance**: 3-5x faster than JSON with 43% smaller payloads
- **Zero-Copy**: Direct Arrow IPC deserialization for minimal overhead
- **SQL Query Editor**: Full SQL support with syntax highlighting
- **Time-series Optimized**: Native support for time_bucket aggregations
- **Multi-database**: Query across different Arc databases/schemas
- **Secure**: API key authentication with secure credential storage

## Performance

Compared to traditional JSON datasources:
- **7.36x faster** for large result sets (100K+ rows)
- **43% smaller** network payloads
- **Zero-copy** data deserialization

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
| Use Arrow | Enable Arrow protocol | No | `true` (recommended) |

## Usage

### Query Editor

The Arc datasource provides a SQL query editor with:
- Syntax highlighting
- Auto-completion for tables and columns
- Time range macros

#### Example Queries

**Basic time-series query:**
```sql
SELECT
  time,
  host,
  usage_idle
FROM cpu
WHERE $__timeFilter(time)
ORDER BY time
```

**Aggregated metrics:**
```sql
SELECT
  time_bucket(INTERVAL '1 minute', time) as time,
  host,
  AVG(usage_idle) as avg_cpu_idle,
  MAX(usage_user) as max_cpu_user
FROM cpu
WHERE $__timeFilter(time)
GROUP BY time, host
ORDER BY time
```

**Multi-database query:**
```sql
SELECT
  p.time,
  p.host,
  p.usage_idle as prod_cpu,
  s.usage_idle as staging_cpu
FROM production.cpu p
JOIN staging.cpu s
  ON p.time = s.time
  AND p.host = s.host
WHERE $__timeFilter(p.time)
ORDER BY p.time DESC
LIMIT 1000
```

### Macros

The datasource provides several macros for dynamic queries:

| Macro | Description | Example |
|-------|-------------|---------|
| `$__timeFilter(column)` | Adds time range filter | `WHERE $__timeFilter(time)` |
| `$__timeFrom()` | Start of time range | `time >= $__timeFrom()` |
| `$__timeTo()` | End of time range | `time < $__timeTo()` |
| `$__interval` | Grafana's calculated interval | `time_bucket(INTERVAL '$__interval', time)` |
| `$__timeGroup(column, interval)` | Time bucketing | `$__timeGroup(time, '1m')` |

### Variables

Create dashboard variables to make queries dynamic:

**Host variable:**
```sql
SELECT DISTINCT host FROM cpu ORDER BY host
```

**Measurement variable:**
```sql
SHOW TABLES
```

Use in queries:
```sql
SELECT * FROM $measurement WHERE host = '$host'
```

## Development

### Prerequisites

- Node.js 18+
- Go 1.21+
- Mage (Go build tool)
- Grafana 10.0+

### Setup

```bash
# Install dependencies
npm install

# Install Go dependencies
go mod download

# Start development
npm run dev

# In another terminal, run backend
mage -v watch
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
Arc API (/api/v1/query/arrow)
  ↓
Apache Arrow IPC Response
  ↓
Arrow Decoder (Go)
  ↓
Grafana DataFrame
  ↓
Visualization
```

### Arrow Protocol

The datasource uses Arc's Arrow endpoint for optimal performance:

1. **Query Submission**: SQL query sent to Arc with time range
2. **Columnar Response**: Arc returns Apache Arrow IPC stream
3. **Zero-Copy Decode**: Go Arrow library deserializes directly
4. **DataFrame Conversion**: Arrow Table → Grafana DataFrame
5. **Rendering**: Grafana visualizes data

Benefits:
- No JSON serialization/deserialization overhead
- Columnar format perfect for time-series
- Compression at protocol level
- Type-safe data transfer

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

1. **Use time filters**: Always use `$__timeFilter()` macro
2. **Limit results**: Add `LIMIT` clause for large datasets
3. **Aggregate data**: Use `time_bucket()` for long time ranges
4. **Enable caching**: Configure Grafana query caching
5. **Use Arrow**: Keep Arrow protocol enabled (default)

## Contributing

Contributions welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Building a Release

```bash
# Build frontend
npm run build

# Build backend for all platforms
mage -v buildAll

# Create release archive
npm run package
```

## License

Apache License 2.0 - see [LICENSE](LICENSE)

## Support

- GitHub Issues: https://github.com/basekick-labs/grafana-arc-datasource/issues
- Arc Documentation: https://docs.arc.io
- Grafana Plugin Development: https://grafana.com/docs/grafana/latest/developers/plugins/

## Related Projects

- [Arc](https://github.com/basekick-labs/arc) - High-performance time-series database
- [Telegraf Arc Output](https://github.com/basekick-labs/telegraf) - Telegraf output plugin for Arc
- [arc-superset-arrow](https://pypi.org/project/arc-superset-arrow/) - Apache Superset dialect for Arc

---

Built with ❤️ by [Basekick Labs](https://github.com/basekick-labs)
