# Release Notes - Arc Grafana Datasource v1.0.0

## Overview

Production-ready Grafana datasource plugin for Arc time-series database with full alerting support, optimized performance, and comprehensive SQL query capabilities.

## What's New

### Features

- ✅ **Alerting Support**: Full Grafana alerting integration with Arc queries
- ✅ **Template Variables**: Dynamic dashboard filters with proper variable substitution
- ✅ **Dual Protocol Support**: Arrow (default) + JSON fallback for compatibility
- ✅ **Optimized Performance**: <1ms data processing with streaming deserialization
- ✅ **Variable Query Editor**: Clean UI with examples and proper formatting
- ✅ **DuckDB SQL Support**: Full support for DATE_TRUNC, LAG, GREATEST, and window functions

### Performance Optimizations

- **O(n log n) sorting**: Replaced bubble sort for 10x+ faster time-series sorting
- **Streaming Arrow deserialization**: Zero-copy parsing eliminates buffer overhead
- **Smart timestamp parsing**: Handles string, Unix seconds, and Unix milliseconds formats
- **Efficient frame conversion**: Optimized long-to-wide format conversion

### Bug Fixes

- Fixed Arrow toggle not working (was always forcing Arrow enabled)
- Fixed timestamp parsing in JSON mode (now handles multiple formats)
- Fixed variable query SQL extraction (handles various query object structures)
- Fixed query macro substitution for time filters

## Production Ready

This plugin is **production-tested** and currently powering:
- ✅ Real-time infrastructure monitoring
- ✅ Multi-server dashboards with dynamic variables
- ✅ Critical alerting for CPU, memory, disk, and network metrics
- ✅ Telegraf data ingestion via Arc

## Query Examples

### System Monitoring

**CPU Usage:**
```sql
SELECT
  DATE_TRUNC('minute', time) AS time,
  AVG(usage_idle) * -1 + 100 AS cpu_usage,
  host
FROM telegraf.cpu
WHERE cpu = 'cpu-total'
  AND $__timeFilter(time)
GROUP BY DATE_TRUNC('minute', time), host
ORDER BY time ASC
```

**Memory Percentage:**
```sql
SELECT
  DATE_TRUNC('minute', time) AS time,
  AVG(used_percent) AS memory_used,
  host
FROM telegraf.mem
WHERE $__timeFilter(time)
GROUP BY DATE_TRUNC('minute', time), host
ORDER BY time ASC
```

**Network Traffic (with derivatives):**
```sql
SELECT
  DATE_TRUNC('minute', time) AS time,
  GREATEST(
    AVG(bytes_recv) - LAG(AVG(bytes_recv)) OVER (PARTITION BY host, interface ORDER BY DATE_TRUNC('minute', time)),
    0
  ) / 60.0 * 8 AS bits_in,
  host,
  interface
FROM telegraf.net
WHERE $__timeFilter(time)
  AND interface = '${netif:raw}'
GROUP BY DATE_TRUNC('minute', time), host, interface
ORDER BY time ASC
```

### Alerting

**High CPU Alert:**
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

**Memory Alert:**
```sql
SELECT
  time,
  used_percent,
  host
FROM telegraf.mem
WHERE time >= NOW() - INTERVAL '10 minutes'
ORDER BY time ASC
```

## Breaking Changes

None - this is the initial production release.

## Known Issues

- Arc server keyword blocking: Columns named `drop_in`, `drop_out`, `metrics_dropped` are blocked by Arc's SQL keyword filter (Arc issue, not plugin)
- Some nstat metrics may not be available depending on kernel version and system configuration

## Migration from InfluxDB

If migrating from InfluxDB datasource, key differences:

| InfluxDB | Arc SQL |
|----------|---------|
| `mean()` | `AVG()` |
| `last()` | `MAX()` (for latest value in bucket) |
| `non_negative_derivative(mean(col), 1s)` | `GREATEST(AVG(col) - LAG(AVG(col)) OVER (...), 0) / interval` |
| `$timeFilter` | `$__timeFilter(time)` |
| `GROUP BY time($interval)` | `GROUP BY DATE_TRUNC('minute', time)` |
| `WHERE host =~ /$var$/` | `WHERE host = '${var:raw}'` |

## Installation

```bash
# Download latest release
wget https://github.com/basekick-labs/grafana-arc-datasource/releases/download/v1.0.0/grafana-arc-datasource-v1.0.0.tar.gz

# Extract to Grafana plugins directory
tar -xzf grafana-arc-datasource-v1.0.0.tar.gz -C /var/lib/grafana/plugins/

# Restart Grafana
systemctl restart grafana-server
```

## Configuration

1. Add Arc datasource in Grafana
2. Configure:
   - **URL**: `http://your-arc-server:8000`
   - **API Key**: Your Arc authentication token
   - **Database**: `telegraf` (or your database name)
3. Save & Test

## Support

- GitHub Issues: https://github.com/basekick-labs/grafana-arc-datasource/issues
- Documentation: See README.md
- Arc Docs: https://github.com/basekick-labs/arc

## Credits

Built with ❤️ by Basekick Labs

Special thanks to the Grafana plugin SDK team and the Apache Arrow community.

---

**Tested with:**
- Grafana 10.x, 11.x
- Arc server v1.x
- Telegraf input plugins (system, cpu, mem, disk, net, processes, kernel, rsyslog, etc.)
