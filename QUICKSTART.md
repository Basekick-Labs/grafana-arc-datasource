# Quick Start Guide

Get up and running with the Arc Grafana datasource in 5 minutes.

## Prerequisites

- Grafana 10.0+ installed
- Arc database running (http://localhost:8000 by default)
- Arc API token with read permissions

## Step 1: Get Arc API Token

```bash
# Using Arc CLI
cd /path/to/arc
python3 cli.py auth create-token --name grafana --permissions read

# Output:
# Token created: arc_1234567890abcdef...
```

**Save this token** - you'll need it for Grafana configuration.

## Step 2: Build Plugin

```bash
# Clone repository
git clone https://github.com/basekick-labs/grafana-arc-datasource
cd grafana-arc-datasource

# Install dependencies
npm install
go mod download

# Install Mage (if not already installed)
go install github.com/magefile/mage@latest

# Build plugin
npm run build
mage build
```

## Step 3: Install Plugin

### Option A: Development (Symlink)

```bash
# Create symlink
sudo ln -s $(pwd)/dist /var/lib/grafana/plugins/grafana-arc-datasource

# Allow unsigned plugin
sudo tee -a /etc/grafana/grafana.ini > /dev/null <<EOF
[plugins]
allow_loading_unsigned_plugins = basekick-arc-datasource
EOF

# Restart Grafana
sudo systemctl restart grafana-server
```

### Option B: Production (Copy)

```bash
# Copy to plugins directory
sudo cp -r dist /var/lib/grafana/plugins/grafana-arc-datasource

# Set permissions
sudo chown -R grafana:grafana /var/lib/grafana/plugins/grafana-arc-datasource

# Restart Grafana
sudo systemctl restart grafana-server
```

### Option C: Docker

```bash
# Build plugin first (as above)
# Then mount in docker-compose.yml:

services:
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=basekick-arc-datasource
    volumes:
      - ./grafana-arc-datasource/dist:/var/lib/grafana/plugins/grafana-arc-datasource
```

## Step 4: Configure Datasource

1. **Open Grafana**
   - Navigate to: http://localhost:3000
   - Login (default: admin/admin)

2. **Add Datasource**
   - Go to: Configuration → Data sources
   - Click "Add data source"
   - Search for "Arc"
   - Click "Select"

3. **Configure Settings**
   ```
   URL:      http://localhost:8000
   API Key:  arc_1234567890abcdef...  (from Step 1)
   Database: default
   Timeout:  30
   Use Arrow: ✓ (enabled)
   ```

4. **Test Connection**
   - Click "Save & Test"
   - Should see: ✅ "Arc datasource is working"

## Step 5: Create Dashboard

1. **New Dashboard**
   - Click "+" → Dashboard
   - Click "Add new panel"

2. **Select Arc Datasource**
   - Data source dropdown → Arc

3. **Write Query**
   ```sql
   SELECT
     time,
     host,
     usage_idle,
     usage_user
   FROM cpu
   WHERE $__timeFilter(time)
   ORDER BY time
   LIMIT 1000
   ```

4. **Configure Visualization**
   - Panel type: Time series
   - Title: "CPU Usage"

5. **Save Dashboard**
   - Click "Apply"
   - Click "Save dashboard"
   - Name: "Arc System Metrics"

## Step 6: Add More Queries

### Query 2: Average CPU by Host

```sql
SELECT
  time_bucket(INTERVAL '$__interval', time) as time,
  host,
  AVG(usage_idle) as avg_cpu_idle,
  MAX(usage_user) as max_cpu_user
FROM cpu
WHERE $__timeFilter(time)
GROUP BY time, host
ORDER BY time
```

### Query 3: Memory Usage

```sql
SELECT
  time,
  host,
  used_percent
FROM mem
WHERE $__timeFilter(time)
  AND host = 'server-01'
ORDER BY time
LIMIT 1000
```

### Query 4: Cross-Measurement Join

```sql
SELECT
  c.time,
  c.host,
  c.usage_idle as cpu_idle,
  m.used_percent as mem_used
FROM cpu c
JOIN mem m
  ON c.time = m.time
  AND c.host = m.host
WHERE $__timeFilter(c.time)
ORDER BY c.time
LIMIT 1000
```

## Step 7: Add Template Variables

1. **Create Variable**
   - Dashboard settings → Variables → Add variable
   - Name: `host`
   - Type: Query

2. **Variable Query**
   ```sql
   SELECT DISTINCT host FROM cpu ORDER BY host
   ```

3. **Use in Queries**
   ```sql
   SELECT * FROM cpu
   WHERE $__timeFilter(time)
     AND host = '$host'
   ```

## Troubleshooting

### Plugin Not Appearing

**Check plugin directory:**
```bash
ls -la /var/lib/grafana/plugins/
# Should see: grafana-arc-datasource
```

**Check Grafana logs:**
```bash
sudo tail -f /var/log/grafana/grafana.log
# Look for plugin loading errors
```

**Verify unsigned plugin config:**
```bash
grep "allow_loading_unsigned_plugins" /etc/grafana/grafana.ini
# Should see: allow_loading_unsigned_plugins = basekick-arc-datasource
```

### Connection Test Fails

**Verify Arc is running:**
```bash
curl http://localhost:8000/health
# Should return: {"status":"ok"}
```

**Test API token:**
```bash
curl -H "x-api-key: arc_your_token_here" \
     http://localhost:8000/api/v1/query/arrow \
     -X POST \
     -H "Content-Type: application/json" \
     -d '{"sql":"SHOW DATABASES"}'
```

**Check token permissions:**
```bash
cd /path/to/arc
python3 cli.py auth list-tokens
# Verify token has 'read' permission
```

### No Data in Panels

**Verify data exists:**
```bash
# Using Arc CLI
python3 cli.py query "SELECT COUNT(*) FROM cpu"
```

**Check time range:**
- Ensure dashboard time range overlaps with data
- Try "Last 24 hours" or "Last 7 days"

**Verify SQL syntax:**
- Test query in Arc CLI first
- Ensure macros are used correctly

**Check browser console:**
- F12 → Console
- Look for error messages

## Next Steps

- [ ] Read full [README.md](README.md) for advanced features
- [ ] Review [ARCHITECTURE.md](ARCHITECTURE.md) for technical details
- [ ] Create custom dashboards for your use case
- [ ] Set up Grafana alerts based on Arc queries
- [ ] Explore multi-database queries
- [ ] Configure template variables for dynamic filtering

## Example Dashboards

### System Overview Dashboard

**Panels:**
1. CPU Usage (time series)
2. Memory Usage (time series)
3. Disk I/O (time series)
4. Network Traffic (time series)
5. Top Hosts by CPU (table)
6. Alert Status (stat)

**Variables:**
- `host` - Filter by host
- `interval` - Aggregation interval

### Application Metrics Dashboard

**Panels:**
1. Request Rate (time series)
2. Response Time (time series)
3. Error Rate (time series)
4. Active Connections (gauge)
5. Database Query Time (heatmap)

## Resources

- **Arc Documentation:** https://github.com/basekick-labs/arc
- **Grafana Dashboards:** https://grafana.com/grafana/dashboards/
- **SQL Reference:** DuckDB SQL documentation
- **Support:** GitHub Issues

## Getting Help

- **Issues:** https://github.com/basekick-labs/grafana-arc-datasource/issues
- **Discussions:** https://github.com/basekick-labs/grafana-arc-datasource/discussions
- **Email:** support@basekick.com
