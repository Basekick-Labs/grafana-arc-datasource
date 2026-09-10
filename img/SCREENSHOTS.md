# Screenshots TODO

Add these screenshots to this directory:

## Required Screenshots

### 1. dashboard-overview.png
- **What to capture**: Full Grafana dashboard showing your system monitoring
- **Should include**:
  - Multiple panels (CPU, memory, disk, network)
  - Time range selector
  - Template variables dropdown (host selection)
  - At least 2-3 different visualization types
- **Recommended size**: 1920x1080 or similar
- **Tips**: Use a 6-hour time range to show good data variety

### 2. query-editor.png
- **What to capture**: Query editor panel in edit mode
- **Should include**:
  - SQL query text area with a real query
  - Arc datasource selected
  - Format dropdown (Time series/Table)
  - Query options visible
- **Example query to show**:
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
- **Recommended size**: 1200x800

### 3. variables.png
- **What to capture**: Dashboard settings → Variables page
- **Should include**:
  - Variable list showing defined variables (server, netif, etc.)
  - One variable's configuration expanded showing:
    - Query type
    - Arc datasource
    - SQL query (e.g., `SELECT DISTINCT host FROM telegraf.cpu ORDER BY host`)
    - Preview values
- **Recommended size**: 1200x800

### 4. alerting.png
- **What to capture**: Alert rule configuration page
- **Should include**:
  - Alert rule query with Arc datasource
  - Query showing something like CPU > 80%
  - Alert condition configuration
  - Notification channel settings (optional)
- **Example query**:
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
- **Recommended size**: 1200x800

## Optional Screenshots (Nice to have)

### 5. datasource-config.png
- Arc datasource configuration page showing:
  - URL input
  - API key (masked)
  - Database field
  - Arrow toggle
  - Save & Test button

### 6. panel-data.png
- Panel inspector showing:
  - Query tab with executed SQL
  - Data tab with actual results
  - Stats showing execution time

## Screenshot Tips

1. **Clean UI**: Hide any sensitive information (IPs, hostnames if needed)
2. **Good data**: Make sure panels show interesting data, not all zeros
3. **Light theme**: Screenshots usually look better in Grafana's light theme
4. **High resolution**: Use at least 1920x1080 for dashboard, 1200x800 for modals
5. **No personal data**: Avoid showing production secrets or private information
6. **Crop appropriately**: Focus on the relevant parts, remove unnecessary UI

## How to Take Screenshots

1. **Full page**: Use browser's built-in screenshot (Cmd+Shift+4 on Mac)
2. **Specific area**: Use Cmd+Shift+4, then drag to select area
3. **Clean up**: Use Preview or similar to crop/resize if needed
4. **Optimize**: Use ImageOptim or similar to reduce file size
5. **Save as PNG**: For better quality than JPG

## After Adding Screenshots

1. Verify all 4 required images are in `/img/` directory
2. Check they display correctly in README
3. Optimize file sizes (aim for <500KB each)
4. Commit with message: `docs: Add screenshots for README`
