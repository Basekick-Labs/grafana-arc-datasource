# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `$__timeGroup` buckets by the dashboard's timezone. Previously a "day"
  bucket started at 00:00 UTC, so a dashboard in UTC-6 showed bars that each
  mixed two local calendar days, and the first and last bar of a range were
  partial. Hour, day and week buckets now truncate on local boundaries,
  including across DST transitions where a local day is not 86400 seconds
  long. "Browser Time" is resolved per viewer, so two people in different
  zones each see their own days.

  UTC dashboards are unaffected: they produce byte-identical SQL to previous
  releases, on purpose. `date_trunc` keeps nanosecond residuals on
  `TIMESTAMP_NS` columns, and on a `TIMESTAMPTZ` column it truncates in the
  database session's timezone — so a "UTC" dashboard on the calendar path
  would silently follow Arc's session setting instead of UTC.

  Sub-hour buckets, and spans that are not whole calendar units (`6h`, `12h`,
  `3d`), stay on epoch arithmetic, which is correct for them. `7d` is
  deliberately not treated as a calendar week: `date_trunc('week')` anchors on
  Monday, and "seven days wide" does not mean "Monday to Sunday". Ask for `1w`
  if that is what you want.

### Fixed
- Query splitting no longer corrupts wide time buckets. A bucket wider than a
  chunk was aggregated once per chunk it spanned and merged back as several
  partial rows sharing one timestamp: a one-day bucket over a four-day range
  returned 16 rows where 5 were correct, each carrying a fraction of the true
  count. Such queries now run unsplit. This affected UTC dashboards too, and
  predates the timezone feature.
- The plugin binary embeds the IANA timezone database. `time.LoadLocation`
  otherwise reads the host's zoneinfo, which Grafana's distroless image and
  Windows hosts do not ship, and the failure was silent: every zone degraded
  to UTC with no error.

## [1.4.0] - 2026-09-10

Restores compatibility with dashboards written against 1.2.0. Upgrade straight
from 1.2.0; releases 1.3.0 through 1.3.11 are superseded.

### Changed
- Several defaults introduced by the 1.3.2 hardening are restored to what
  1.2.0 did, because they rejected input that had been valid. Each is still
  configurable; only the default changed.

  - **Private/RFC1918 Arc URLs are permitted again.** Self-hosted Arc usually
    runs on a private network or a Docker service name like
    `http://arc:8000`, and blocking those by default meant the datasource
    could not connect at all. An explicit "Allow Private IPs" setting is still
    honoured; link-local and cloud-metadata addresses remain blocked.
  - **The per-query database override works again** without first enabling a
    toggle. It has been an advertised feature since 1.1.0.
  - **A datasource with no protocol recorded resolves to JSON**, as in 1.2.0.
    Resolving it to Arrow broke `SHOW DATABASES` / `SHOW TABLES` variable
    queries, which Arc's Arrow endpoint rejects, and changed column type
    inference. New datasources still default to Arrow.
  - **`$__timeGroup` accepts any `<n><unit>` interval**, not 13 fixed strings.
    Grafana's own `$__interval` routinely produces `20s`, `2m` and `2h`, none
    of which were accepted, so the macro was left unexpanded and Arc received
    a literal `$`.
  - **Macro column arguments accept expressions**: `"time"`, `t."time"`,
    `time::TIMESTAMP`, a function call, a non-ASCII name. Only characters that
    could break out of the generated SQL are refused.
  - **`$__timeGroup` tolerates the Postgres/Timescale fill argument** instead
    of rejecting the whole macro, so migrated dashboards keep working.
  - **Concurrency is two settings.** "Max Concurrency" (default 4) shapes one
    query's chunk fan-out; the new "Max In Flight" (default 32) bounds the
    datasource across all panels. Using one number for both meant a 12-panel
    dashboard served requests four at a time.

### Changed
- Several defaults introduced by the 1.3.2 hardening are restored to what
  1.2.0 did, because they rejected input that had been valid. Each is still
  configurable; only the default changed.

  - **Private/RFC1918 Arc URLs are permitted again.** Self-hosted Arc usually
    runs on a private network or a Docker service name like
    `http://arc:8000`, and blocking those by default meant the datasource
    could not connect at all. An explicit "Allow Private IPs" setting is still
    honoured; link-local and cloud-metadata addresses remain blocked.
  - **The per-query database override works again** without first enabling a
    toggle. It has been an advertised feature since 1.1.0.
  - **A datasource with no protocol recorded resolves to JSON**, as in 1.2.0.
    Resolving it to Arrow broke `SHOW DATABASES` / `SHOW TABLES` variable
    queries, which Arc's Arrow endpoint rejects, and changed column type
    inference. New datasources still default to Arrow.
  - **`$__timeGroup` accepts any `<n><unit>` interval**, not 13 fixed strings.
    Grafana's own `$__interval` routinely produces `20s`, `2m` and `2h`, none
    of which were accepted, so the macro was left unexpanded and Arc received
    a literal `$`.
  - **Macro column arguments accept expressions**: `"time"`, `t."time"`,
    `time::TIMESTAMP`, a function call, a non-ASCII name. Only characters that
    could break out of the generated SQL are refused.
  - **`$__timeGroup` tolerates the Postgres/Timescale fill argument** instead
    of rejecting the whole macro, so migrated dashboards keep working.
  - **Concurrency is two settings.** "Max Concurrency" (default 4) shapes one
    query's chunk fan-out; the new "Max In Flight" (default 32) bounds the
    datasource across all panels. Using one number for both meant a 12-panel
    dashboard served requests four at a time.

### Fixed
- Template variables are interpolated by Grafana's own rule again: a value is
  quoted only when the variable is multi-value or has an "Include All" option,
  and is otherwise escaped without adding quotes. Since 1.3.2 every
  single-value variable was wrapped in quotes, so the standard idioms
  `WHERE host = '$server'` and `host ~ '^$server$'` produced `''h01''` and
  `'^'h01'$'` — both parser errors — and Grafana's own `$__interval` became
  `INTERVAL ''30s''`.

  The 1.3.2 change came from a security finding asserting that Grafana's
  Postgres datasource quotes unconditionally. It does not, and never has:
  upstream `SqlDatasource.interpolateVariable` quotes only multi/All values.
  Embedded quotes are still doubled on both paths, which is the part that
  actually keeps a URL-supplied value inside the literal it is placed in.

- `$__interval` expands inside string literals again. Its documented form is
  `time_bucket('$__interval', time)` / `INTERVAL '$__interval'`, and the
  literal-skipping introduced in 1.3.2 left the token unexpanded, so DuckDB
  rejected it with "Could not convert string '$__interval' to INTERVAL". The
  parenthesised macros (`$__timeFilter`, `$__timeFrom()`, `$__timeTo()`,
  `$__timeGroup`) still skip literals, since their expansions carry their own
  quotes.

- `$__interval_ms` is no longer clobbered. It shares a prefix with
  `$__interval`, so a substring replacement rewrote it to `10 seconds_ms`.
  Both tokens are now matched on a word boundary, which also leaves any
  unrecognised `$__interval*` token untouched rather than corrupting it, and
  makes the replacement order irrelevant. The interval macros skip SQL
  comments, so query text shown in Grafana's inspector still matches what the
  author wrote. Only backend-only paths (alerting, recorded queries) see
  either token unexpanded.

- A multi-value variable with nothing selected now interpolates as `NULL`
  instead of an empty string, so `WHERE host IN ($hosts)` degrades to a query
  matching no rows rather than `IN ()`, a parser error.

- Query splitting no longer runs on queries it cannot safely split. A query
  that buckets with `$__timeGroup` but takes its range from a literal `WHERE`
  was split into chunks that each re-ran the same unfiltered query, so every
  row came back once per chunk and every total was inflated. Splitting now
  requires `$__timeFilter`, or both `$__timeFrom()` and `$__timeTo()`.
- `ORDER BY time ASC` is added again for time-series queries that lack one. It
  was advertised in 1.1.0 and silently disabled in 1.3.2 because it matched
  "time" as a substring, rewriting queries whose only "time" was inside
  `lifetime` or `timestamp`. Table-format queries keep the author's row order.
- DuckDB parser and syntax errors reach the panel again instead of "query
  failed (see server logs for detail)". They quote the author's own SQL back
  at them, so hiding them meant reading the server log to find a mistake that
  was fixable in the editor. Every other DuckDB error type stays summarised:
  catalog errors name tables, binder errors append a list of candidate column
  names, and conversion errors echo an actual row value, none of which a
  dashboard viewer should learn from a failed panel. The failing SQL is now on
  the error-level log line for operators.
- `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` are honoured again; the custom
  transport added in 1.3.2 ignored them, so Grafana behind an egress proxy
  could not reach a cloud-hosted Arc. Idle connections are also kept for five
  minutes rather than 90 seconds, so a refreshing dashboard stops re-dialling.
- A query chunk dropped for a schema mismatch now raises a panel notice rather
  than silently returning a partial series.

### Note on 1.3.3 - 1.3.11
Those releases were withdrawn. They attempted to patch the regression above
without a verification loop against real dashboards, and each one fixed one
query idiom while breaking another. Use 1.2.0 until this release ships.

## [1.3.2] - 2026-09-02

### Fixed
- Backend binaries are now built with Go 1.26.6, clearing the 8 standard-library vulnerabilities the Grafana catalog's govulncheck binary scan reported against the 1.3.1 build.

## [1.3.1] - 2026-09-02

### Changed
- Migrated the repository to Grafana's official create-plugin scaffolding, as required by the plugin catalog's source and build-tooling checks: plugin.json moved to src/, standard webpack/jest/eslint configuration under .config/, SDK-standard Magefile that emits the Go build manifest, and the grafana/plugin-actions release pipeline.
- Updated grafana-plugin-sdk-go to v0.296.4 and the frontend Grafana packages to 13.2.1, clearing all high and critical vulnerabilities the catalog's dependency scanner flagged in the previous lockfile.
- Minimum supported Grafana version is now 12.3 (floor of the current create-plugin toolchain).

### Added
- Playwright e2e smoke tests (config editor, query editor) running against Grafana in Docker on CI.

## [1.3.0] - 2026-09-02

### Added
- MessagePack protocol support: new datasource Protocol selector (Arrow, MessagePack, JSON) wired to Arc's `/api/v1/query/msgpack` endpoint (stable since Arc 26.09.1). The columnar envelope is decoded in a single streaming pass, with identical Grafana field types across all three protocols (int64/uint64 promoted to float64, all fields nullable). The legacy Use Arrow toggle is migrated automatically.
- MessagePack-aware error parsing: the msgpack endpoint encodes its errors as msgpack; these are now decoded to readable messages instead of falling through to a raw-bytes fallback.
- CI workflow: build, typecheck, lint, and tests for both frontend and backend on every push and pull request, plus a plugin-validator run against the packaged plugin.

### Fixed
- Arrow decoder: DuckDB DATE columns (Arrow date32) now decode to time fields instead of strings, matching the MessagePack path so switching protocols never changes a dashboard's field types.
- Health check: `Save & Test` failed on the default (Arrow) configuration because Arc rejects `SHOW DATABASES` on the Arrow endpoint. The probe now runs `SELECT 1` through the configured protocol.
- Release workflow: binary copy steps referenced the pre-rewrite Magefile output layout (`dist/linux_amd64/gpx_arc`), which no longer exists, so tag builds failed. The workflow now uses the flat `dist/gpx_arc_<os>_<arch>` layout the Magefile produces, pins Go from `go.mod` instead of a stale 1.21, runs tests before packaging, verifies the tag matches `plugin.json`, and re-enables plugin-validator.
- Grafana catalog readiness: README links converted to absolute URLs (required by the catalog), dead `docs.arc.io` link replaced with `docs.basekick.net/arc`, LICENSE upgraded to the full Apache 2.0 text, and stale documentation claims removed (editor auto-completion, `mage watch`, `npm run package`).

## [1.2.0] - 2026-05-14

### Fixed
- Promote INT64/UINT64 Arrow columns to float64 so Grafana panels (Stat, Time series) recognize aggregate results as numeric value fields.
- Aggregation detection now strips string literals before keyword matching, so a literal such as `'count of things'` no longer disables query splitting.
- Error chain preserved through request-failure wrapping for programmatic inspection via `errors.Is`/`errors.As`.

## [1.1.0] - 2026-02-20

### Fixed
- Fix LongToWide null-fill bloat: `FillModeNull` expanded hourly data into per-second null-filled rows (604K rows / 59MB for a 7-day query). Pass `nil` instead to only include timestamps present in source data.
- Fix `$__timeGroup` precision: DuckDB's `date_trunc` retains nanosecond residuals on `TIMESTAMP_NS` columns, causing `GROUP BY` to produce per-second rows. Replaced with epoch-based integer math (`epoch_ns // interval`).
- Fix `$__timeFilter` hardcoded to `time` column: now dynamically extracts the column name from the macro argument.
- Fix error messages: surface Arc errors directly in UI instead of generic "query failed" messages. Add user-friendly messages for timeouts, connection refused, and EOF errors while preserving the original error chain.

### Added
- Query splitting: break large time ranges into parallel chunks executed concurrently. Configurable via query editor dropdown (Auto, Off, 1h, 6h, 12h, 1d, 3d, 7d). Auto mode picks chunk size based on the time range.
- Smart split-skipping: automatically bypasses splitting for LIMIT queries, aggregations without `$__timeGroup`, queries without `$__timeFilter`, UNION queries, and window functions.
- Per-query database override: specify a different database per query panel, overriding the datasource default.
- Auto-migrate `rawSql` from Postgres/MySQL/MSSQL/ClickHouse datasources when switching to Arc.
- Auto-add `ORDER BY time ASC` for time series queries without one.
- Configurable max concurrency for query splitting (default 4) via datasource settings.
- 40 unit tests covering query splitting, macros, frame merging, and aggregation detection.

## [1.0.0] - 2025-10-22

### Added
- Initial release of Arc Grafana datasource plugin
- Apache Arrow protocol support for high-performance data transfer
- Backend plugin (Go) for secure credential storage
- Frontend UI components (TypeScript/React):
  - ConfigEditor for datasource settings
  - QueryEditor with SQL text area
  - VariableQueryEditor for template variables
- Grafana macro support:
  - `$__timeFilter(column)` - Automatic time range filtering
  - `$__timeFrom()` / `$__timeTo()` - Time boundaries
  - `$__interval` - Auto-calculated interval
  - `$__timeGroup(column, interval)` - Time bucketing
- Multi-database query support
- Health check endpoint
- Comprehensive documentation (README, ARCHITECTURE)
- Build system with webpack and mage
- Support for all Arrow data types (INT64, FLOAT64, STRING, TIMESTAMP, BOOL)

### Performance
- 7.36x faster queries compared to JSON for large datasets (100K+ rows)
- 43% smaller network payloads
- Zero-copy Arrow deserialization
- Tested with Arc's 2.43M records/sec write performance

### Security
- Encrypted API key storage using Grafana secrets
- Backend-only credential access
- HTTPS support

[Unreleased]: https://github.com/basekick-labs/grafana-arc-datasource/compare/v1.4.0...HEAD
[1.4.0]: https://github.com/basekick-labs/grafana-arc-datasource/compare/v1.3.2...v1.4.0
[1.3.2]: https://github.com/basekick-labs/grafana-arc-datasource/compare/v1.3.1...v1.3.2
[1.3.1]: https://github.com/basekick-labs/grafana-arc-datasource/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/basekick-labs/grafana-arc-datasource/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/basekick-labs/grafana-arc-datasource/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/basekick-labs/grafana-arc-datasource/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/basekick-labs/grafana-arc-datasource/releases/tag/v1.0.0
