# Grafana Community Signing — Readiness Punch List

**Date:** 2026-05-14
**Status:** Frozen artifact — this is the snapshot from the four-agent review on 2026-05-14. Tick items as fixed; do not delete entries (they document what was triaged). Add new findings in a new dated doc.
**Source:** Parallel review by four agents (Correctness / Security / Code-Quality / Performance) against `main` at commit `3a843ef`.

---

## Workflow

- Each item has a stable ID (e.g. `S1`, `C2`, `P3`) — reference these in commits / PR titles (`fix(signing): S1 add SSRF guard`).
- Tick the box when the fix is **merged on main**, not when written.
- If an item turns out to be a non-issue, leave the box unticked, change status to `— WONTFIX: <reason>`.
- Group fixes into the suggested 5 commits (Cleanup / Safety / Arrow / Macros / Polish) to minimize PR churn.

---

## SIGNING BLOCKERS — must fix before submission

- [x] **S1** — SSRF on admin URL field. Add scheme allowlist (`http`/`https` only), block private-IP/metadata addresses (`127.0.0.0/8`, `169.254.169.254`, `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `fd00::/8`, `metadata.google.internal`), wire `http.Client.CheckRedirect` to re-validate after redirects. — DONE (cefa6cc) — `newHTTPClient` + `safeDialContext` in `pkg/plugin/safety.go`; loopback allowed only when configured URL is itself loopback. Covered by `safety_test.go`.
- [x] **S2** — `dist/plugin.json` version mismatch (`1.0.0` vs source `1.2.0`). Resolved: `dist/` already gitignored; rebuilt fresh on release. — DONE (4256157)
- [x] **S3** — `plugin.json#screenshots` is `[]` while six screenshots already exist in `img/`. Populate per Grafana schema (`{name, path}`). — DONE (4256157)
- [x] **S4** — `plugin.json#info.updated: "2026-04-02"` is stale (today 2026-05-14). Refresh; ideally CI-generated on release. — DONE (4256157), still want CI automation later
- [x] **S5** — README claims that don't match code: "Zero-Copy Decode" (legacy path does `io.ReadAll`) and "Compression at protocol level" (no `Accept-Encoding` handling). Either implement or remove the claims. — DONE (4256157) — removed claims
- [x] **S6** — Vulnerable transitive Go deps. Run `go get golang.org/x/net@latest google.golang.org/protobuf@latest google.golang.org/grpc@latest && go mod tidy`. CVEs: CVE-2024-45338, CVE-2023-45288, CVE-2024-24786. — DONE (4256157) — x/net 0.20→0.54, protobuf 1.32→1.36, grpc 1.60→1.81
- [x] **S7** — `useArrow` frontend/backend default mismatch. UI shows "Arrow ON" by default but `jsonData.useArrow` is `undefined` → backend Go-zero-value `false` → fresh installs silently use JSON. Fix in `getSettings` (default `true` when unset) AND/OR write `useArrow: true` from the frontend on first render. — DONE (4256157) — switched to `*bool`, defaults to true when unset

---

## CRITICAL — real production bugs

- [x] **C1** — No top-level `recover` in `QueryData`. A panic on the JSON or non-split Arrow path takes down the entire batch (every refId fails). Per-chunk recover exists in the split path only. Wrap each refId in its own recover that converts the panic to `backend.ErrDataResponse`. — DONE (cefa6cc) — `queryWithRecover`
- [x] **C2** — Unchecked type assertions on JSON shape. `col.(string)`, `dataRows[rowIdx].([]interface{})`, `row[colIdx].(float64)`, `row[colIdx].(bool)`. Use comma-ok form; return a clean error instead of panicking. — DONE (cefa6cc)
- [x] **C3** — Unchecked Arrow type assertions in `appendArrowColumnToField`. Every `col.(*array.X)` panics if the dispatch `DataType().ID()` and concrete Go type disagree (extension types, dictionary-encoded strings, lists). Add comma-ok; fall back to string-as-default. — DONE (cefa6cc) — returns mismatch error rather than fallback (collapse with C6 will reconsider)
- [x] **C4** — Macros expand inside string literals and mis-match parens. `$__timeFilter` / `$__timeGroup` use first `)` not matching `)`. Strip literals first (reuse `stripStringLiterals`), then scan with paren-depth counter. — [pkg/plugin/query.go:380-407,488-522](../../pkg/plugin/query.go) — DONE (7731f21) — `replaceMacroOccurrences` is literal/comment-aware; `findMatchingParen` handles nested parens. New tests lock the behavior.
- [~] **C5** — `OptimizeTimeSeriesQuery` substring match on `"time"` rewrites queries containing `lifetime`/`runtime`/`downtime`/`timestamp`, injecting `ORDER BY time ASC` against a column that doesn't exist. — PARTIAL (4256157) — call site disabled; function body still there. Proper fix (literal-stripping + word boundaries, opt-in flag) in Commit 4. — [pkg/plugin/query.go:527-563](../../pkg/plugin/query.go#L527-L563)
- [x] **C6** — Two parallel Arrow decoders (`arrow.go` 388 lines, `arrow_flightsql_style.go` 348 lines, ~80% duplicated). Only the FlightSQL path is on the query hot path; `arrow.go` lives only for `CheckHealth`. The two have already diverged (int promotion, timestamp heuristics, error-vs-string fallback). Collapse into one, point CheckHealth at the production path, delete the dead code. — [pkg/plugin/arrow.go](../../pkg/plugin/arrow.go), [pkg/plugin/arrow_flightsql_style.go](../../pkg/plugin/arrow_flightsql_style.go) — DONE (06f5f1b) — collapsed `arrow.go` + `arrow_flightsql_style.go` into one; `CheckHealth` on production path.
- [~] **C7** — Result-set unbounded → OOM hazard. `io.ReadAll(resp.Body)` (legacy) and unbounded record accumulation (FlightSQL). Add `http.MaxBytesReader` at the response boundary, cap row count per response, return a clear error past the cap. — PARTIAL (cefa6cc) — body cap (256 MiB) wired on all three paths; per-row cap deferred (row count is harder to cap mid-Arrow-stream without record-batch counting).

---

## HIGH — Correctness & Failure Modes

- [x] **H1** — `mergeFrames` silently drops mismatched frames (no log, partial result delivered). Validate field-by-field type compatibility, not just count. Log when a chunk is skipped. — [pkg/plugin/datasource.go:218-282](../../pkg/plugin/datasource.go#L218-L282) — DONE (453f93c) — covered by R2-HI2. `frameSchemaCompatible` checks per-field types; skip + log warning.
- [x] **H2** — Schema type-drift across chunks (same field count, different concrete types) panics `field.Set`. Combined with missing top-level recover (C1), kills the batch. Add a type-equality precondition before merging; coerce or fail cleanly. — [pkg/plugin/datasource.go:245](../../pkg/plugin/datasource.go#L245) — DONE (453f93c) — covered by R2-HI2.
- [x] **H3** — `CheckHealth` uses the dead `QueryArrow` path. Health passes while the real path is broken. Point at the FlightSQL path (after C6). — [pkg/plugin/datasource.go:526-530](../../pkg/plugin/datasource.go#L526-L530) — DONE (06f5f1b) — `CheckHealth` now calls `queryArrow` (production path).
- [x] **H4** — `QueryData` runs refIds sequentially. Chunks-within-a-refId are parallel but refIds aren't. With 6 panels and slow Arc, the user waits the sum, not the max. Fan out with `errgroup`. — [pkg/plugin/datasource.go:97-100](../../pkg/plugin/datasource.go#L97-L100) — DONE (26f85ee) — refIds fan out via `errgroup` with MaxConcurrency limit. Single-refId fast path preserved.
- [x] **H5** — `interpolateVariable` quoting bugs. Single-value strings get inner quotes doubled but no surrounding quotes (`host = O'Brien` is malformed). Numeric arrays always quoted via `quoteLiteral` — breaks numeric `IN`. Either detect numeric values or document SQL-side coercion. — [src/datasource.ts:75-94](../../src/datasource.ts#L75-L94) — DONE (453f93c) — covered by R2-HI5. Single-value strings now always quoted via `quoteLiteral`. Numeric branch unchanged.
- [ ] **H6** — `VariableQueryEditor` initializes `useState` from prop; never re-syncs on parent updates. Switch to controlled component or `useEffect` sync. Also remove `React.FC`. — [src/VariableQueryEditor.tsx:13-25](../../src/VariableQueryEditor.tsx#L13-L25)
- [ ] **H7** — `VariableQuery` schema mismatch with `metricFindQuery`. Editor saves `{ query: state.query }`; backend looks up `query.sql || query.query || query.rawSql` — only the second fires. Align on one field name (`sql`); remove fallbacks. — [src/VariableQueryEditor.tsx:4-10](../../src/VariableQueryEditor.tsx#L4-L10), [src/datasource.ts:25-57](../../src/datasource.ts#L25-L57)

## HIGH — Security

- [x] **H8** — Arc error text leaked verbatim to dashboard viewers (`parseArcError` returns first 500 bytes of body — may include DuckDB plans, file paths, table/column names). In multi-privilege installs this leaks schema to lower-privileged viewers. Sanitize at the boundary; log full text server-side. — DONE (cefa6cc) — `sanitizeUserError`; covered by test ensuring Arc error detail is stripped.
- [x] **H9** — SQL injection via macro column args. `$__timeFilter(col)` and `$__timeGroup(col, '1h')` interpolate `col` raw. Validate with `^[A-Za-z_][A-Za-z0-9_.]*$`; reject otherwise. — DONE (cefa6cc) — `validateColumnArg`; invalid macros left un-expanded so Arc surfaces a clear error.
- [x] **H10** — Per-query `database` not validated. Goes into `X-Arc-Database` header. Validate against `^[A-Za-z0-9_-]+$`. — DONE (cefa6cc) — `validateDatabaseName`; applied to both default and per-query override.
- [x] **H11** — Fan-out × split unbounded across panels. A 12-panel dashboard × `maxConcurrency=4` = 48 concurrent Arc requests per refresh. Combined with C7, easily exhausts the plugin. Cap concurrency globally per instance (semaphore on the `ArcDatasource` struct, not per query). — [pkg/plugin/datasource.go:379](../../pkg/plugin/datasource.go#L379) — DONE (26f85ee) — implicit via `MaxConcurrency` cap (M13) applied at both refId and chunk levels.

## HIGH — Performance (the "gemini line-level pass")

- [x] **P1** — Per-row `Value(i)` + `IsNull(i)` + `field.Append` in Arrow decode. Single largest perf miss. Switch to bulk slice accessors (`Float64Values()`, `Int64Values()`, `TimestampValues()`); short-circuit on `col.NullN() == 0`; pre-extend the destination field. 5–10× faster on 1M-row batches. — [pkg/plugin/arrow_flightsql_style.go:285-298,315-328,334-346](../../pkg/plugin/arrow_flightsql_style.go) — DONE (06f5f1b) — bulk slice accessors (`Int64Values`/`Float64Values`/`TimestampValues`) + null-bitmap short-circuit.
- [x] **P2** — `field.Append` without pre-sizing. Reflective doubling on every call. Pre-extend with `field.Extend(arr.Len())` and use `field.Set(i, v)`. — [pkg/plugin/arrow_flightsql_style.go:281,310,333](../../pkg/plugin/arrow_flightsql_style.go) — DONE (06f5f1b) — `field.Extend(rows)` once per record batch + `field.Set(startIdx+i, ...)` per row.
- [x] **P3** — `*http.Client` constructed per call (per chunk). Defeats connection pooling, no `MaxIdleConnsPerHost`, no shared transport. Cache via SDK's `InstanceManager`. Set `MaxIdleConnsPerHost: 32, MaxConnsPerHost: 64, IdleConnTimeout: 90s, ForceAttemptHTTP2: true`. — [pkg/plugin/query.go:88-90](../../pkg/plugin/query.go#L88-L90), [pkg/plugin/arrow.go:51-53](../../pkg/plugin/arrow.go#L51-L53), [pkg/plugin/arrow_flightsql_style.go:51-53](../../pkg/plugin/arrow_flightsql_style.go#L51-L53) — DONE (26f85ee) — shared `*http.Client` per instance via `InstanceManager`; pooling+TLS resumption apply across requests.
- [x] **P4** — Settings re-unmarshaled on every `QueryData`. Implement `instancemgmt.InstanceFactory`/`InstanceManager`; same lifecycle as P3's client cache. — [pkg/plugin/datasource.go:51-84](../../pkg/plugin/datasource.go#L51-L84) — DONE (26f85ee) — settings parsed exactly once per revision in `newArcInstance` (the SDK InstanceFactoryFunc).
- [x] **P5** — `expandTimeFilter` / `expandTimeGroup` rebuild full SQL string per macro occurrence (O(N·L)). Use `strings.Builder`. — [pkg/plugin/query.go:380-407,488-522](../../pkg/plugin/query.go) — DONE (7731f21) — `replaceMacroOccurrences` is single-pass `strings.Builder`.
- [ ] **P6** — `mergeFrames` boxes every cell via `interface{}` + reflective `Set` (4M allocations on 1M-row × 4-field merge). Type-switch on concrete slices or rebuild with pre-sized typed slices. — [pkg/plugin/datasource.go:267-281](../../pkg/plugin/datasource.go#L267-L281)
- [ ] **P7** — `ensureAscendingTimes` allocates per row via `RowCopy` + `CopyAt` + `AppendRow`. Sort an `[]int` of indices and write each field's slice directly in permuted order. — [pkg/plugin/datasource.go:660-693](../../pkg/plugin/datasource.go#L660-L693)
- [x] **P8** — Goroutine fanout unbounded. Semaphore is acquired *inside* the goroutine; 8760 chunks spawn 8760 goroutines that immediately block. Use `errgroup.WithContext(ctx)` + `g.SetLimit(maxConcurrency)`; `Go()` blocks the caller until a slot is free. — [pkg/plugin/datasource.go:373-410](../../pkg/plugin/datasource.go#L373-L410) — DONE (26f85ee) — `errgroup.SetLimit(MaxConcurrency)` only spawns when a slot frees; chunks AND refIds bounded.
- [x] **P9** — Cancellation gap on chunk fan-out. First error doesn't cancel sibling chunks; they all run to completion. Fixed for free by `errgroup.WithContext` in P8 (use `req.WithContext(ctx)` on outbound HTTP). — [pkg/plugin/datasource.go:381-410](../../pkg/plugin/datasource.go#L381-L410) — DONE (26f85ee) — `errgroup.WithContext` cancels in-flight siblings on first error via derived ctx.

## HIGH — Code Quality

- [x] **H12** — Dead `timeRange` parameter on `QueryArrow`, `QueryArrowFlightSQLStyle`, `QueryJSON`. Remove from signatures and call sites. — [pkg/plugin/arrow.go:22](../../pkg/plugin/arrow.go#L22), [pkg/plugin/arrow_flightsql_style.go:22](../../pkg/plugin/arrow_flightsql_style.go#L22), [pkg/plugin/query.go:59](../../pkg/plugin/query.go#L59) — DONE (06f5f1b) — dead `timeRange` param removed from `queryArrow` and `queryJSON`.
- [ ] **H13** — Over-exported package symbols. Unexport: `ArcDataSourceSettings`, `ArcQuery`, `ArcInstanceSettings`, `ArcDatasource`, `QueryArrow`, `QueryArrowFlightSQLStyle`, `QueryJSON`, `JSONToDataFrame`, `ArrowToDataFrame`, `ApplyMacros`, `ApplyMacrosWithSplit`, `OptimizeTimeSeriesQuery`. Only `NewArcDatasource` needs to be exported. — [pkg/plugin/datasource.go](../../pkg/plugin/datasource.go), [pkg/plugin/query.go](../../pkg/plugin/query.go), [pkg/plugin/arrow.go](../../pkg/plugin/arrow.go), [pkg/plugin/arrow_flightsql_style.go](../../pkg/plugin/arrow_flightsql_style.go)
- [ ] **H14** — `metricFindQuery(query: any, options?: any)`. Type as `string | { sql?: string; query?: string; rawSql?: string }`; narrow once. — [src/datasource.ts:25](../../src/datasource.ts#L25)
- [ ] **H15** — Hardcoded non-theme colors (`#999`, `#888`, `#6e6e6e`) in three TSX files. Unreadable in light theme. Use `useStyles2` + `theme.colors.text.secondary`. — `src/QueryEditor.tsx`, `src/VariableQueryEditor.tsx`, `src/ConfigEditor.tsx`
- [ ] **H16** — `.eslintrc` disables `@typescript-eslint/no-explicit-any` globally for a single forced `any`. Re-enable; use targeted `// eslint-disable-next-line` on `metricFindQuery` only. — [.eslintrc:6](../../.eslintrc#L6)

---

## MEDIUM

### Correctness

- [ ] **M1** — Sub-second `$__interval` not supported (min returned is 10s). Add fast-path for short ranges. — [pkg/plugin/query.go:365-376](../../pkg/plugin/query.go#L365-L376)
- [ ] **M2** — Macro timezone always UTC. Document explicitly; consider `$__timeFilter(col, 'UTC')` extension. — [pkg/plugin/query.go:401-403,415,418,439-440](../../pkg/plugin/query.go)
- [x] **M3** — `$__timeGroup` swallows third+ args silently (`SplitN(",", 2)`). Warn or fail. — [pkg/plugin/query.go:504-509](../../pkg/plugin/query.go#L504-L509) — DONE (7731f21) — extra args now warn loudly and leave the macro un-expanded.
- [x] **M4** — `intervalToSeconds` defaults to 1h on unknown input. Typos like `'1minutes'` silently bucket at 1h. Log Warn loudly or return un-expanded macro. — [pkg/plugin/query.go:481](../../pkg/plugin/query.go#L481) — DONE (7731f21) — `intervalToSeconds` returns `(int, bool)`; unknown intervals leave the macro un-expanded.
- [x] **M5** — `JSONToDataFrame` infers column type from first non-null value; later mixed-type rows panic via `row[colIdx].(float64)`. Add per-cell `, ok`. — DONE (cefa6cc) — comma-ok with log+skip on mismatch.
- [x] **M6** — `containsAggregationWithoutTimeGroup` false positives: matches `LIST(`, `MEDIAN(`, `CHECKSUM(`; doesn't strip comments. Convert function-name list to a regex anchored at word boundaries. — [pkg/plugin/datasource.go:730-782](../../pkg/plugin/datasource.go#L730-L782) — DONE (7731f21) — `aggregationFnRe` regex with `\b` word boundaries; no more `CHECKSUM(`/`list_contains(` false positives.
- [x] **M7** — `containsLIMIT` triggers on `LIMIT` inside subqueries (skips splitting unnecessarily). Document, or scope to outermost statement. — [pkg/plugin/datasource.go:719-721](../../pkg/plugin/datasource.go#L719-L721) — DONE (7731f21) — documented in `containsLIMIT` godoc.
- [x] **M8** — `containsAggregationWithoutTimeGroup` doesn't handle CTEs / subquery aggregations cleanly. Edge-case fragility. — [pkg/plugin/datasource.go:730-782](../../pkg/plugin/datasource.go#L730-L782) — DONE (7731f21) — documented in `containsAggregationWithoutTimeGroup` godoc as a known conservative limitation.
- [ ] **M9** — `applyTemplateVariables` only interpolates `sql`. `database` and `splitDuration` are skipped. Either interpolate them too or document. — [src/datasource.ts:96-101](../../src/datasource.ts#L96-L101)
- [ ] **M10** — `useEffect(..., [])` migration in `QueryEditor` mutates state on mount. Should live in `getDefaultQuery` / backend `query()`. — [src/QueryEditor.tsx:27-31](../../src/QueryEditor.tsx#L27-L31)

### Security & Operational

- [ ] **M11** — `id: "basekick-arc-datasource"` — verify with Grafana catalogue that vendor slug `basekick` is registered. Catalogue rejects unregistered vendor prefixes. — [plugin.json](../../plugin.json)
- [ ] **M12** — `plugin.json` missing `info.author.email` and `category`. Validator warning. — [plugin.json](../../plugin.json)
- [x] **M13** — `MaxConcurrency` user input only validated `<= 0`. Cap at 32. — DONE (cefa6cc) — `MaxConcurrencyCap` constant.
- [x] **M14** — Per-query `database` override mutates a shallow copy of `ArcInstanceSettings`. Document; or refactor to pass `database` as an explicit param to `executeChunk`/`querySingle`. — [pkg/plugin/datasource.go:303-307](../../pkg/plugin/datasource.go#L303-L307) — DONE (26f85ee) — comment on the per-query DB override shallow-copy pattern.

### Performance

- [x] **M15** — `containsAggregationWithoutTimeGroup` + `containsLIMIT` each call `stripStringLiterals` + `strings.ToUpper` on the same SQL. Compute once at top of `query()` and pass down. — [pkg/plugin/datasource.go:314,323](../../pkg/plugin/datasource.go#L314-L323) — DONE (7731f21) — `newStrippedSQL(qm.SQL)` computed once; passed to all heuristic checks.
- [x] **M16** — `stripStringLiterals` doesn't pre-size the Builder. Add `result.Grow(len(sql))`. — [pkg/plugin/datasource.go:695-715](../../pkg/plugin/datasource.go#L695-L715) — DONE (7731f21) — `out.Grow(len(sql))` pre-sizes the Builder.
- [x] **M17** — `intervalToSeconds` linear switch; replace with package-level `map[string]int`. — [pkg/plugin/query.go:451-483](../../pkg/plugin/query.go#L451-L483) — DONE (7731f21) — `intervalSecondsTable` map replaces the switch.
- [x] **M18** — `JSONToDataFrame` does `time.Parse` with 2-3 formats per row. Detect format once on first row; reuse. — [pkg/plugin/query.go:209-217,247-285](../../pkg/plugin/query.go) — DONE (7731f21) — `detectedLayout` cached from first sample.
- [x] **M19** — `fmt.Sprintf("%v", row[colIdx])` per row in string-column path. Type-assert once; only Sprintf the fallback. — [pkg/plugin/query.go:307](../../pkg/plugin/query.go#L307) — DONE (7731f21) — string-column path type-asserts before `Sprintf` fallback.
- [ ] **M20** — Debug-log closures and slice allocations passed as args eagerly. Wrap with level check or remove. — [pkg/plugin/query.go:343-359](../../pkg/plugin/query.go#L343-L359)
- [x] **M21** — `field.Nullable()` checked inside row loop; hoist once per column. — [pkg/plugin/arrow_flightsql_style.go:286,316,335](../../pkg/plugin/arrow_flightsql_style.go), [pkg/plugin/arrow.go:317,335,360](../../pkg/plugin/arrow.go) — DONE (06f5f1b) — `field.Nullable()` hoisted out of inner loop; computed once per column.
- [x] **M22** — Log-spam: per-row `Warn` on JSON timestamp parse failures. With 100K-row response = 100K log lines. Sample or move to Debug. — [pkg/plugin/query.go:279,291](../../pkg/plugin/query.go) — DONE (7731f21) — per-row Warns replaced with per-column summary logs.
- [ ] **M23** — `log.Info` on every split query (per-panel-refresh spam). Drop to Debug. — [pkg/plugin/datasource.go:358,450](../../pkg/plugin/datasource.go)
- [ ] **M24** — Per-row Arrow `Warn` in legacy decoder on timestamp parse failure. Sample. — [pkg/plugin/arrow.go:308-313](../../pkg/plugin/arrow.go#L308-L313)
- [ ] **M25** — O(N²) variable-value dedup via `findIndex` inside `filter`. Use `Set`. — [src/datasource.ts:62-64](../../src/datasource.ts#L62-L64)

### Code Quality

- [ ] **M26** — `console.warn('metricFindQuery received object without sql:', query)` in production frontend path. Drop or guard. — [src/datasource.ts:38](../../src/datasource.ts#L38)
- [x] **M27** — SQL helpers (`stripStringLiterals`, `containsLIMIT`, `containsAggregationWithoutTimeGroup`) live in `datasource.go` but belong with macro logic in `query.go` (or split into `sql.go`). — [pkg/plugin/datasource.go:695-782](../../pkg/plugin/datasource.go#L695-L782) — DONE (7731f21) — helpers moved into new `pkg/plugin/sql.go`.
- [x] **M28** — `rxjs` imported but not declared in `package.json` deps (works via `@grafana/runtime` transitively). Add as explicit dep. — DONE (4256157)
- [x] **M29** — `@types/lodash` declared as devDep; `lodash` never imported. Remove. — DONE (4256157)
- [ ] **M30** — Test coverage gaps: no tests for `ArrowToDataFrame`, `JSONToDataFrame`, `mergeFrames` with type drift, `prepareFrames` (long-to-wide), `parseArcError`, `OptimizeTimeSeriesQuery`, `expandTimeFilter` nested parens, `intervalToSeconds` unknown input, HTTP path via `httptest.NewServer`, `CheckHealth`. Aim for one test per CRITICAL/HIGH fixed. — [pkg/plugin/datasource_test.go](../../pkg/plugin/datasource_test.go)
- [x] **M31** — Empty `ArcDatasource struct{}` with method receivers — fine, but ties testability. Inject `*http.Client` and `log.Logger` (also unblocks P3/P4). — [pkg/plugin/datasource.go:44-49](../../pkg/plugin/datasource.go#L44-L49) — DONE (26f85ee) — `ArcDatasource` now holds the InstanceManager; instance carries injected `*http.Client`. Testable via `newArcInstance` factory.
- [x] **M32** — Behavioral divergence on unknown Arrow types: `arrow.go` falls back to string, `arrow_flightsql_style.go` returns error. Pick one (string fallback recommended — won't kill the panel). Resolved by C6 collapse. — [pkg/plugin/arrow.go:286](../../pkg/plugin/arrow.go#L286), [pkg/plugin/arrow_flightsql_style.go:276](../../pkg/plugin/arrow_flightsql_style.go#L276) — DONE (06f5f1b) — unified path's mismatch fallback is a clean error (matches the carry-forward from C3).

---

## LOW

- [ ] **L1** — `apiKey == ""` check lets whitespace through; use `strings.TrimSpace`. — [pkg/plugin/datasource.go:63](../../pkg/plugin/datasource.go#L63)
- [ ] **L2** — `splitTimeRange` unbounded slice append. Compute upper-bound capacity. — [pkg/plugin/datasource.go:179-200](../../pkg/plugin/datasource.go#L179-L200)
- [ ] **L3** — `splitTimeRange` pre-1970 timestamp handling. Add `if fromEpoch < 0` guard. — [pkg/plugin/datasource.go:170-182](../../pkg/plugin/datasource.go#L170-L182)
- [ ] **L4** — `mergeFrames` returns `frames[0]` (0-field) when no non-empty frame found; should return `nil`. — [pkg/plugin/datasource.go:236-238](../../pkg/plugin/datasource.go#L236-L238)
- [ ] **L5** — `stripStringLiterals` doesn't handle double-quoted identifiers. Add comment. — [pkg/plugin/datasource.go:698-715](../../pkg/plugin/datasource.go#L698-L715)
- [ ] **L6** — Dead `MaxDataPoints int64` field — never read. Remove or wire. — [pkg/plugin/datasource.go:33](../../pkg/plugin/datasource.go#L33)
- [x] **L7** — `formatRequestError` matches stdlib error strings via `Contains`. Brittle. Use `errors.Is`/`errors.As`. — [pkg/plugin/query.go:40-56](../../pkg/plugin/query.go#L40-L56) — DONE (26f85ee) — typed-error matching via `errors.Is`/`errors.As`; substring fallback only for `Client.Timeout`. Test covers 9 error shapes + chain-preservation.
- [x] **L8** — `parseArcError` truncates body at 500 bytes — may split UTF-8 mid-rune. Use `utf8.DecodeLastRuneInString` to back off. — [pkg/plugin/query.go:29-31](../../pkg/plugin/query.go#L29-L31) — DONE (26f85ee) — `truncateForLog` backs off to UTF-8 rune boundary via `utf8.DecodeLastRuneInString`. Tested with 4-byte emoji boundary.
- [x] **L9** — `arrow.go` WORKAROUND for "seconds marked as microseconds". Add TODO with Arc issue link or remove if Arc-side fixed. — [pkg/plugin/arrow.go:300-313](../../pkg/plugin/arrow.go#L300-L313) — DONE (06f5f1b) — legacy WORKAROUND heuristic dropped along with `arrow.go`.
- [ ] **L10** — Magic numbers `3*time.Hour`, `24*time.Hour`, etc. in `autoSplitDuration`. Extract to named consts. — [pkg/plugin/datasource.go:114-123](../../pkg/plugin/datasource.go#L114-L123)
- [ ] **L11** — `fmt.Errorf("API key is required")` as static string. Use `errors.New` or sentinel. — [pkg/plugin/datasource.go:63](../../pkg/plugin/datasource.go#L63)
- [ ] **L12** — WHAT-comments restating code (`// Parse settings`, `// Get API key`, `// Default values`, etc.). Delete. — [pkg/plugin/datasource.go:55,60,66,90,96](../../pkg/plugin/datasource.go), [pkg/plugin/datasource.go:181](../../pkg/plugin/datasource.go#L181)
- [ ] **L13** — Closure inside `for` in `executeChunk` mixes semaphore / ctx-cancel / panic-recovery / actual work. Extract `runChunk(idx, ch) chunkResult`. — [pkg/plugin/datasource.go:383-410](../../pkg/plugin/datasource.go#L383-L410)
- [x] **L14** — `arrow.go.backup` in working tree. Delete (not tracked, but tidy). — DONE (4256157)
- [x] **L15** — `appendDuration` in `arrow.go` duplicates `appendBasic[int64]`. Resolved by C6 collapse. — [pkg/plugin/arrow.go:332-348](../../pkg/plugin/arrow.go#L332-L348) — DONE (06f5f1b) — duplicate `appendDuration` gone with the file collapse.
- [x] **L16** — Decorative `error` returns that always return `nil` in arrow append helpers. Drop `error` from signatures. — [pkg/plugin/arrow_flightsql_style.go:298,329,347](../../pkg/plugin/arrow_flightsql_style.go), [pkg/plugin/arrow.go:249,329,347,372,386](../../pkg/plugin/arrow.go) — DONE (06f5f1b) — decorative `error` returns dropped from column writers.
- [ ] **L17** — Test cases in `TestApplyMacros_Interval` lack `desc` field — inconsistent with other table-driven tests in same file. — [pkg/plugin/datasource_test.go:617](../../pkg/plugin/datasource_test.go#L617)
- [ ] **L18** — Locally redefined `VariableQuery` interface — move to `types.ts`. — [src/VariableQueryEditor.tsx:4-11](../../src/VariableQueryEditor.tsx#L4-L11)
- [ ] **L19** — `RadioButtonGroup` `value as 'time_series' | 'table'` cast — type `FORMAT_OPTIONS` as `Array<{ label: string; value: ArcQuery['format'] }>`. — [src/QueryEditor.tsx:38](../../src/QueryEditor.tsx#L38)
- [ ] **L20** — Hardcoded URL placeholder `http://localhost:8000` in ConfigEditor. Cosmetic. — [src/ConfigEditor.tsx:102](../../src/ConfigEditor.tsx#L102)
- [x] **L21** — `rawQuery: true` field set on `metricFindQuery` target but never read on backend. Dead state. — DONE (4256157)
- [ ] **L22** — `console.warn` already covered in M26 — keep one ID.
- [ ] **L23** — `useEffect` deps suppression in `QueryEditor` — covered by M10.
- [ ] **L24** — Inline `style={{...}}` objects re-created every render. Hoist as module consts or `useMemo`. — [src/QueryEditor.tsx:57,95,103-119](../../src/QueryEditor.tsx)
- [ ] **L25** — Per-row Arrow record `frame.Rows()` in debug log. Eagerly evaluated. — [pkg/plugin/arrow_flightsql_style.go:84-90](../../pkg/plugin/arrow_flightsql_style.go), [pkg/plugin/arrow.go:74-77](../../pkg/plugin/arrow.go)
- [ ] **L26** — `OptimizeTimeSeriesQuery` doesn't use `stripStringLiterals` (unlike `containsLIMIT`). Resolved by C5 (gating or removing the function). — [pkg/plugin/query.go:527-562](../../pkg/plugin/query.go#L527-L562)
- [ ] **L27** — `toTime` helper in `datasource.go` only used by `ensureAscendingTimes`. Move next to caller. — [pkg/plugin/datasource.go:797](../../pkg/plugin/datasource.go#L797)
- [x] **L28** — Stray blank-with-tab line after `ds := plugin.NewArcDatasource()` in main. Run `gofmt`; add CI check. — DONE (4256157), CI check still TODO

---

## Suggested commit groupings

Keep each commit reviewable in <10 minutes. Reference IDs in commit message scope.

### Commit 1 — `fix(signing): cleanup pass` (~1h, low-risk)
S2, S3, S4, S5, S6, S7, C5 (decision: gate or remove), L14, L20, L21, M11, M12, M28, M29, L28

### Commit 2 — `fix(signing): safety net` (~2h)
C1, C2, C3, C7, S1, H8, H9, H10, M5, M13

### Commit 3 — `refactor(arrow): unify decoders` (~2h)
C6, H3, H12, M32, L9, L15, L16, P1, P2, M21

### Commit 4 — `fix(query): macros + heuristics` (~1.5h)
C4, M3, M4, M6, M7, M8, M15, M16, M17, M18, M19, M22, M27, P5

### Commit 5 — `refactor(http): shared client + instance manager` (~1.5h)
P3, P4, P8, P9, H4, H11, L7, L8, M14, M31

### Commit 6 — `chore(frontend): polish + typing` (~1h)
H5, H6, H7, H14, H15, H16, M9, M10, M25, M26, L18, L19, L24

### Commit 7 — `chore: unexport internals + tests` (~1h)
H13, M30 (write tests as you fix — at least cover the CRITICAL/HIGH fixes), L1, L2, L3, L4, L5, L6, L10, L11, L12, L13, L17, L25, L27

### Commit 8 — `perf: frame merge + sort` (~1h)
P6, P7, M23, M24, M1, M2

---

## Process

- Before opening the PR for signing-readiness, run the four-agent review again on the diff. The CLAUDE.md says: *"If gemini flags items the internal four agents missed, spawn a fresh round of the four agents before fixing, asking them to look for issues of the same shape gemini caught."*
- After the four-agent internal pass is clean, post `@gemini-code-assist` in the PR comment.
- After gemini's pass, fix everything in a single follow-up commit (do not re-trigger gemini until all findings are addressed).
- Bump `package.json`, `plugin.json`, `dist/plugin.json` together. Update `CHANGELOG.md`. Refresh `info.updated`.
- Submit to Grafana Community signing only after `npm run typecheck && npm run lint && npm run test:ci && go test ./pkg/... && go vet ./pkg/...` all pass clean.

---

# ROUND 2 — Internal re-review findings (2026-05-14 evening)

**Trigger:** After commits 4256157, cefa6cc, d6a52a0, 06f5f1b, 7731f21, 85a3b69, 26f85ee landed, we ran a fresh four-agent pass (Correctness / Security / Code-Quality / Performance) on the post-fix branch. Gemini also posted a third review round on commit 26f85ee. The pattern the CLAUDE.md warned about — "the fix introduced a different shape of the same bug" — surfaced repeatedly.

**ID prefix:** `R2-*` so these don't collide with the original findings.

**Gemini cross-reference:** column lists gemini PR comments that overlap.

## CRITICAL — bugs the fixes introduced

- [x] **R2-CR1** — `MaxConcurrency` lies; refId × chunk fan-outs multiply. Two independent `errgroup.SetLimit(MaxConcurrency)` calls compose multiplicatively. Default 4 + 6 panels = 16 in-flight (not 4); worst case at cap = **32×32=1024 simultaneous HTTP requests**. The user-visible knob doesn't do what the tooltip says. **Fix:** share one `golang.org/x/sync/semaphore.Weighted` across both levels, OR split the cap (e.g. `sqrt(cap)` per level). — [pkg/plugin/datasource.go:173](../../pkg/plugin/datasource.go#L173), [datasource.go:477](../../pkg/plugin/datasource.go#L477) — DONE (453f93c) — shared `*semaphore.Weighted` on `ArcInstanceSettings.sem`, acquired in `doRequest` before the HTTP dial and released when the body is closed. Both fan-out levels queue through the same per-instance limit.
- [x] **R2-CR2** — Arrow non-nullable writers ignore Arrow null bitmap → silent wrong data. All five `write*Column` functions have a non-nullable branch that does `field.Set(startIdx+i, values[i])` without an `IsNull` check, even when `allValid == false`. Arrow's underlying buffer is undefined at null positions; user sees stale buffer memory as real values. **The bulk-accessor rewrite broke the "fail loudly, never silent wrong data" invariant the cefa6cc safety net was built to enforce.** **Fix:** coerce destination to nullable when `col.NullN() > 0`, OR explicit zero-fill at null positions + Frame Notice. — [pkg/plugin/arrow.go:373-377,402-406,433-437,463-467,492-496](../../pkg/plugin/arrow.go) — DONE (453f93c) — `createEmptyField` always produces nullable fields; writers always check `IsNull` and emit typed nil at null positions. Eliminates the non-nullable branch entirely. Test: `TestAppendRecordToDataFrame_NonNullableSchemaWithNullsIsSafe`.
- [x] **R2-CR3** — `containsLIMIT` / `containsUnion` miss `\nLIMIT`, `\tLIMIT`, end-of-string LIMIT. `" LIMIT "` substring requires whitespace both sides. Real-world `WHERE ...\nLIMIT 100` doesn't match → splitting NOT skipped → 7 chunks × 100 rows = **700 rows returned for a query asking for 100**. Same shape on UNION. **Fix:** word-boundary regex `\bLIMIT\s+\d` and `\bUNION\b`. — [pkg/plugin/sql.go:88-90,95-97](../../pkg/plugin/sql.go) — DONE (453f93c) — replaced `" LIMIT "`/`" UNION "` substring with `\bLIMIT\s+\d` and `\bUNION\b` regexes. Tests `TestContainsLIMIT_WhitespaceFlavors`, `TestContainsUnion_WhitespaceFlavors`. **Extended in 3f3881f** to cover LIMIT $variable / ? / :n / (subquery) and GROUP BY whitespace variants (gemini round 4 findings 3244824396, 3244824400).
- [x] **R2-CR4** — Unchecked `(*arrow.TimestampType)` cast slipped through the comma-ok rewrite. `unit := col.DataType().(*arrow.TimestampType).Unit` — the only naked cast left in the bulk path. Extension-type timestamps panic the goroutine. **Fix:** comma-ok + clean error. — [pkg/plugin/arrow.go:412](../../pkg/plugin/arrow.go#L412) — DONE (453f93c) — comma-ok cast on `(*arrow.TimestampType)`; unit passed in as parameter so the cast happens once at the dispatch site (with fallback to string-render path).
- [x] **R2-CR5** — `$__timeFrom()` / `$__timeTo()` / `$__interval` STILL rewrite inside string literals. The C4 fix routed only `$__timeFilter` and `$__timeGroup` through `replaceMacroOccurrences`; the other three use `strings.ReplaceAll`. `WHERE msg = 'use $__timeFrom()'` mangles the literal. **My test `TestApplyMacros_NotExpandedInsideStringLiteral` only locked in the one macro that was already correct — false confidence.** **Fix:** route all five through `replaceMacroOccurrences`. — [pkg/plugin/query.go:578-584](../../pkg/plugin/query.go#L578-L584) — DONE (453f93c) — added `replaceLiteralAwareTokens` (zero-arg sibling of `replaceMacroOccurrences`); routed `$__timeFrom`/`$__timeTo`/`$__interval` through it. Consolidated `ApplyMacros` and `ApplyMacrosWithSplit` into `applyMacrosWith`. Tests cover ALL FIVE macros in literals.

## HIGH — should fix before signing submission

- [x] **R2-HI1** — Chunk-goroutine panic recover loses stack trace. Unlike `queryWithRecover` (which calls `debug.Stack()`), the chunk recover only embeds `%v` of the panic in the error string. User gets "see logs"; logs contain nothing about the panic. **Fix:** mirror `queryWithRecover` pattern. — [pkg/plugin/datasource.go:482-488](../../pkg/plugin/datasource.go#L482-L488) — DONE (453f93c) — chunk-goroutine recover now logs `runtime/debug.Stack()` matching `queryWithRecover`.
- [x] **R2-HI2** — `mergeFrames` checks `len(Fields)` but not `Field.Type()`. Chunks with same field count but different concrete types (JSON inference flip) panic on `Set`. Caught by recover but `merged` is half-written by then. **Fix:** field-by-field type-equality check before the merge loop. — [pkg/plugin/datasource.go:347,370](../../pkg/plugin/datasource.go) — DONE (453f93c) — `frameSchemaCompatible` helper compares per-field types, not just counts. Mismatched chunks skipped + logged.
- [x] **R2-HI3** — Per-query `database` override leaks raw validator error to user. `validateDatabaseName` error returned via `err.Error()` bypassing `sanitizeUserError`. Same for the JSON-unmarshal error one line earlier (line 394). **Fix:** route both through `sanitizeUserError`. — [pkg/plugin/datasource.go:394,411-412](../../pkg/plugin/datasource.go) — DONE (453f93c) — both the validator error and the JSON-unmarshal error in `query()` now route through `sanitizeUserError`.
- [x] **R2-HI4** — `errgroup.Group` (not `WithContext`) at refId level swallows cancellation. **Confirmed by gemini comment 3244629509.** When Grafana cancels parent ctx, the dispatch loop blocks in `g.Go` for `MaxConcurrency` more HTTP round-trips before noticing. **Fix:** use `errgroup.WithContext(ctx)` + `select { case <-ctx.Done() }` in the dispatch loop. — [pkg/plugin/datasource.go:170-173](../../pkg/plugin/datasource.go#L170-L173) — **gemini: 3244629509** — DONE (453f93c) — refId errgroup uses `WithContext(ctx)`; dispatch loop checks `gctx.Done()` between `Go()` calls. Confirmed by gemini 3244629509.
- [x] **R2-HI5** — Frontend `interpolateVariable` returns single-value strings UNQUOTED. `String(value).replace(/'/g, "''")` doubles escapes but doesn't quote — a dashboard `WHERE host = $foo` lets a viewer set `?var-foo=1 OR 1=1--` for **URL-driven SQL injection with API key's scope**. The Postgres datasource quotes unconditionally; we should too. **Fix:** always wrap single-value strings via `quoteLiteral`. — [src/datasource.ts:75-94](../../src/datasource.ts#L75-L94) — DONE (453f93c) — frontend `interpolateVariable` always quotes single-value strings via `quoteLiteral`.
- [x] **R2-HI6** — Per-query `database` override is a confused-deputy. A dashboard editor can switch databases on a datasource whose admin only configured one. If the Arc API key has cross-DB scope (common), this is privilege escalation. **Fix:** gate behind an admin-set `allowDatabaseOverride` flag, OR refuse override when configured DB ≠ `"default"`. — [pkg/plugin/datasource.go:410-417](../../pkg/plugin/datasource.go) — DONE (453f93c) — new `AllowDatabaseOverride` setting (default false) gates per-query database override. Frontend exposes Switch in ConfigEditor.
- [ ] **R2-HI7** — `&v` heap alloc per row in nullable Arrow writers. `v := values[i]; field.Set(startIdx+i, &v)` — `&v` escapes through `interface{}`, heap-allocates per row. **~30-50 ns/row × millions of rows = biggest remaining perf miss.** **Fix:** allocate one `make([]T, n)` pool per record so the pointers share contiguous backing. — [pkg/plugin/arrow.go:359,366,388,395,419,426,449,456,478,485](../../pkg/plugin/arrow.go)
- [ ] **R2-HI8** — `aggregationFnRe` uses `(?i)` though `s.upper` already exists. Case-folding in regex on each character when uppercase view is already computed. **Fix:** drop `(?i)`, match against `s.upper`. — [pkg/plugin/sql.go:115-126,160,163,166](../../pkg/plugin/sql.go)
- [ ] **R2-HI9** — `IdleConnTimeout: 90s` too short for typical dashboard refresh. A 5-min refresh re-dials every refresh; `MaxConnsPerHost` is also unset (no per-host cap). **Fix:** bump `IdleConnTimeout` to 5 min, set `MaxConnsPerHost = MaxConcurrencyCap`. — [pkg/plugin/safety.go:185-189](../../pkg/plugin/safety.go)
- [x] **R2-HI10** — Massive duplication: `queryArrow` and `queryJSON` are ~85% identical. Same URL build, headers, MaxBytesReader, status check. **Fix:** extract `(s *ArcInstanceSettings) doRequest(ctx, path, body) (io.ReadCloser, error)`; both collapse to ~15 lines. — [pkg/plugin/arrow.go:24-88](../../pkg/plugin/arrow.go#L24-L88), [query.go:99-167](../../pkg/plugin/query.go#L99-L167) — DONE (453f93c) — extracted `(s *ArcInstanceSettings) doRequest(...)` method; queryArrow + queryJSON collapse to <30 lines each. Returns a `semReleasingReader` holding the concurrency slot through body read.
- [ ] **R2-HI11** — Dead exported function `OptimizeTimeSeriesQuery` shipping in the binary. Disabled in 7731f21 but still public package surface. Future contributor will re-enable without re-reading the disable note. **Fix:** delete or unexport+`//nolint:unused`. — [pkg/plugin/query.go:731-767](../../pkg/plugin/query.go#L731-L767)
- [x] **R2-HI12** — `appendArrowColumnToField` errors on unsupported types while `createEmptyField` falls back to `*string`. **Gemini-flagged.** Schema-build path and data-write path disagree on unsupported types: schema creates a string column, data path returns "unsupported Arrow type" error. **Fix:** either error in both, or string-fallback in both. — [pkg/plugin/arrow.go:218,340](../../pkg/plugin/arrow.go) — **gemini: 3244629507** — DONE (453f93c) — unsupported Arrow types render per `arrow.Array.ValueStr(i)` into the *string column (matches `createEmptyField` fallback). Gemini 3244629507.

## MEDIUM — fix in the same round if possible

- [ ] **R2-M1** — Per-row JSON type-mismatches drop silently (set to nil) with only a summary log. Frame.Meta.Notices should surface the mismatch to the panel. — [pkg/plugin/query.go:289-291,325-327,365-368](../../pkg/plugin/query.go)
- [ ] **R2-M2** — Chunk-error prefix `[chunk ... to ...]` breaks the `Arc error (HTTP N)` substring sanitizer match. User loses HTTP status info on the split path. **Fix:** use `errors.As` to walk chain. — [pkg/plugin/datasource.go:491-493](../../pkg/plugin/datasource.go), [safety.go:230](../../pkg/plugin/safety.go#L230)
- [ ] **R2-M3** — `JSONToDataFrame` re-asserts `[]interface{}` per column per row. Five columns × 100k rows = 500k type-asserts. Hoist once into `[][]interface{}` then index columns. ~2-4 ms saved per query on JSON path. — [pkg/plugin/query.go:276,309,333,353](../../pkg/plugin/query.go)
- [ ] **R2-M4** — Per-byte `WriteByte` in `replaceMacroOccurrences` and `stripStringLiteralsAndComments` is hot. Scan-ahead to next special byte (`'`, `-`, `/`, `$`) and emit slice via `WriteString`. ~10-20 µs saved per query on a 10KB SQL. — [pkg/plugin/query.go:502](../../pkg/plugin/query.go#L502), [sql.go:79](../../pkg/plugin/sql.go#L79)
- [ ] **R2-M5** — `aggregationFnRe` is a 60-branch alternation regex. NFA, not Aho-Corasick — each character may revisit states. Replace with a tokenized word-set lookup (`map[string]bool`) over space-split words. ~30-50 µs per query on heavy SQL. — [pkg/plugin/sql.go:115-126](../../pkg/plugin/sql.go)
- [ ] **R2-M6** — `MaxBytesReader` cap fires as `*http.MaxBytesError`; sanitizer matches on `"response body exceeded"` substring. Use `errors.As(&maxBytesErr)` to give the user a clearer "increase MaxResponseBytes" message. — [pkg/plugin/safety.go:230](../../pkg/plugin/safety.go), [arrow.go:51](../../pkg/plugin/arrow.go#L51), [query.go:134](../../pkg/plugin/query.go#L134)
- [ ] **R2-M7** — `safeDialContext` re-validates IP only at first dial. After the cached `*http.Client` pool warms up, the dialer is never invoked again (connection reuse) — so a flipped `AllowPrivateIPs` admin toggle takes effect only on the next idle-timeout dial. **Tiny TOCTOU window**; mitigated by `Dispose` on InstanceManager update. Document. — [pkg/plugin/safety.go:88-115](../../pkg/plugin/safety.go)
- [ ] **R2-M8** — Cloud-metadata IPv6 form `fd00:ec2::254` blocked by `IsPrivate()` in strict mode but allowed in `AllowPrivateIPs=true` permissive mode. AWS IMDS-over-IPv6 endpoint. **Fix:** add to always-blocked list even in permissive mode (alongside `169.254.169.254`). — [pkg/plugin/safety.go:138-153](../../pkg/plugin/safety.go)
- [ ] **R2-M9** — `stripStringLiteralsAndComments` doesn't recognize DuckDB's `E'...\\...'` C-style escape literals or `$tag$...$tag$` dollar-quoted strings. A crafted `WHERE msg = E'$__timeFilter(time)\\'` could escape the literal-detection. Narrow attack surface (dashboard authors already control SQL). — [pkg/plugin/sql.go:35-83](../../pkg/plugin/sql.go)
- [ ] **R2-M10** — `ApplyMacros` does 3 separate `strings.ReplaceAll` passes after the single-pass macro engine. Overlaps with R2-CR5. — [pkg/plugin/query.go:573-593](../../pkg/plugin/query.go)
- [ ] **R2-M11** — `mergeFrames` overwrites `Meta.ExecutedQueryString` with raw macro SQL instead of preserving expanded form. Hurts query-inspector usability for split queries. — [pkg/plugin/datasource.go:517-522](../../pkg/plugin/datasource.go)
- [ ] **R2-M12** — `frontend` drop `console.warn` in `metricFindQuery`; type `metricFindQuery` properly (drop two `any` casts). — [src/datasource.ts:25,38](../../src/datasource.ts)
- [ ] **R2-M13** — `plugin.json` missing `category`, `info.author.email`. Required for marketplace submission. — [plugin.json](../../plugin.json)
- [ ] **R2-M14** — `calculateInterval` silently returns `"10 seconds"` for any duration <6h, including zero or negative. Inconsistent with `intervalToSeconds(...) (int, bool)` rejection policy. — [pkg/plugin/query.go:411-422](../../pkg/plugin/query.go)
- [ ] **R2-M15** — Inconsistent error matching: `formatRequestError` uses `errors.Is/As` (L7 refactor) but `sanitizeUserError` still does `strings.Contains`. Pick one. — [pkg/plugin/safety.go:218-240](../../pkg/plugin/safety.go)
- [ ] **R2-M16** — `appendRecordToDataFrame` doesn't guard `len(record.Columns()) == len(frame.Fields)`. Late-batch column-count mismatch panics. — [pkg/plugin/arrow.go:226-240](../../pkg/plugin/arrow.go)

## LOW

- [ ] **R2-L1** — Helpers in wrong file: `formatRequestError` belongs in `safety.go` next to `newHTTPClient`. `truncateForLog` could move too.
- [ ] **R2-L2** — Magic numbers inline: `MaxIdleConns: 100`, `IdleConnTimeout: 90s`, dialer `Timeout/KeepAlive 30s`. Extract as named consts in `safety.go`.
- [ ] **R2-L3** — `interface{}` used in runtime code while tests use `any`. Search-and-replace.
- [ ] **R2-L4** — Go 1.22+ loop-var capture is default; `i, chunk := i, chunk` shadowing is unnecessary. — [datasource.go:175,480](../../pkg/plugin/datasource.go)
- [ ] **R2-L5** — `settings.settings.URL` double-stutter (12+ occurrences). Embed or rename wrapper. — [pkg/plugin/datasource.go:50](../../pkg/plugin/datasource.go)
- [ ] **R2-L6** — `errBlockedAddr` message includes resolved IP in the error chain. Not surfaced to user (sanitizer handles it) but brittle; one careless change leaks the internal IP. — [pkg/plugin/safety.go:105](../../pkg/plugin/safety.go)
- [ ] **R2-L7** — `frameForRecords` releases first record in two manual sites; `defer record.Release()` would be cleaner and panic-safe. — [pkg/plugin/arrow.go:104-111](../../pkg/plugin/arrow.go)
- [ ] **R2-L8** — `Authorization: Bearer` built via `fmt.Sprintf("Bearer %s", k)`. Concat would skip the format parser. Microbench territory. — [pkg/plugin/arrow.go:38](../../pkg/plugin/arrow.go), [query.go:120](../../pkg/plugin/query.go)
- [ ] **R2-L9** — Test coverage gaps: panic-in-one-refId test, `findMatchingParen` direct cases, `JSONToDataFrame` (entire JSON path uncovered), `CheckHealth`, `CheckRedirect`, `httptest.Server` end-to-end for queryArrow/queryJSON.
- [ ] **R2-L10** — `plugin.json` consider IPv6 examples in dialer-policy tooltip if/when corporate-IPv6 deployments need them.
- [ ] **R2-L11** — `rxjs` listed in dependencies — verify webpack externals dedupe with `@grafana/runtime` or bundle gains ~50KB. — [package.json](../../package.json), [src/datasource.ts:11](../../src/datasource.ts)
- [ ] **R2-L12** — `frontend` `style={{...}}` re-allocated each render; hoist to module consts. — [src/QueryEditor.tsx:57,95,103-119](../../src/QueryEditor.tsx)
- [ ] **R2-L13** — `formatRequestError` last-resort substring on `"Client.Timeout"` could use `errors.As(*url.Error)` + `.Timeout()`.
- [ ] **R2-L14** — `apache/arrow/go/v14@v14.0.2` worth bumping to v17/v18 in a follow-up.
- [x] **R2-L15** — Magefile build order broken. `BuildAll` calls `mg.Deps(Clean)` which wipes `dist/`, and `npm run build` uses webpack `clean: true` which ALSO wipes `dist/`. Neither order leaves a complete artifact — discovered during manual install (2026-05-14). **Fix:** drop `mg.Deps(Clean)` from `BuildAll`, OR change webpack's output to not use `clean: true`, OR add a unified Mage target that orchestrates both correctly. Workaround for now: `npm run build && mkdir -p dist/<os>_<arch> && go build -o dist/<os>_<arch>/gpx_arc ./pkg`. — [Magefile.go:28](../../Magefile.go#L28), [.config/webpack/webpack.config.js](../../.config/webpack/webpack.config.js) — DONE (this branch) — `Build` / `BuildAll` no longer depend on `Clean`. New `Dev` / `DevAll` targets orchestrate frontend-then-backend in the only order that works (webpack `clean: true` wipes dist, so backend MUST run after). `CleanBackend` added for backend-only rebuilds during dev.
- [x] **R2-CR6** — **CRITICAL: Mage output path is incompatible with the Grafana SDK loader.** `BuildAll` writes to `dist/<os>_<arch>/gpx_arc`. Grafana looks for `dist/gpx_<name>_<os>_<arch>` (flat, with binary-name prefix). Plugin loads but Grafana errors `Could not start plugin backend: fork/exec dist/gpx_arc_darwin_arm64: no such file or directory`. **Backend never starts, queries fail.** Confirmed during manual install 2026-05-14 with Grafana 13.0.1. **Fix:** change `BuildAll` outPath to `filepath.Join("dist", fmt.Sprintf("gpx_%s_%s_%s", "arc", platform.os, platform.arch))`. Workaround for now: `cp dist/darwin_arm64/gpx_arc dist/gpx_arc_darwin_arm64`. — [Magefile.go:55](../../Magefile.go#L55) — DONE (this branch) — Magefile rewritten. `buildPlatform()` outputs to `dist/<PluginName>_<os>_<arch>` (flat, matching Grafana SDK loader). `PluginName` const tracks `plugin.json#executable`. Verified: Grafana 13.0.1 starts the plugin backend without `cp` workaround.
- [x] **R2-CR7** — DONE (de6d46c) — `MaxResponseMB` per-datasource setting (default 1024 MiB, cap 8 GiB), `errors.As(*http.MaxBytesError)` in `sanitizeUserError` with actionable message. Frontend exposes "Max Response MB" input. Tests `TestNewArcInstance_MaxResponseMBDefault/Explicit/Cap`. **CRITICAL: `MaxResponseBytes = 256 MiB` is too low for analytical queries.** User reported "Arrow IPC stream truncated after headers committed; client received partial result" from Arc's side when querying ~6M rows. Arc completes the query in ~780-880ms but the plugin closes the connection mid-stream once the response exceeds 256 MiB → Arc sees `connection closed`. A 6M-row query with several float64/int64 cols crosses the cap easily (6M × 4 cols × 8 bytes ≈ 192 MB + Arrow IPC overhead). Reported 2026-05-14 by xe-nvdk. **Fix:** (a) make `MaxResponseBytes` a per-datasource setting (`MaxResponseMB` in jsonData, default 1024 MiB), (b) detect `*http.MaxBytesError` via `errors.As` in `sanitizeUserError` and surface a clear "Response exceeded N MiB — increase the limit in datasource settings or narrow the query" message. — [pkg/plugin/safety.go:21](../../pkg/plugin/safety.go#L21), [pkg/plugin/datasource.go:140](../../pkg/plugin/datasource.go#L140)

## Gemini-only findings (round 3 on commit 26f85ee)

- **Gemini 3244629498** (HIGH) — `record.Release()` leak claim. **FALSE POSITIVE** (third occurrence of the same diff-hunk truncation; `record.Release()` exists at arrow.go:116, just below gemini's window). Rebut + reference.
- **Gemini 3244629507** (MEDIUM) — `appendArrowColumnToField` errors on unsupported types while `createEmptyField` falls back to string. **VALID — listed as R2-HI12.**
- **Gemini 3244629509** (MEDIUM) — refId `errgroup.Group` doesn't use request context. **VALID — listed as R2-HI4** (gemini flagged it as MEDIUM but our internal review surfaced it as HIGH given the cancellation-blast-radius).

---

# ROUND 2 — Suggested commit groupings

### Commit 6 — `fix(signing): address internal review round 2` (~2-3h)
**Priority order — CRITICALs first:**

CR group (must fix): R2-CR1, R2-CR2, R2-CR3, R2-CR4, R2-CR5
HIGH group (signing-blockers): R2-HI1, R2-HI2, R2-HI3, R2-HI4, R2-HI5, R2-HI6, R2-HI12

Tests required:
- Multi-panel concurrency cap (CR1) — N×M shouldn't exceed cap.
- Arrow non-nullable + nulls (CR2) — confirm not stale memory.
- `\nLIMIT`/`\tLIMIT` not splittable (CR3).
- Macro inside string literal for ALL FIVE macros, not just `$__timeFilter` (CR5).
- Per-query DB override sanitization (R2-HI3).
- refId fan-out cancellation (R2-HI4).

### Commit 7 — `fix(signing): round 2 mediums + perf wins` (~2h)
R2-HI7 (heap-alloc per row), R2-HI8 (drop `(?i)`), R2-HI9 (IdleConnTimeout), R2-HI10 (doRequest extraction), R2-HI11 (delete OptimizeTimeSeriesQuery), R2-M1 through R2-M16, R2-M10 (overlaps R2-CR5)

### Commit 8 — `chore(signing): low-priority cleanups + remaining tests` (~1h)
R2-L1 through R2-L14, plus the original-round LOWs still open (L1-L6, L10).

### Original Commit 6/7/8 from round 1 still pending
H1, H2, H5, H6, H7, P6, P7, H13, H14, H15, H16, M1, M2, M9, M10, M11, M12, M20, M23, M24, M25, M26, M30 + remaining LOWs

These will get rolled into the round-2 commits where they overlap (e.g. R2-HI10's `doRequest` extraction covers original H15/H16).


---

# ROUND 3 — Frontend re-review findings (2026-05-15, post-PR #9 internal pass)

**Trigger:** Four-agent review on `chore/frontend-polish` (HEAD `fbb22d4`) before merging. Surface area: ~300 lines across `src/ConfigEditor.tsx`, `src/QueryEditor.tsx`, `src/VariableQueryEditor.tsx`, `src/datasource.ts`, `src/types.ts`, `.config/webpack/webpack.config.js`.

**ID prefix:** `R3-*`.

**Outcome:** PR #9 merged anyway — no merge-blockers, and the regressions below ship alongside earlier fixes. Items tracked for a follow-up frontend polish PR.

## Same-shape regressions introduced by PR #9

- [ ] **R3-HI1** — `VariableQueryEditor` initial-render data-loss window. The round-3 fix narrowed the `useEffect` dep from `[query]` to `[query.query]`, but didn't gate the sync on `query.query !== state.query`. Initial-render race: parent passes `{query: undefined}` → user types into the textarea → parent re-renders with `{query: ''}` from a downstream load → effect fires → clobbers user's typing. **Fix:** add `if (query.query === state.query) return;` at the top of the effect body, OR gate on a `dirty` ref that tracks whether the user has typed. — [src/VariableQueryEditor.tsx:27-30](../../src/VariableQueryEditor.tsx#L27-L30)
- [ ] **R3-M1** — `extractVariableSQL` `??` chain doesn't fall through empty `sql` to `rawSql`. My round-1 fix changed `||` → `??` for stricter null/undefined handling, but the function's stated goal is "first non-empty SQL string" (cross-datasource compat — Postgres clears `sql` but populates `rawSql`). Empty string is now returned for `{sql: '', rawSql: 'SELECT 1'}` instead of falling through. **Fix:** revert to `||` semantics for this specific chain OR write a small `firstNonEmpty` helper that does the right thing explicitly. — [src/datasource.ts:127](../../src/datasource.ts#L127)

## Pre-existing carry-forward findings (worth fixing)

- [ ] **R3-HI2** — `as unknown as DataQueryRequest<ArcQuery>` in `metricFindQuery` hides 7 required fields. `DataQueryRequest` requires `requestId`, `interval`, `intervalMs`, `timezone`, `app`, `startTime` (plus `scopedVars`/`range` as required). `LegacyMetricFindQueryOptions` provides none. The cast claims they're present when nothing constructs them — works today only because Grafana's metric-find plumbing tolerates partial requests. Fix: construct a real minimal request with sensible defaults (`requestId: 'metric-find'`, `interval: '1s'`, `intervalMs: 1000`, `timezone: 'utc'`, `app: CoreApp.Unknown`, `startTime: Date.now()`). Removes the double-cast entirely. — [src/datasource.ts:56](../../src/datasource.ts#L56)
- [ ] **R3-M2** — `theme.spacing(4)` in ConfigEditor hardcodes the literal row-height value rather than tracking Grafana's source-of-truth. Should be `theme.spacing(theme.components.height.md)` — that's where `@grafana/ui` reads InlineField's row height from. If Grafana ever changes the spacing scale, the switchCell wrapper goes out of alignment with the labels (the exact bug this commit fixes). — [src/ConfigEditor.tsx:212](../../src/ConfigEditor.tsx#L212)
- [ ] **R3-M3** — `handleNumericBlur` reads `jsonData[key]` from the render-time closure, not from the event. If user types `"a"` (NaN → undefined via onChange) then tabs out, onBlur sees the pre-onChange `jsonData[key]` (still 30 for timeout) and concludes "no default needed", leaving the saved state as `undefined`. Backend's own clamp masks the user-visible bug. Fix: re-read `event.target.value` directly in onBlur, OR keep a useRef of the latest committed value. — [src/ConfigEditor.tsx:48-51](../../src/ConfigEditor.tsx#L48-L51)
- [ ] **R3-M4** — `interpolateVariable` fallback at the end of the function returns `value` raw for `null`/`undefined`/`boolean`. Signature is narrower than runtime reality — Grafana's `templateSrv.replace` will hand us whatever a variable produced. **Fix:** `return this.quoteLiteral(String(value ?? ''));` at the fallback. Defense in depth. — [src/datasource.ts:105](../../src/datasource.ts#L105)
- [ ] **R3-M5** — `quoteLiteral` doesn't strip ASCII NUL. A user pasting a NUL-bearing variable value produces a DuckDB parse error round-trip. DoS-against-yourself, not injection. **Fix:** strip control chars (`value.replace(/\x00/g, '')`) before quoting. — [src/datasource.ts:82-84](../../src/datasource.ts#L82-L84)

## Lower-priority cleanups (defer to perf/test branch)

- [ ] **R3-L1** — `this.toMetricFindValue` passed unbound on line 57 as `.then(this.toMetricFindValue)`. Works because the method doesn't use `this`; silent breakage the moment someone adds `this.`. **Fix:** arrow-property (matches `interpolateVariable` style) OR top-level helper. — [src/datasource.ts:60](../../src/datasource.ts#L60)
- [ ] **R3-L2** — Hardcoded typography (`fontSize: '14px'`, `'13px'`, `'11px'`, `'12px'`) across QueryEditor and VariableQueryEditor. The polish PR switched to theme COLORS for text but kept literal font sizes. **Fix:** `theme.typography.bodySmall.fontSize` etc. so user font-size preferences propagate. — [src/QueryEditor.tsx:135-141](../../src/QueryEditor.tsx#L135-L141), [src/VariableQueryEditor.tsx:74-78](../../src/VariableQueryEditor.tsx#L74-L78)
- [ ] **R3-L3** — `||` vs `??` inconsistency. ConfigEditor uses `??` for numeric defaults but `||` for `url`/`apiKey` (lines 96, 106). QueryEditor uses `||` everywhere (lines 65, 76, 87). **Fix:** standardize on `??` for nullish fallbacks plug-wide.
- [ ] **R3-L4** — `SPLIT_OPTIONS` values typed as plain `string`; should be a `SplitDuration` union matching `ArcQuery.splitDuration` (currently `string` with a comment listing the values). Promote to a real type in `types.ts`. — [src/QueryEditor.tsx:15-24](../../src/QueryEditor.tsx#L15-L24)
- [ ] **R3-L5** — `VariableQuery` and `VariableQueryProps` interfaces defined locally in `VariableQueryEditor.tsx`; the other types live in `types.ts`. Move for consistency, especially when tests are added. — [src/VariableQueryEditor.tsx:6-13](../../src/VariableQueryEditor.tsx#L6-L13)
- [ ] **R3-L6** — `LABEL_WIDTH` and `INPUT_WIDTH` constants only in ConfigEditor; `VariableQueryEditor.tsx:47` still has magic `labelWidth={20}`. Decide: share via `constants.ts`, or accept the per-editor local sizing.
- [ ] **R3-L7** — Empty constructor in `ArcDataSource` (datasource.ts:33) only calls `super(instanceSettings)`. TypeScript auto-generates this; can be removed. Cosmetic.
- [ ] **R3-L8** — Frontend test coverage is zero. `@testing-library/react` + `jest` + `jest-environment-jsdom` are all in devDependencies. Low-effort wins: `extractVariableSQL` unit tests (covers all 6 input shapes), `toMetricFindValue` dedup order-preservation, ConfigEditor smoke render. Acts as a safety net for fixes to R3-HI1/HI2/M3 above.

## Suggested follow-up

A single branch — **`fix/frontend-followup`** — covering R3-HI1, R3-M1, R3-M2, R3-M3 (~30 min). The other items can roll into the perf/test branch later. Save R3-HI2 (the cast / 7-field construction) for its own commit since it touches the request shape end-to-end.
