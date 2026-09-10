# Arc Grafana Datasource — Claude Code Instructions

## Project Overview

`grafana-arc-datasource` is a **Grafana datasource plugin** for the Arc time-series database. It uses Arc's Apache Arrow IPC endpoint (`/api/v1/query/arrow`) for high-performance columnar data transfer, with a JSON endpoint (`/api/v1/query`) as fallback. Apache-2.0, public repo.

**Tech stack:** TypeScript + React (Grafana UI) on the frontend; Go + `grafana-plugin-sdk-go` on the backend; Apache Arrow Go library for IPC decoding. Build is npm + Mage. Plugin ID is `basekick-arc-datasource`, binary is `gpx_arc`.

## Architecture

- **Frontend (`src/`)** — Datasource entry point + Grafana UI components.
  - [datasource.ts](src/datasource.ts) — query dispatcher, extends `DataSourceWithBackend`
  - [QueryEditor.tsx](src/QueryEditor.tsx) — SQL editor, format selector (time series / table), query-splitting tooltip
  - [ConfigEditor.tsx](src/ConfigEditor.tsx) — URL, API key (`secureJsonData`), database, timeout, Arrow toggle
  - [VariableQueryEditor.tsx](src/VariableQueryEditor.tsx) — template-variable SQL
  - [types.ts](src/types.ts) — shared TS types for query model + datasource options
- **Backend (`pkg/plugin/`)** — Go plugin invoked by Grafana via the plugin SDK.
  - [datasource.go](pkg/plugin/datasource.go) — `QueryData`, `CheckHealth`, `CallResource`
  - [query.go](pkg/plugin/query.go) — macro expansion (`$__timeFilter`, `$__timeFrom`, `$__timeTo`, `$__interval`, `$__timeGroup`), query-splitting heuristic, ORDER BY auto-addition
  - [arrow.go](pkg/plugin/arrow.go) — Arrow IPC decoder → `*data.Frame`
  - JSON fallback path lives alongside the Arrow path; both must return shape-compatible frames so panels keep working when Arrow is off
- **Data contract with Arc:** SQL string in, Arrow IPC stream (or JSON envelope) out. All time columns expected as `TIMESTAMP[ms]` from Arc; backend converts to `*time.Time`.
- **Time-series vs table format:** the editor format selector drives whether the backend wide-format frames are emitted or returned as-is. Long-format conversion is Grafana's job — don't reimplement it here.

## Build & Test

```bash
# Frontend
npm install
npm run build          # production bundle in dist/
npm run dev            # webpack watch
npm run typecheck      # tsc --noEmit
npm run lint           # eslint
npm run test:ci        # jest

# Backend
mage -v                # build current platform
mage -v buildAll       # cross-compile every supported platform
go test ./pkg/...      # backend unit tests

# Plugin packaging (release)
npm run build && mage -v buildAll
npm run sign           # @grafana/sign-plugin
```

**Local install for manual testing:**
```bash
ln -s "$(pwd)/dist" /var/lib/grafana/plugins/basekick-arc-datasource
systemctl restart grafana-server
```

The compiled backend binary lives at `dist/gpx_arc_<os>_<arch>`. Grafana loads it based on `plugin.json#executable: "gpx_arc"`.

## Conventions

### TypeScript / React

- Functional components + hooks; match `@grafana/ui` patterns. No class components.
- Strict TypeScript — no `any` unless you can name the Grafana SDK type that resists narrowing, and leave a one-line WHY comment.
- Format with Prettier (`.prettierrc.js`). Lint with the bundled `@grafana/eslint-config`.
- Component files are PascalCase (`QueryEditor.tsx`); modules are camelCase (`datasource.ts`).
- Secure datasource fields go in `secureJsonData` (API key). Never put credentials in `jsonData` — that field is readable by anyone with dashboard view access.

### Go (backend plugin)

- Format with `gofmt`; vet with `go vet ./...`.
- Use the `grafana-plugin-sdk-go` logger via the `backend` package — `log.DefaultLogger` is fine. **No `fmt.Println` / `log.Print` in handlers** — Grafana drops them.
- Return errors via `backend.DataResponse{Error: err}` rather than panicking. The `panic-recovery` work in [9521bed] is the policy here — every handler must survive a malformed Arrow batch without taking the plugin process down.
- Parameterize at the API boundary; never `fmt.Sprintf` user input into SQL when forwarding to Arc. (The macro expander operates on trusted dashboard state, not user-typed SQL — the SQL the user types goes to Arc verbatim, which is correct: Arc is the authorization boundary.)
- Time-series wide-frames: timestamp field first, value fields after. Match what Grafana's transformations expect.

### Macros (the load-bearing contract)

Macros are expanded by `query.go` before the SQL hits Arc. Adding a macro means changing one place (`expandMacros`-style function) and adding tests in `query_test.go`. Current set:

| Macro | Expands to |
|-------|-----------|
| `$__timeFilter(col)` | `col >= '<from>' AND col < '<to>'` |
| `$__timeFrom()` | `'<from>'` |
| `$__timeTo()` | `'<to>'` |
| `$__interval` | DuckDB-compatible interval string (e.g. `'1 minute'`) |
| `$__timeGroup(col, interval)` | `time_bucket(INTERVAL '<interval>', col)` using `epoch_ns` integer-division precision (see [04c316f]) |

**Don't change the timestamp string format** without checking Arc's DuckDB parser — RFC3339 / ISO-8601 with `Z` suffix is what currently works. DuckDB's `read_parquet()` paths use single-quote escaping; macros must do the same if they ever interpolate paths.

### Query splitting heuristic

Lives in `query.go`. The heuristic skips splitting when any of these appear:
- `LIMIT` clause
- No `$__timeFilter` (no time range to split)
- Aggregations without time bucketing (collapsing to scalar, so chunked execution would change semantics)
- Window functions, `UNION`, DuckDB-specific aggregates, `DISTINCT(`, `APPROX_COUNT_DISTINCT` (see [bd3bd7b], [4c912bb], [2e34aee])

Before changing this heuristic, **read `stripStringLiterals` first** (introduced in the gemini-review fix-up [8b9c5f2]) — string-literal `count(` must not trigger the aggregation detector. When in doubt, add the failing query as a test case before changing the regex.

### Arrow type handling

`arrow.go` maps Arrow types to Grafana `data.Field` types. **Int64 / Uint64 promote to `*float64`** for Grafana numeric panels (see [3a843ef] — this is required, not optional; Grafana's stat / gauge / graph panels coerce int columns inconsistently and the promotion makes them all just work). Don't revert this without checking every panel type.

Timestamp handling: Arc emits `TIMESTAMP[ms]`. Use `timestampType.GetToTimeFunc()` rather than dividing by 1000 yourself — the Arrow library handles unit conversion correctly. Bool, string, float64 are straight through.

If you add a new Arrow type branch, default the fallback to **string**, not panic — unknown types arriving in production should degrade the panel, not crash the plugin.

### Git & PRs

- Branch from `main`. Names: `feat/description`, `fix/description`.
- Commit format: `feat(scope): description` / `fix(scope): description`. Scopes seen in history: `fields`, `query`, `arrow`, `frontend`, `backend`, `build`, `docs`.
- PR description = Summary bullets + Test plan checklist (matches the existing PRs in this repo).
- **PR review — gemini-code-assist:** post a comment containing `@gemini-code-assist` (no brackets, no `[bot]` suffix) on every PR. The `/gemini review` slash command also works. **Do NOT use `gh pr edit --add-reviewer 'gemini-code-assist[bot]'`** — verified in May 2026 to return `Could not resolve user with login`. The bot cannot be assigned via the reviewer API; trigger it via @-mention or slash command in a PR comment.
- Address gemini findings in a **single fix-up commit** — multiple recent PRs (#6 chain, [4e4e14b], [12eb788], [ff18973]) demonstrate the pattern: gemini flags H/M/L items, one commit addresses them all, then re-review.

## Planning & Review Process

### Plan documentation

When exiting plan mode for a non-trivial change, **save the plan first** as `docs/progress/<date>-<feature>.md` (the directory is gitignored — these are scratch artifacts, not tracked docs). The plan goes through pre-implementation review before any code is written.

After a feature merges, two kinds of docs DO get tracked under `docs/` (create the directory if it doesn't exist yet — there's precedent for this pattern in the broader Basekick repos):

- **Frozen artifacts** — readiness reviews, design decisions tied to a moment. File name `docs/<date>-<topic>.md`, don't edit after merge.
- **Living artifacts** — roadmap, operating procedures, anything updated over time. File name `docs/<topic>.md` (no date prefix), edited in place. Living docs should say "Living document. Update as scope or priorities shift." in their preamble.

### Pre-plan validation

Before finalizing a plan, use another agent to validate findings. If there is consensus, ask the user for authorization to move forward.

### Post-implementation review — four agents in parallel

The bar is **"would gemini-code-assist or a sharp human reviewer flag this on the next PR pass?"** Past PRs in this repo have received line-level findings from gemini (string-literal handling in regexes, error-chain preservation, schema-safe merging, panic recovery, ORDER BY semantics). Internal review must catch those *before* gemini — internal review is the cheap pass; gemini is the budget reviewer that should land on a near-clean diff.

Frame each agent as a **staff/principal engineer with deep expertise in Grafana plugin development, Apache Arrow internals, and SQL query semantics**. Include the directive in every prompt: *"Do a line-level pass — flag log-spam in loops, unbounded allocations, hot-path string concatenations, repeated parsing/marshalling, dead parameters, helpers placed in the wrong file. Don't be deferential. Output file:line for every finding."*

Run these **FOUR agents in parallel**:

1. **Correctness & failure modes** (staff database/plugin engineer lens):
   - Arrow decode: empty record batches, zero-row batches, nullable columns, mid-stream errors. Does the decoder handle a record with no columns?
   - Frame-merging across split chunks (see [9521bed]): schema mismatch between chunks, missing column in one chunk but present in another, type drift. Are we panicking or degrading gracefully?
   - Macro expansion: time range with `from == to`, `to < from`, sub-second intervals, timezone-aware vs naive timestamps. Does `$__interval` round to something DuckDB accepts?
   - Query splitting boundaries (see [3097a68]): are chunk boundaries aligned to the bucket interval? Does the union of chunks equal the unsplit result, exactly?
   - Panic recovery: does every `QueryData` path have a recover? Does a panic in one refId fail just that query, not the batch?
   - `context.Context` propagation: every outbound HTTP call to Arc honors the request context. No `context.Background()` slipping in.

2. **Security** (principal security lens):
   - **API key exposure:** `secureJsonData` only. Never logged, never echoed in error messages back to the frontend, never put in URL strings.
   - **SQL injection upstream:** the user's typed SQL goes to Arc verbatim (correct — Arc is the authorization boundary). But anything *we* construct (macro expansion, ORDER BY auto-addition, split chunk SQL) must escape single quotes in interpolated strings, especially path-shaped values.
   - **SSRF on URL field:** the datasource URL is admin-configured, but the backend HTTP client should not follow redirects to internal addresses without thought. Default Go `http.Client` does follow redirects — if that's a concern for this datasource model, document it.
   - **Error-message leakage:** Arc's error responses sometimes include query plan fragments or paths. Verify nothing sensitive surfaces in `backend.DataResponse.Error` strings shown to dashboard viewers (who may have less privilege than the datasource admin).
   - **Resource exhaustion:** unbounded result sets — does the plugin OOM on a 10M-row response? Streaming decode vs full buffer. Timeout respected.

3. **Code quality + idioms** (staff TS + Go lens):
   - Redundant code, multiple passes over the same Arrow record, helpers used cross-file but defined in a feature file.
   - Errors wrapped with `%w`; preserve the error chain (see [8b9c5f2]). Sentinel errors for taxonomy, not substring matching on `err.Error()`.
   - Dead parameters, unused fields, comments that just restate the code.
   - Magic numbers extracted to named constants (chunk sizes, timeout defaults).
   - TS: no `any` without justification, no `as` casts that bypass narrowing, props typed exactly. React: no `useEffect` that should be a derived value.

4. **Performance, observability, operational hygiene** (staff perf engineer lens — the **"gemini line-level pass"** agent; **do not skip this one**):
   - **Hot-path string concatenation** — flag every `out += x` in loops. Use `strings.Builder` in Go, array `join` in TS.
   - **Repeated parsing / marshalling** that should be cached (regexes, JSON templates).
   - **Map / slice allocations** with hardcoded undersized initial capacity. Pre-size when you know the row count.
   - **Arrow column iteration** — use `array.Float64.Float64Values()` slices where available; per-row `Value(i)` in a tight loop is slower than the bulk accessor.
   - **Logging** — Debug logs in production hot paths, missing structured fields (`refId`, `panel_id`, `duration_ms`). Use `log.DefaultLogger.Debug` with key/value pairs.
   - **Schema-safe merging:** when joining frames across split chunks, are we re-allocating the result on every chunk (`append` to a growing slice) instead of pre-sizing? See [9521bed] for the reference fix.
   - **Frontend bundle size:** any new `import` from `@grafana/ui` should be a named import, not the entire module. Webpack tree-shakes named imports.
   - **Cancellation propagation** — does every long-running loop check `ctx.Err()` at boundaries? In TS, does the datasource cancel in-flight `fetch` when Grafana cancels the query?

5. **Release hygiene**: Update `CHANGELOG.md`, bump `package.json` and `plugin.json` versions together. **Re-update after each post-review fix-up commit** — do not let docs drift behind the code across review rounds.

### Review loop discipline

Address all internal-review findings in a **single follow-up commit** BEFORE asking gemini. Do not ship intermediate commits to gemini that internal review already caught. Each gemini round-trip costs context and time; aim for a clean first-pass.

If gemini flags items the internal review missed, **before fixing them**, spawn a fresh round of the four agents with explicit instructions to look for issues *of the same shape* gemini caught. This breaks the back-and-forth pattern. The PR history (#6 chain with `Address Gemini review`, `Address architecture observations`, `Fix remaining PR review items`) shows what happens without this discipline — multiple cleanup rounds that one internal pass could have caught.

## Plugin-specific gotchas

These are real issues that hit recent PRs in this repo. Memorize them.

1. **Int64 / Uint64 must promote to float64 for Grafana panels.** [3a843ef] — Grafana's stat / gauge panels coerce integer columns inconsistently. Promoting in the Arrow decoder makes every panel type just work. Don't revert without testing every visualization.

2. **Frame schema-safe merging is mandatory across split chunks.** [9521bed] — if chunk A returns columns `[time, cpu]` and chunk B returns `[time, cpu, host]` (e.g. because chunk B's range includes data from a different writer), naive `frame.AppendRow` panics. The merger must reconcile schemas before appending.

3. **`time_bucket` precision via `epoch_ns` integer division.** [04c316f] — DuckDB's native `time_bucket` with sub-second intervals loses precision. The macro expands to `epoch_ns(col) - epoch_ns(col) % <ns>` form to preserve nanosecond accuracy. Don't "simplify" this back to native `time_bucket` without re-running the precision tests.

4. **Query splitting must skip queries without `$__timeFilter`.** [6ecbe09] — Without a time range, there's no axis to split along. The heuristic returns the unsplit query; do not invent a synthetic time range.

5. **String literals in SQL must be stripped before aggregation detection.** [8b9c5f2] — `WHERE message = 'count(*) is high'` should not trigger the aggregation heuristic. `stripStringLiterals` runs first. If you add a new detector, run it on the stripped string.

6. **ORDER BY auto-addition is for time-series format only.** [5ac0662] — table format should preserve the user's row order. Adding ORDER BY to a table query that the user explicitly didn't sort will surprise them.

7. **`@gemini-code-assist[bot]` cannot be added as a reviewer.** Use `@gemini-code-assist` in a PR comment or the `/gemini review` slash command. See "Git & PRs" above.

8. **Plugin signing is required for unsigned-plugin-restricted Grafana installs.** `npm run sign` runs `@grafana/sign-plugin`. The signature is environment-specific; a plugin signed for `private` distribution will not load in `community` mode. Don't commit signatures.

## Common pitfalls

- **`fmt.Println` / `console.log` in handlers** — both disappear silently in Grafana. Use the plugin SDK logger / Grafana's `console` via `getBackendSrv`.
- **`any` in TS types** — strict mode catches most cases, but `as any` casts bypass it. If you must, add `// WHY:` explaining why narrowing fails here.
- **Forgetting `mage -v` after Go changes** — the frontend `npm run dev` watcher doesn't rebuild the backend. Restart Grafana to pick up a new `gpx_arc` binary.
- **`dist/` is the deploy artifact, not source.** Don't edit files there expecting them to persist; the next build wipes them.
- **`arrow.go.backup` is a leftover from a refactor.** Don't extend it; if you need the old behavior, pull it from git history. Same for `arrow_flightsql_style.go` if it ends up dead — prune rather than parallel-maintain.
- **Plugin ID is `basekick-arc-datasource`, binary is `gpx_arc`.** These come from `plugin.json#id` and `plugin.json#executable` respectively; changing either is a breaking change for existing installations.
- **Grafana 10.0+ is the minimum** (`plugin.json#grafanaDependency`). Don't use 11.x-only APIs without bumping that floor and the README install instructions.

## Reference docs (external)

- Grafana plugin development: <https://grafana.com/docs/grafana/latest/developers/plugins/>
- `grafana-plugin-sdk-go` godoc: <https://pkg.go.dev/github.com/grafana/grafana-plugin-sdk-go>
- Apache Arrow Go: <https://pkg.go.dev/github.com/apache/arrow/go/v15>
- `@grafana/ui` Storybook: <https://developers.grafana.com/ui/latest/>
- Arc API (the upstream this plugin queries): the Arc repo's `docs/` and `internal/api/`

## Companion docs

- Plugin architecture: [ARCHITECTURE.md](ARCHITECTURE.md) — keep in sync when changing data flow or Arrow handling
- User-facing docs: [README.md](README.md) — update when adding macros, config options, or supported query shapes
- Release process: [PUBLISHING.md](PUBLISHING.md) — signing, version bumps, marketplace submission
- Arc-side query endpoints (upstream): the Arc repo, `internal/api/query.go` for the `/api/v1/query/arrow` handler
