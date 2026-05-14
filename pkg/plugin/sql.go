package plugin

import (
	"regexp"
	"strings"
)

// strippedSQL is the result of removing single-quoted string literals AND
// SQL comments from a SQL statement, plus an upper-cased view of the same.
// Both are computed once per query and passed to every keyword-detection
// heuristic that follows. Without the cache, each heuristic re-ran
// stripStringLiterals + ToUpper independently — three full-string passes per
// query before C4/M15 (see signing-readiness M15/M16/M27).
type strippedSQL struct {
	stripped string // literals + comments removed, original case
	upper    string // uppercase of stripped
}

// newStrippedSQL produces a strippedSQL view of `sql` for downstream heuristics.
func newStrippedSQL(sql string) strippedSQL {
	s := stripStringLiteralsAndComments(sql)
	return strippedSQL{stripped: s, upper: strings.ToUpper(s)}
}

// stripStringLiteralsAndComments removes content inside single-quoted string
// literals AND removes `-- line comments` and `/* block comments */`. This
// prevents heuristics from false-positive matching on values inside literals
// (e.g. `WHERE message = 'count(*) is high'`) or on commented-out keywords
// (e.g. `-- LIMIT 10`).
//
// Single-quoted literals use SQL's escaped-quote convention (`''` inside).
// Double-quoted identifiers are NOT touched — DuckDB and Postgres use them
// for column names that contain special characters, so keyword detection on
// them is still desired.
func stripStringLiteralsAndComments(sql string) string {
	var out strings.Builder
	out.Grow(len(sql))
	i := 0
	for i < len(sql) {
		c := sql[i]
		// Line comment: -- ... \n
		if c == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			end := strings.IndexByte(sql[i:], '\n')
			if end < 0 {
				return out.String() // rest of SQL is a comment
			}
			i += end // keep the newline, drop the comment text
			continue
		}
		// Block comment: /* ... */. Replace with a single space so adjacent
		// tokens stay separated (e.g. `SELECT/*x*/col` becomes `SELECT col`,
		// not `SELECTcol`) — otherwise keyword detection would miss the SELECT.
		if c == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				return out.String() // unterminated block comment
			}
			out.WriteByte(' ')
			i += 2 + end + 2
			continue
		}
		// Single-quoted literal
		if c == '\'' {
			i++
			for i < len(sql) {
				if sql[i] == '\'' {
					// Escaped quote ('') inside literal
					if i+1 < len(sql) && sql[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			continue
		}
		out.WriteByte(c)
		i++
	}
	return out.String()
}

// limitRe matches a LIMIT clause anywhere in the (stripped) SQL —
// whitespace-bounded on both sides so `WHERE\nLIMIT 100`, `WHERE\tLIMIT 100`,
// and end-of-string `LIMIT 100` all match. Previously the substring check
// required a literal " LIMIT " with single ASCII spaces, missing newline-
// and tab-separated forms (R2-CR3 — splitting was NOT skipped, returning
// N×LIMIT rows for a LIMIT-N query).
var limitRe = regexp.MustCompile(`(?i)\bLIMIT\s+\d`)

// unionRe matches the UNION keyword bounded by whitespace on both sides.
// Same whitespace-fragility fix as limitRe.
var unionRe = regexp.MustCompile(`(?i)\bUNION\b`)

// containsLIMIT reports whether the SQL has a LIMIT clause. Note this can
// match a LIMIT inside a subquery — conservative on purpose: skipping
// splitting is always correct, just slower.
func containsLIMIT(s strippedSQL) bool {
	return limitRe.MatchString(s.stripped)
}

// containsUnion reports whether the SQL contains a UNION operator. Macro
// expansion in multi-statement queries produces mangled SQL when split, so
// we conservatively skip splitting on UNION.
func containsUnion(s strippedSQL) bool {
	return unionRe.MatchString(s.stripped)
}

// hasTimeFilterMacro reports whether the SQL uses one of the time macros in
// a position where the macro engine would expand it (i.e. outside string
// literals and comments). A commented-out macro shouldn't keep splitting
// enabled — the macro won't expand, so each chunk would re-run the full
// query without a time filter.
func hasTimeFilterMacro(s strippedSQL) bool {
	return strings.Contains(s.stripped, "$__timeFilter") ||
		strings.Contains(s.stripped, "$__timeFrom") ||
		strings.Contains(s.stripped, "$__timeGroup")
}

// aggregationFnRe matches any SQL aggregation function call. Anchored at a
// word boundary so it doesn't fire on substrings: `LIST(` won't match
// `list_contains(`, and `SUM(` won't match `CHECKSUM(`. CTE / subquery
// false positives are still possible (`SELECT * FROM (SELECT COUNT(*) ...)`
// will skip splitting unnecessarily) — conservative is correct here.
var aggregationFnRe = regexp.MustCompile(`(?i)\b(SUM|FSUM|COUNT|COUNTIF|AVG|FAVG|MIN|MAX|ANY_VALUE|` +
	`ARG_MIN|ARG_MIN_NULL|ARG_MAX|ARG_MAX_NULL|FIRST|LAST|PRODUCT|` +
	`STRING_AGG|LIST|ARRAY_AGG|BOOL_AND|BOOL_OR|` +
	`BIT_AND|BIT_OR|BIT_XOR|BITSTRING_AGG|GEOMETRIC_MEAN|WEIGHTED_AVG|` +
	`MEDIAN|MODE|MAD|STDDEV|STDDEV_POP|STDDEV_SAMP|` +
	`VARIANCE|VAR_POP|VAR_SAMP|SKEWNESS|SKEWNESS_POP|` +
	`KURTOSIS|KURTOSIS_POP|ENTROPY|CORR|` +
	`COVAR_POP|COVAR_SAMP|QUANTILE|QUANTILE_CONT|QUANTILE_DISC|` +
	`HISTOGRAM|HISTOGRAM_EXACT|HISTOGRAM_VALUES|` +
	`APPROX_COUNT_DISTINCT|APPROX_QUANTILE|APPROX_TOP_K|RESERVOIR_QUANTILE|` +
	`REGR_AVGX|REGR_AVGY|REGR_COUNT|REGR_INTERCEPT|REGR_R2|REGR_SLOPE|` +
	`REGR_SXX|REGR_SXY|REGR_SYY)\s*\(`)

// distinctRe matches `DISTINCT` as a keyword (followed by space, paren, or
// end-of-SQL), not as a substring inside an identifier.
var distinctRe = regexp.MustCompile(`(?i)\bDISTINCT\b`)

// windowFnRe matches the SQL window function `OVER (...)` clause with any
// whitespace between `OVER` and the paren.
var windowFnRe = regexp.MustCompile(`(?i)\bOVER\s*\(`)

// containsAggregationWithoutTimeGroup returns true when the SQL aggregates
// (GROUP BY, DISTINCT, an aggregate function, or a window function) but has
// no `$__timeGroup` macro. Splitting such queries would aggregate each chunk
// independently and produce wrong results (duplicated groups, inflated counts,
// per-chunk window resets).
//
// Word-boundary matching prevents false positives on `CHECKSUM(`,
// `list_contains(`, identifiers ending in `_DISTINCT`, etc. Comments and
// string literals are stripped via newStrippedSQL.
//
// Known limitation: a subquery aggregation inside an outer non-aggregating
// query will still trigger this and disable splitting unnecessarily. The
// inverse error (allowing a split that produces wrong results) is much worse
// than the over-conservative one we have, so we accept it.
func containsAggregationWithoutTimeGroup(s strippedSQL) bool {
	// Use stripped view so a commented-out $__timeGroup doesn't disable the
	// aggregation guard (the macro wouldn't expand, so the chunked-aggregation
	// hazard would still be present).
	if strings.Contains(s.stripped, "$__timeGroup") {
		return false
	}
	if strings.Contains(s.upper, "GROUP BY") {
		return true
	}
	if distinctRe.MatchString(s.stripped) {
		return true
	}
	if aggregationFnRe.MatchString(s.stripped) {
		return true
	}
	if windowFnRe.MatchString(s.stripped) {
		return true
	}
	return false
}
