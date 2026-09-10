package plugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

// DefaultMaxResponseMB is the default per-response body size cap when the
// user hasn't set `MaxResponseMB` in datasource settings. 1024 MiB fits
// roughly 30M rows with several float64 columns — well clear of the
// 6–10M-row analytical queries Arc serves in normal use (R2-CR7). The
// original hardcoded 256 MiB was reported truncating real workloads.
const DefaultMaxResponseMB = 1024

// MaxResponseMBCap is the upper bound a user can set via `MaxResponseMB`.
// Higher values risk OOMing the plugin process on a runaway query; this is
// a defense in depth bound — set it as high as feels safe for the host's
// memory profile.
const MaxResponseMBCap = 8192

// DefaultMaxInFlight bounds simultaneous Arc requests per datasource across
// every panel and viewer. 32 keeps a busy dashboard responsive while still
// protecting Arc; 1.3.2 used MaxConcurrency (default 4) for both, so a
// 12-panel dashboard served requests four at a time.
const DefaultMaxInFlight = 32

// MaxInFlightCap bounds MaxInFlight. Beyond this, file-descriptor pressure and
// TLS-handshake storms against Arc become the limiting factor.
const MaxInFlightCap = 128

// MaxConcurrencyCap is the upper bound on user-configurable parallel chunk fanout.
// Higher values risk file-descriptor pressure and TLS-handshake storms against Arc.
const MaxConcurrencyCap = 32

// databaseNameRe matches a permitted Arc database name. Conservative on purpose —
// the name flows into an HTTP header and into SQL identifier contexts.
var databaseNameRe = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// errBlockedAddr is returned by the SSRF-safe dialer when a request resolves to
// a private, loopback, or link-local address. Surfaces in errors.Is for callers.
var errBlockedAddr = errors.New("destination address is not permitted")

// columnArgUnsafe matches the characters that could let a macro's column
// argument escape the SQL the macro generates.
//
// The argument is interpolated as an EXPRESSION, never inside a quoted
// literal — `%s >= '<from>'` and `epoch_ns(%s) // n` — so the risk is a single
// quote (which would open a literal and swallow the timestamp that follows),
// a statement separator, or a comment introducer that would comment out the
// rest of the generated predicate.
//
// A double quote is NOT unsafe here: DuckDB uses it for quoted identifiers,
// `"time"` and `t."time"` are ordinary column references, and it cannot
// terminate the single-quoted literals this macro emits.
var columnArgUnsafe = regexp.MustCompile(`[';]|--|/\*`)

// validateColumnArg returns an error if a macro's column argument contains
// something that could break out of the SQL the macro generates.
//
// Deliberately permissive: it rejects dangerous characters rather than
// requiring a bare identifier. 1.3.2 required `^[A-Za-z_][A-Za-z0-9_.]*$`,
// which rejected every ordinary DuckDB column expression -- a quoted
// identifier `"time"`, a qualified-and-quoted `t."time"`, a cast
// `time::TIMESTAMP`, a non-ASCII column name -- and left the macro unexpanded,
// so Arc received a literal `$__timeFilter(...)` and failed to parse.
//
// The permissiveness costs nothing here: the SQL the user types is forwarded
// to Arc verbatim anyway, so Arc's API-key scope, not this regex, is the
// authorization boundary. What this guard is actually for is making sure the
// text WE generate around the argument stays well-formed.
func validateColumnArg(name string) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("empty column argument")
	}
	if columnArgUnsafe.MatchString(name) {
		return fmt.Errorf("column argument contains an unsafe character (quote, semicolon, or comment marker)")
	}
	return nil
}

// validateDatabaseName returns an error if name contains characters that could
// pollute the X-Arc-Database header or be misinterpreted as a SQL identifier.
func validateDatabaseName(name string) error {
	if !databaseNameRe.MatchString(name) {
		return fmt.Errorf("invalid database name %q: must match %s", name, databaseNameRe.String())
	}
	return nil
}

// validateURL rejects URLs whose scheme is not http/https. Hostname-level
// blocking happens at dial time via safeDialContext.
func validateURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("URL scheme %q not allowed (use http or https)", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("URL is missing a host")
	}
	return nil
}

// dialPolicy carries the two independent permissions the SSRF dialer respects:
// loopback-only (for `http://localhost:8000` dev setups) and full private
// access (the user-opt-in `AllowPrivateIPs` flag for corporate-intranet Arc
// deployments). They were previously collapsed into a single `allowPrivate`
// bool, which meant a loopback-configured URL also opened RFC1918 redirects —
// gemini round 5 finding 3244943519.
type dialPolicy struct {
	allowLoopback bool // configured URL is loopback → only loopback IPs allowed
	allowPrivate  bool // admin opted in to RFC1918/CGNAT (intranet deployment)
}

// safeDialContext wraps a net.Dialer so it refuses to connect to disallowed
// addresses. This is the SSRF guard for the user-supplied Arc URL.
//
// `policy` carries the two independent permissions: `allowLoopback` (loopback
// destinations only — derived from `isLoopbackURL(URL)` so dev setups against
// `http://localhost:8000` keep working) and `allowPrivate` (admin opt-in via
// AllowPrivateIPs for corporate intranets — permits both loopback AND RFC1918
// / CGNAT / ULA).
//
// Link-local (including the cloud-metadata 169.254.169.254), multicast, and
// unspecified addresses are blocked regardless — they are never a legitimate
// Arc target.
//
// Resolution-then-validate avoids a TOCTOU between DNS rebind and connect: we
// resolve the host ourselves, drop any disallowed address, then dial the
// remaining ones explicitly by IP. If every resolved address is blocked the
// dial returns errBlockedAddr.
func safeDialContext(policy dialPolicy) func(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		var lastErr error
		for _, ip := range ips {
			if isBlockedIP(ip.IP, policy) {
				lastErr = fmt.Errorf("%w: %s resolves to blocked address %s", errBlockedAddr, host, ip.IP)
				continue
			}
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("%w: no resolvable address for %s", errBlockedAddr, host)
		}
		return nil, lastErr
	}
}

// isBlockedIP returns true for IP ranges the plugin should refuse to contact
// from a user-supplied URL.
//
// Always-blocked (never a legitimate Arc target):
//   - unspecified (0.0.0.0/::)
//   - link-local (incl. 169.254.169.254 cloud metadata)
//   - multicast
//
// Conditionally allowed (per dialPolicy):
//   - loopback (127.0.0.0/8, ::1) — when `policy.allowLoopback` OR `policy.allowPrivate`
//   - private RFC1918 (10/8, 172.16/12, 192.168/16) + IPv6 ULA (fc00::/7) +
//     CGNAT (100.64.0.0/10) — only when `policy.allowPrivate`
//
// The two flags are independent: `allowLoopback=true, allowPrivate=false`
// (dev URL is loopback, admin didn't opt in to private) lets the dialer
// reach `127.0.0.1` but still blocks redirects to `10.0.0.5`. Previously
// these were collapsed into one bool and a loopback URL opened RFC1918 too.
func isBlockedIP(ip net.IP, policy dialPolicy) bool {
	if ip == nil {
		return true
	}
	// Unconditional blocks — these are never a real Arc deployment.
	if ip.IsUnspecified() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.IsMulticast() {
		return true
	}
	if ip.IsLoopback() {
		// Loopback is allowed by either flag.
		return !policy.allowLoopback && !policy.allowPrivate
	}
	// Non-loopback private ranges: only allowed by allowPrivate.
	if policy.allowPrivate {
		return false
	}
	if ip.IsPrivate() {
		return true
	}
	// 100.64.0.0/10 — Carrier-grade NAT, not covered by IsPrivate.
	if ip4 := ip.To4(); ip4 != nil && ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127 {
		return true
	}
	return false
}

// isLoopbackURL reports whether the URL's host is a loopback hostname/IP.
// Used to derive the dialer's loopback policy from the configured Arc URL so
// dev setups against http://localhost:8000 keep working while a configured
// public URL cannot be redirected back to loopback (e.g. a malicious 302 to
// http://127.0.0.1/...).
func isLoopbackURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	host := u.Hostname()
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return true
	}
	return false
}

// newHTTPClient builds a long-lived http.Client that:
//   - refuses to connect to private/loopback/metadata addresses,
//   - validates redirects against the same blocklist,
//   - applies a request-level timeout.
//
// One client is created per datasource instance (in newArcInstance) and
// reused across every request — sharing the transport's connection pool and
// TLS session cache. The policy carries TWO independent flags:
//   - allowLoopback: configured URL is loopback (`localhost`/`127.0.0.1`)
//     → loopback IPs are permitted on dial; RFC1918 stays blocked.
//   - allowPrivate: admin opted in via AllowPrivateIPs (corporate intranet)
//     → loopback AND RFC1918/CGNAT/ULA are all permitted.
//
// Link-local (incl. cloud-metadata) and unspecified addresses are blocked
// regardless. Previously these two were collapsed into one bool, which meant
// a loopback URL would also open RFC1918 redirects (gemini round 5 finding
// 3244943519).
func newHTTPClient(timeout time.Duration, policy dialPolicy) *http.Client {
	transport := &http.Transport{
		// Honour HTTP_PROXY / HTTPS_PROXY / NO_PROXY. The custom transport
		// added in 1.3.2 omitted this, so a Grafana behind a corporate egress
		// proxy silently stopped reaching a cloud-hosted Arc — http.Transport
		// defaults to no proxy, unlike http.DefaultTransport.
		Proxy:           http.ProxyFromEnvironment,
		DialContext:     safeDialContext(policy),
		MaxIdleConns:    100,
		MaxConnsPerHost: MaxInFlightCap,
		// A dashboard refreshing every 1-5 minutes re-dialled on every refresh
		// at 90s, paying a TLS handshake each time.
		IdleConnTimeout:       5 * time.Minute,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			if err := validateURL(req.URL.String()); err != nil {
				return err
			}
			return nil
		},
	}
}

// sanitizeUserError takes an internal error (which may contain server-side
// detail like DuckDB plan fragments, file paths, table/column names) and
// returns a string safe to surface to dashboard viewers who may have less
// privilege than the datasource admin.
//
// The full error is logged server-side for operator diagnostics; the returned
// string keeps the high-level category but strips identifiers and paths.
// duckdbUserErrorPrefixes are the DuckDB error types that describe a defect in
// the SQL the user wrote, rather than anything about the server. Each names
// the user's own text back to them and carries no schema they did not already
// reference, no file path, and no query plan.
// duckdbUserErrorPrefixes lists the DuckDB error types whose text contains
// NOTHING but the user's own SQL. Membership was decided by running each
// error type against DuckDB 1.4.3 and reading what it actually emits, not by
// how the name sounds:
//
//	Parser Error: "syntax error at or near \"h01\"" — quotes the user's text.
//	Syntax Error: same shape.
//
// Deliberately EXCLUDED, because each leaks something the dashboard viewer
// never referenced and may not be entitled to see:
//
//	Catalog Error:    names tables and schemas.
//	Binder Error:     appends `Candidate bindings: "secret_salary"` — a list
//	                  of real column names from the table.
//	Conversion Error: echoes the offending ROW VALUE, e.g. Could not convert
//	                  string 'secret-value-xyz' to INT32.
//	Type Error:       can carry column names from the failing expression.
//
// Those stay summarised; their full text is in the server log.
var duckdbUserErrorPrefixes = []string{
	"Parser Error",
	"Syntax Error",
}

// userFixableArcError returns the DuckDB detail from an Arc error message when
// that detail describes a mistake in the user's SQL. The returned string keeps
// the "Arc error (HTTP n):" prefix so the origin stays visible.
func userFixableArcError(msg string) (string, bool) {
	for _, prefix := range duckdbUserErrorPrefixes {
		if strings.Contains(msg, prefix+":") {
			return msg, true
		}
	}
	return "", false
}

// sanitizeUserErrorSQL is sanitizeUserError plus the expanded SQL in the
// server-side log line.
//
// The panel deliberately never shows the SQL, but an operator reading the log
// needs it: without it the log says a query failed and not which one, so
// diagnosing a macro or interpolation problem means reconstructing the query
// by hand. That reconstruction is what produced four wrong diagnoses during
// the 1.3.x regression.
func sanitizeUserErrorSQL(refID, sql string, err error) string {
	return sanitizeUserErrorWith(refID, err, "sql", sql)
}

func sanitizeUserError(refID string, err error) string {
	return sanitizeUserErrorWith(refID, err)
}

// sanitizeUserErrorWith is the shared implementation. `extra` is appended to
// the server-side log line as key/value pairs and never reaches the user.
func sanitizeUserErrorWith(refID string, err error, extra ...any) string {
	// User-initiated cancellation is benign — Grafana cancels the in-flight
	// request when the user edits a query, changes the dashboard, or
	// navigates away. Logging at Error level on every panel edit would
	// flood the operator's log; log at Debug and return a neutral message.
	if errors.Is(err, context.Canceled) {
		log.DefaultLogger.Debug("Arc query canceled by client", "refId", refID)
		return "Query canceled"
	}
	fields := append([]any{"refId", refID, "error", err.Error()}, extra...)
	log.DefaultLogger.Error("Arc query failed", fields...)
	msg := err.Error()
	// Typed-error matching first (preferred). String contains is a fallback
	// for paths that don't have a typed sentinel yet.
	var maxBytesErr *http.MaxBytesError
	switch {
	case errors.Is(err, errBlockedAddr):
		return "Arc URL resolves to a blocked address (private/loopback). Update the datasource URL or enable 'Allow Private IPs'."
	case errors.As(err, &maxBytesErr):
		// R2-CR7: the previous "exceeded the configured size limit" message
		// didn't tell the user how to fix it. The cap is now per-datasource
		// via MaxResponseMB — point them at it.
		return fmt.Sprintf("Query result exceeded the configured size limit (%d MiB). Raise 'Max Response MB' in datasource settings, add LIMIT, or narrow the time range.", maxBytesErr.Limit/(1024*1024))
	case errors.Is(err, context.DeadlineExceeded), strings.Contains(msg, "Client.Timeout"):
		return "Query timed out. Try reducing the time range, increasing the timeout, or enabling query splitting."
	case strings.Contains(msg, "connection refused"):
		return "Cannot connect to Arc — connection refused."
	case strings.Contains(msg, "no such host"):
		return "Cannot connect to Arc — hostname not found."
	case strings.HasPrefix(msg, "Arc error (HTTP "):
		// A DuckDB error naming the user's own mistake — a parser error, an
		// unknown column, a missing table — belongs in the panel. Hiding it
		// forces the dashboard author to hunt through the Grafana server log
		// for something they could fix in the editor, which is exactly what
		// made the 1.3.x regression take a day to diagnose instead of minutes.
		//
		// Classified by DuckDB's error TYPE, not by HTTP status: Arc reports a
		// parser error as 500, so a status-based rule would hide the very
		// errors that matter most. Anything not on the allowlist stays
		// summarised, and the full text is in the log line above.
		if detail, ok := userFixableArcError(msg); ok {
			return detail
		}
		end := strings.Index(msg, "):")
		if end > 0 {
			return msg[:end+1] + " query failed (see server logs for detail)"
		}
		return "Arc query failed (see server logs for detail)"
	default:
		return "Query failed (see server logs for detail)"
	}
}
