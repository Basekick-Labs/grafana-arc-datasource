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

// MaxConcurrencyCap is the upper bound on user-configurable parallel chunk fanout.
// Higher values risk file-descriptor pressure and TLS-handshake storms against Arc.
const MaxConcurrencyCap = 32

// columnNameRe matches a SQL column or qualified column reference (table.col).
// Used to validate macro arguments before interpolating them into SQL.
var columnNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.]*$`)

// databaseNameRe matches a permitted Arc database name. Conservative on purpose —
// the name flows into an HTTP header and into SQL identifier contexts.
var databaseNameRe = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// errBlockedAddr is returned by the SSRF-safe dialer when a request resolves to
// a private, loopback, or link-local address. Surfaces in errors.Is for callers.
var errBlockedAddr = errors.New("destination address is not permitted")

// validateColumnArg returns an error if name doesn't look like a safe SQL column
// reference. Used by macro expanders before interpolating column arguments.
func validateColumnArg(name string) error {
	if !columnNameRe.MatchString(name) {
		return fmt.Errorf("invalid column argument %q: must match %s", name, columnNameRe.String())
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
		return !(policy.allowLoopback || policy.allowPrivate)
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
		DialContext:           safeDialContext(policy),
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
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
func sanitizeUserError(refID string, err error) string {
	log.DefaultLogger.Error("Arc query failed", "refId", refID, "error", err.Error())
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
	case strings.Contains(msg, "context deadline exceeded"), strings.Contains(msg, "Client.Timeout"):
		return "Query timed out. Try reducing the time range, increasing the timeout, or enabling query splitting."
	case strings.Contains(msg, "connection refused"):
		return "Cannot connect to Arc — connection refused."
	case strings.Contains(msg, "no such host"):
		return "Cannot connect to Arc — hostname not found."
	case strings.HasPrefix(msg, "Arc error (HTTP "):
		// Preserve the HTTP status (already a category, not a detail) but drop
		// the server-supplied message body.
		end := strings.Index(msg, "):")
		if end > 0 {
			return msg[:end+1] + " query failed (see server logs for detail)"
		}
		return "Arc query failed (see server logs for detail)"
	default:
		return "Query failed (see server logs for detail)"
	}
}
