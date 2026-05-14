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

// MaxResponseBytes caps the size of a single Arc response body the plugin will
// read into memory. Set high enough for legitimate analytical queries (256 MiB)
// while preventing an unbounded SELECT * from OOMing the plugin process.
// A response that exceeds the cap fails fast with a clear error.
const MaxResponseBytes = 256 * 1024 * 1024

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

// safeDialContext wraps a net.Dialer so it refuses to connect to disallowed
// addresses. This is the SSRF guard for the user-supplied Arc URL.
//
// allowPrivate controls whether RFC1918/loopback/CGNAT addresses are permitted
// (intended for corporate-intranet Arc deployments — see
// allowPrivateForSettings). Link-local (including the cloud-metadata
// 169.254.169.254), multicast, and unspecified addresses are blocked
// regardless — they are never a legitimate Arc target.
//
// Resolution-then-validate avoids a TOCTOU between DNS rebind and connect: we
// resolve the host ourselves, drop any disallowed address, then dial the
// remaining ones explicitly by IP. If every resolved address is blocked the
// dial returns errBlockedAddr.
func safeDialContext(allowPrivate bool) func(ctx context.Context, network, addr string) (net.Conn, error) {
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
			if isBlockedIP(ip.IP, allowPrivate) {
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
// Conditionally-blocked when allowPrivate=false:
//   - loopback (127.0.0.0/8, ::1)
//   - private RFC1918 (10/8, 172.16/12, 192.168/16) + IPv6 ULA (fc00::/7)
//   - CGNAT (100.64.0.0/10)
func isBlockedIP(ip net.IP, allowPrivate bool) bool {
	if ip == nil {
		return true
	}
	// Unconditional blocks — these are never a real Arc deployment.
	if ip.IsUnspecified() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.IsMulticast() {
		return true
	}
	if allowPrivate {
		return false
	}
	if ip.IsLoopback() || ip.IsPrivate() {
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

// allowPrivateForSettings returns the dialer's private-IP policy for these
// settings. Private (RFC1918) addresses are permitted when either:
//   - the user explicitly opted in via AllowPrivateIPs (corporate intranet
//     deployments where Arc is on 10.x/192.168.x), or
//   - the configured Arc URL is itself loopback (dev workflow against
//     http://localhost:8000), since blocking loopback there would be useless
//     and confusing.
//
// Metadata addresses (169.254.169.254) and other link-local ranges are still
// blocked unconditionally — they are never a legitimate Arc target.
func allowPrivateForSettings(s *ArcInstanceSettings) bool {
	return s.settings.AllowPrivateIPs || isLoopbackURL(s.settings.URL)
}

// newHTTPClient builds a per-request http.Client that:
//   - refuses to connect to private/loopback/metadata addresses,
//   - validates redirects against the same blocklist,
//   - applies a request-level timeout.
//
// allowPrivate should be derived from datasource settings via
// allowPrivateForSettings. Link-local (incl. cloud-metadata) and unspecified
// addresses are blocked regardless.
func newHTTPClient(timeout time.Duration, allowPrivate bool) *http.Client {
	transport := &http.Transport{
		DialContext:           safeDialContext(allowPrivate),
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
	switch {
	case errors.Is(err, errBlockedAddr):
		return "Arc URL resolves to a blocked address (private/loopback). Update the datasource URL."
	case strings.Contains(msg, "context deadline exceeded"), strings.Contains(msg, "Client.Timeout"):
		return "Query timed out. Try reducing the time range, increasing the timeout, or enabling query splitting."
	case strings.Contains(msg, "connection refused"):
		return "Cannot connect to Arc — connection refused."
	case strings.Contains(msg, "no such host"):
		return "Cannot connect to Arc — hostname not found."
	case strings.Contains(msg, "response body exceeded"):
		return "Query result exceeded the configured size limit. Add LIMIT or narrow the time range."
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
