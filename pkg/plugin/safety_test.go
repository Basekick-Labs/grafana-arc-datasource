package plugin

import (
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
)

func TestValidateColumnArg(t *testing.T) {
	for _, tc := range []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"plain", "time", false},
		{"qualified", "events.time", false},
		{"underscored", "_time", false},
		{"camel", "createdAt", false},
		{"empty", "", true},
		{"space", "time col", true},
		{"injection", "time) OR 1=1 --", true},
		{"semicolon", "time;DROP", true},
		{"quote", "time'", true},
		{"paren", "time(x)", true},
		{"unicode", "τime", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateColumnArg(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateColumnArg(%q) error=%v wantErr=%v", tc.input, err, tc.wantErr)
			}
		})
	}
}

func TestValidateDatabaseName(t *testing.T) {
	for _, tc := range []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"plain", "production", false},
		{"underscored", "prod_db", false},
		{"hyphenated", "prod-db", false},
		{"numeric", "db123", false},
		{"empty", "", true},
		{"space", "prod db", true},
		{"crlf-injection", "prod\r\nX-Foo: bar", true},
		{"dot", "prod.db", true},
		{"slash", "prod/db", true},
		{"quote", "prod'db", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDatabaseName(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateDatabaseName(%q) error=%v wantErr=%v", tc.input, err, tc.wantErr)
			}
		})
	}
}

func TestValidateURL(t *testing.T) {
	for _, tc := range []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"http", "http://arc.example.com:8000", false},
		{"https", "https://arc.example.com", false},
		{"http-with-path", "http://arc.example.com/api", false},
		{"file", "file:///etc/passwd", true},
		{"gopher", "gopher://example.com", true},
		{"unix", "unix:///tmp/sock", true},
		{"no-scheme", "arc.example.com:8000", true},
		{"no-host", "http://", true},
		{"empty", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateURL(tc.input)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateURL(%q) error=%v wantErr=%v", tc.input, err, tc.wantErr)
			}
		})
	}
}

// TestIsBlockedIP_Strict locks in the default policy (allowPrivate=false):
// loopback, RFC1918, link-local, CGNAT, multicast, unspecified are all blocked.
func TestIsBlockedIP_Strict(t *testing.T) {
	for _, tc := range []struct {
		ip      string
		blocked bool
	}{
		// Should be blocked when allowPrivate=false
		{"127.0.0.1", true},      // loopback
		{"::1", true},            // loopback v6
		{"10.0.0.1", true},       // RFC1918
		{"172.16.0.1", true},     // RFC1918
		{"192.168.1.1", true},    // RFC1918
		{"169.254.169.254", true}, // AWS/GCP metadata, link-local
		{"100.64.0.1", true},     // CGNAT
		{"100.127.0.1", true},    // CGNAT end
		{"0.0.0.0", true},        // unspecified
		{"224.0.0.1", true},      // multicast
		{"fc00::1", true},        // ULA v6 (RFC4193, IsPrivate)
		{"fe80::1", true},        // link-local v6

		// Should NOT be blocked
		{"8.8.8.8", false},
		{"1.1.1.1", false},
		{"100.63.255.255", false}, // just below CGNAT
		{"100.128.0.1", false},    // just above CGNAT
		{"2001:4860:4860::8888", false},
	} {
		t.Run(tc.ip, func(t *testing.T) {
			ip := net.ParseIP(tc.ip)
			if ip == nil {
				t.Fatalf("invalid test IP %q", tc.ip)
			}
			got := isBlockedIP(ip, dialPolicy{})
			if got != tc.blocked {
				t.Errorf("isBlockedIP(%s, strict) = %v, want %v", tc.ip, got, tc.blocked)
			}
		})
	}
}

// TestIsBlockedIP_LoopbackOnly locks in gemini round-5 finding 3244943519:
// a loopback URL (e.g. `http://localhost:8000` in dev) unlocks loopback IPs
// but MUST NOT also unlock RFC1918. Previously these two were collapsed into
// one bool so a loopback URL opened RFC1918 redirects too.
func TestIsBlockedIP_LoopbackOnly(t *testing.T) {
	policy := dialPolicy{allowLoopback: true, allowPrivate: false}
	cases := []struct {
		ip      string
		blocked bool
	}{
		// Loopback: allowed when allowLoopback=true
		{"127.0.0.1", false},
		{"::1", false},

		// RFC1918 / CGNAT / ULA / link-local / metadata: STILL blocked
		// even when allowLoopback=true (the key new invariant)
		{"10.0.0.1", true},
		{"172.16.0.1", true},
		{"192.168.1.1", true},
		{"100.64.0.1", true},
		{"fc00::1", true},
		{"169.254.169.254", true},
		{"224.0.0.1", true},
		{"0.0.0.0", true},

		// Public addresses still allowed
		{"8.8.8.8", false},
	}
	for _, tc := range cases {
		t.Run(tc.ip, func(t *testing.T) {
			ip := net.ParseIP(tc.ip)
			if ip == nil {
				t.Fatalf("invalid test IP %q", tc.ip)
			}
			got := isBlockedIP(ip, policy)
			if got != tc.blocked {
				t.Errorf("isBlockedIP(%s, loopback-only) = %v, want %v", tc.ip, got, tc.blocked)
			}
		})
	}
}

// TestIsBlockedIP_Permissive locks in the corporate-intranet policy
// (allowPrivate=true): RFC1918/loopback/CGNAT are allowed, but link-local
// (incl. cloud-metadata 169.254.169.254) and multicast are STILL blocked.
// These last two are the must-never-allow signing-readiness invariants.
func TestIsBlockedIP_Permissive(t *testing.T) {
	for _, tc := range []struct {
		ip      string
		blocked bool
	}{
		// Allowed under permissive mode (corporate intranet)
		{"127.0.0.1", false},
		{"10.0.0.1", false},
		{"172.16.0.1", false},
		{"192.168.1.1", false},
		{"100.64.0.1", false},
		{"fc00::1", false},

		// STILL blocked even when allowPrivate=true — these are not legitimate
		// Arc targets regardless of network topology.
		{"169.254.169.254", true}, // cloud metadata — never legitimate
		{"fe80::1", true},         // link-local v6
		{"224.0.0.1", true},       // multicast
		{"0.0.0.0", true},         // unspecified

		// Public addresses obviously still allowed
		{"8.8.8.8", false},
	} {
		t.Run(tc.ip, func(t *testing.T) {
			ip := net.ParseIP(tc.ip)
			if ip == nil {
				t.Fatalf("invalid test IP %q", tc.ip)
			}
			got := isBlockedIP(ip, dialPolicy{allowPrivate: true})
			if got != tc.blocked {
				t.Errorf("isBlockedIP(%s, permissive) = %v, want %v", tc.ip, got, tc.blocked)
			}
		})
	}
}

func TestSafeDialContext_BlocksMetadataEvenWhenPermissive(t *testing.T) {
	// 169.254.169.254 is link-local — never a legitimate Arc target. Must be
	// blocked even with allowPrivate=true.
	dial := safeDialContext(dialPolicy{allowPrivate: true})
	_, err := dial(t.Context(), "tcp", "169.254.169.254:80")
	if err == nil {
		t.Fatal("expected dial to cloud metadata to be blocked even in permissive mode")
	}
	if !errors.Is(err, errBlockedAddr) {
		t.Fatalf("expected errBlockedAddr, got %v", err)
	}
}

func TestSafeDialContext_BlocksPrivateInStrictMode(t *testing.T) {
	dial := safeDialContext(dialPolicy{})
	// Use a clearly-private address that DNS won't fail to look up.
	_, err := dial(t.Context(), "tcp", "10.255.255.255:80")
	if err == nil {
		t.Fatal("expected dial to 10.x to be blocked in strict mode")
	}
	if !errors.Is(err, errBlockedAddr) {
		t.Fatalf("expected errBlockedAddr, got %v", err)
	}
}

func TestSafeDialContext_AllowsPrivateWhenPermitted(t *testing.T) {
	// We don't actually connect — just confirm the policy gate lets us through
	// to the system dialer (which then fails on connect-refused, which is fine).
	dial := safeDialContext(dialPolicy{allowPrivate: true})
	_, err := dial(t.Context(), "tcp", "127.0.0.1:1") // port 1 is reserved; connect-refused expected
	if err != nil && errors.Is(err, errBlockedAddr) {
		t.Fatalf("loopback should be allowed when permitted, got %v", err)
	}
}

func TestSanitizeUserError_StripsServerDetail(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		expect string
	}{
		{
			name:   "timeout",
			err:    errors.New("context deadline exceeded"),
			expect: "timed out",
		},
		{
			name:   "refused",
			err:    errors.New("dial tcp 1.2.3.4:80: connect: connection refused"),
			expect: "connection refused",
		},
		{
			name:   "blocked-addr",
			err:    errBlockedAddr,
			expect: "blocked address",
		},
		{
			name:   "arc-http-error",
			err:    errors.New("Arc error (HTTP 500): Catalog Error: Table with name 'secret_prices' does not exist!"),
			expect: "see server logs",
		},
		{
			// Real *http.MaxBytesError so the typed-error branch fires
			// (R2-CR7 — substring match was replaced with errors.As).
			name:   "size-cap",
			err:    &http.MaxBytesError{Limit: 1024 * 1024 * 1024}, // 1 GiB
			expect: "1024 MiB",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeUserError("A", tc.err)
			if !strings.Contains(got, tc.expect) {
				t.Errorf("sanitizeUserError(%v) = %q, want substring %q", tc.err, got, tc.expect)
			}
			// Critical: the user-facing message must NOT contain the secret detail
			// from arc-http-error (Catalog Error or table name).
			if strings.Contains(got, "secret_prices") || strings.Contains(got, "Catalog Error") {
				t.Errorf("sanitizeUserError leaked server detail: %q", got)
			}
		})
	}
}

func TestIsLoopbackURL(t *testing.T) {
	for _, tc := range []struct {
		input    string
		loopback bool
	}{
		{"http://localhost", true},
		{"http://localhost:8000", true},
		{"http://127.0.0.1:8000", true},
		{"http://[::1]:8000", true},
		{"http://arc.example.com", false},
		{"http://8.8.8.8", false},
		{"", false},
		{"not a url", false},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got := isLoopbackURL(tc.input)
			if got != tc.loopback {
				t.Errorf("isLoopbackURL(%q) = %v, want %v", tc.input, got, tc.loopback)
			}
		})
	}
}
