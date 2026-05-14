package plugin

import (
	"errors"
	"net"
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

func TestIsBlockedIP(t *testing.T) {
	for _, tc := range []struct {
		ip      string
		blocked bool
	}{
		// Should be blocked
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
			got := isBlockedIP(ip)
			if got != tc.blocked {
				t.Errorf("isBlockedIP(%s) = %v, want %v", tc.ip, got, tc.blocked)
			}
		})
	}
}

func TestSafeDialContext_BlocksPrivate(t *testing.T) {
	dial := safeDialContext(false)
	_, err := dial(t.Context(), "tcp", "169.254.169.254:80")
	if err == nil {
		t.Fatal("expected dial to 169.254.169.254 to be blocked")
	}
	if !errors.Is(err, errBlockedAddr) {
		t.Fatalf("expected errBlockedAddr, got %v", err)
	}
}

func TestSafeDialContext_AllowsLoopbackWhenPermitted(t *testing.T) {
	// We don't actually connect — just confirm that the loopback policy gate
	// lets us through to the dialer (which then fails on connect-refused, which
	// is fine — we're not running a server).
	dial := safeDialContext(true)
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
			name:   "size-cap",
			err:    errors.New("http: response body exceeded the limit"),
			expect: "size limit",
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
