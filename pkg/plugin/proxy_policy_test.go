package plugin

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"
)

// recordingProxy is a forward proxy that records every target it is asked
// for. For a plain-HTTP request it answers itself (200, or a 302 to
// redirectTo when set) instead of fetching the target; for CONNECT it answers
// 403, so an https request that reaches it fails there.
type recordingProxy struct {
	mu         sync.Mutex
	targets    []string
	redirectTo string
}

func (p *recordingProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	if r.Method == http.MethodConnect {
		p.targets = append(p.targets, "CONNECT "+r.Host)
	} else {
		p.targets = append(p.targets, r.URL.String())
	}
	redirect := p.redirectTo
	p.mu.Unlock()
	if r.Method == http.MethodConnect {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if redirect != "" && r.URL.Path == "/start" {
		http.Redirect(w, r, redirect, http.StatusFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (p *recordingProxy) seen() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.targets...)
}

func newProxiedClient(t *testing.T, policy dialPolicy, rp *recordingProxy) *http.Client {
	t.Helper()
	srv := httptest.NewServer(rp)
	t.Cleanup(srv.Close)
	proxyURL, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return newHTTPClientWithProxy(5*time.Second, policy, 4, http.ProxyURL(proxyURL))
}

// The proxy listens on loopback, so every policy below permits loopback (via
// allowPrivate or allowLoopback); otherwise the dial to the proxy itself is
// refused and the tests would pass for the wrong reason.
func TestProxy_DestinationPolicyIsEnforced(t *testing.T) {
	cases := []struct {
		name    string
		policy  dialPolicy
		target  string
		blocked bool
	}{
		{"cloud metadata over http", dialPolicy{allowPrivate: true}, "http://169.254.169.254/latest/meta-data/", true},
		{"cloud metadata over https (CONNECT)", dialPolicy{allowPrivate: true}, "https://169.254.169.254/", true},
		{"IPv6 link-local", dialPolicy{allowPrivate: true}, "http://[fe80::1]/", true},
		{"unspecified address", dialPolicy{allowPrivate: true}, "http://0.0.0.0:8000/", true},
		{"RFC1918 without AllowPrivateIPs", dialPolicy{allowLoopback: true}, "http://10.0.0.5:8000/health", true},
		// Spellings getaddrinfo (and so many proxies) reads as 169.254.169.254
		// while Go's resolver, without cgo, does not resolve them at all.
		{"metadata as one decimal number", dialPolicy{allowPrivate: true}, "http://2852039166/", true},
		{"metadata as hex", dialPolicy{allowPrivate: true}, "http://0xa9fea9fe/", true},
		{"metadata as octal dotted", dialPolicy{allowPrivate: true}, "http://0251.0376.0251.0376/", true},
		{"metadata as a.b.c with a 16-bit tail", dialPolicy{allowPrivate: true}, "http://169.254.43518/", true},
		{"metadata with a trailing dot", dialPolicy{allowPrivate: true}, "http://169.254.169.254./", true},
		{"public address with a trailing dot", dialPolicy{allowPrivate: true}, "http://203.0.113.10./health", false},
		{"name with a hex-looking first label", dialPolicy{allowPrivate: true}, "http://0xa9fea9fe.example.invalid/", false},
		{"RFC1918 with AllowPrivateIPs", dialPolicy{allowPrivate: true}, "http://10.0.0.5:8000/health", false},
		{"public address", dialPolicy{allowPrivate: true}, "http://203.0.113.10:8000/health", false},
		// .invalid never resolves (RFC 6761): stands in for a name only the
		// proxy can resolve, which must still be proxied.
		{"name resolvable only at the proxy", dialPolicy{allowPrivate: true}, "http://arc.example.invalid:8000/health", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rp := &recordingProxy{}
			if !tc.policy.allowPrivate && !tc.policy.allowLoopback {
				t.Fatal("test bug: the proxy listens on loopback, so the policy must allow loopback")
			}
			c := newProxiedClient(t, tc.policy, rp)
			resp, err := c.Get(tc.target)
			if resp != nil {
				_ = resp.Body.Close()
			}
			seen := rp.seen()
			if tc.blocked {
				if !errors.Is(err, errBlockedAddr) {
					t.Fatalf("err = %v, want errBlockedAddr", err)
				}
				if len(seen) != 0 {
					t.Fatalf("blocked destination reached the proxy: %v", seen)
				}
				return
			}
			if err != nil {
				t.Fatalf("allowed destination failed: %v", err)
			}
			if len(seen) != 1 {
				t.Fatalf("proxy saw %v, want exactly the one request", seen)
			}
		})
	}
}

// A redirect to the metadata address is a new request, so it must be checked
// even though the first hop was allowed and went through the proxy.
func TestProxy_RedirectToBlockedAddressIsRefused(t *testing.T) {
	rp := &recordingProxy{redirectTo: "http://169.254.169.254/latest/meta-data/"}
	c := newProxiedClient(t, dialPolicy{allowPrivate: true}, rp)
	resp, err := c.Get("http://203.0.113.10:8000/start")
	if resp != nil {
		_ = resp.Body.Close()
	}
	if !errors.Is(err, errBlockedAddr) {
		t.Fatalf("err = %v, want errBlockedAddr", err)
	}
	if seen := rp.seen(); len(seen) != 1 || seen[0] != "http://203.0.113.10:8000/start" {
		t.Fatalf("proxy saw %v, want only the first hop", seen)
	}
}

// A name that resolves to loopback is refused when the configured URL is not
// loopback. Checked on the selector: a client test would need a policy that
// allows loopback to reach the proxy, which would also allow this target.
func TestProxy_LoopbackNameRefusedForNonLoopbackURL(t *testing.T) {
	sel := proxyCheckingDestination(http.ProxyURL(&url.URL{Scheme: "http", Host: "203.0.113.1:3128"}), dialPolicy{})
	req, _ := http.NewRequest(http.MethodGet, "http://localhost:8000/", nil)
	if _, err := sel(req); !errors.Is(err, errBlockedAddr) {
		t.Fatalf("err = %v, want errBlockedAddr", err)
	}
}

// No proxy selected (unset, NO_PROXY match): the selector must not get in the
// way, and the dial-time check is what applies.
func TestProxy_NoProxySelectedIsUnchanged(t *testing.T) {
	sel := proxyCheckingDestination(func(*http.Request) (*url.URL, error) { return nil, nil }, dialPolicy{})
	req, _ := http.NewRequest(http.MethodGet, "http://169.254.169.254/", nil)
	u, err := sel(req)
	if u != nil || err != nil {
		t.Fatalf("got (%v, %v), want (nil, nil)", u, err)
	}
	if proxyCheckingDestination(nil, dialPolicy{}) != nil {
		t.Fatal("a nil selector must stay nil")
	}
}

// Production wiring: newHTTPClient must keep honouring the environment proxy
// (restored in 1.4.0), through the checking wrapper.
func TestProxy_ProductionClientHasAProxySelector(t *testing.T) {
	c := newHTTPClient(time.Second, dialPolicy{}, 1)
	tr, ok := c.Transport.(*http.Transport)
	if !ok || tr.Proxy == nil {
		t.Fatal("newHTTPClient dropped the environment proxy")
	}
}
