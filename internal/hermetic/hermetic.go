// Package hermetic makes a lagotto unit test that reaches AWS (or any host off
// this machine) FAIL instead of quietly writing to whatever account the
// developer's environment points at (#214).
//
// On 2026-11-01 a cmd test called runWatch against a revert. Validation passed,
// so it went on to build a real AWS config from the ambient AWS_PROFILE and ran
// EnsureTables + PutWatch: a real watch record appeared in a live account. In CI,
// which has no credentials, the same calls would simply have failed, so nothing
// there could have noticed.
//
// The environment this package installs (ported from spawn's internal/hermetic,
// spawn#968):
//
//   - strips every AWS_* and SPORE_* variable and sets obviously fake static
//     credentials, AWS_CONFIG_FILE and AWS_SHARED_CREDENTIALS_FILE to /dev/null,
//     AWS_EC2_METADATA_DISABLED, and XDG_CONFIG_HOME to /dev/null (so the shared
//     spore.host config file, which can name a profile, is unreadable) — nothing
//     can authenticate as a real principal even if a request escapes;
//   - points AWS_ENDPOINT_URL at a loopback recorder, which catches every SDK
//     client built by config.LoadDefaultConfig, whatever HTTP client it uses;
//   - points HTTP_PROXY/HTTPS_PROXY at the same recorder, with NO_PROXY reduced
//     to loopback, which catches clients built from a bare aws.Config (no
//     endpoint resolution from the environment), IMDS, and child processes.
//     Go never proxies loopback, so httptest servers and the Substrate emulator
//     keep working.
//
// Any request the recorder sees is refused (HTTP 403) and fails the run, named.
//
// Two entry points share it: test/hermetic, a `go test -exec` wrapper that
// `make test` and CI run every test under (it also covers package init, which
// runs before TestMain), and Main, the TestMain of every lagotto test package
// that can reach an AWS config loader, so that even a plain `go test` is
// hermetic. internal/hermetic's gate test discovers those packages and fails
// if one lacks the TestMain.
package hermetic

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
)

// EnvMarker is set to 1 in a hermetic environment.
const EnvMarker = "LAGOTTO_HERMETIC_TEST"

// FakeAccessKeyID is the access key every hermetic environment carries.
const FakeAccessKeyID = "hermetic-test-key-id"

const loopback = "localhost,127.0.0.1,::1"

// Env returns base with every AWS_*, SPORE_*, XDG_CONFIG_HOME and proxy variable
// removed and the hermetic settings applied. recorderURL is the loopback
// recorder's base URL.
func Env(base []string, recorderURL string) []string {
	var env []string
	for _, kv := range base {
		k, _, _ := strings.Cut(kv, "=")
		if !dropped(k) {
			env = append(env, kv)
		}
	}
	return append(env, settings(recorderURL)...)
}

func dropped(k string) bool {
	u := strings.ToUpper(k)
	return strings.HasPrefix(u, "AWS_") || strings.HasPrefix(u, "SPORE_") ||
		u == "XDG_CONFIG_HOME" ||
		u == "HTTP_PROXY" || u == "HTTPS_PROXY" || u == "ALL_PROXY" || u == "NO_PROXY"
}

func settings(recorderURL string) []string {
	return []string{
		"AWS_ACCESS_KEY_ID=" + FakeAccessKeyID,
		"AWS_SECRET_ACCESS_KEY=hermetic-test-not-a-real-secret",
		"AWS_CONFIG_FILE=/dev/null",
		"AWS_SHARED_CREDENTIALS_FILE=/dev/null",
		"AWS_EC2_METADATA_DISABLED=true",
		"AWS_ENDPOINT_URL=" + recorderURL,
		// The shared spore.host config (~/.config/spore/config.toml) can name a
		// profile; /dev/null/spore/config.toml is unreadable, which sporeconfig
		// tolerates as "no file layer".
		"XDG_CONFIG_HOME=/dev/null",
		"HTTP_PROXY=" + recorderURL,
		"HTTPS_PROXY=" + recorderURL,
		"http_proxy=" + recorderURL,
		"https_proxy=" + recorderURL,
		"NO_PROXY=" + loopback,
		"no_proxy=" + loopback,
		EnvMarker + "=1",
	}
}

// Recorder is a loopback HTTP server that records and refuses every request.
type Recorder struct {
	URL string

	srv  *http.Server
	mu   sync.Mutex
	seen []string
}

// Start listens on a loopback port.
func Start() (*Recorder, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	r := &Recorder{URL: "http://" + ln.Addr().String()}
	r.srv = &http.Server{Handler: r}
	go func() { _ = r.srv.Serve(ln) }()
	return r, nil
}

// Close stops the server.
func (r *Recorder) Close() { _ = r.srv.Close() }

var (
	// Credential=AKID/20260101/us-west-2/dynamodb/aws4_request -> dynamodb
	sigV4Service = regexp.MustCompile(`Credential=[^/]+/\d+/[^/]+/([^/]+)/aws4_request`)
	queryAction  = regexp.MustCompile(`(?:^|&)Action=([A-Za-z0-9]+)`)
)

func (r *Recorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	d := describe(req)
	r.mu.Lock()
	r.seen = append(r.seen, d)
	r.mu.Unlock()
	http.Error(w, "lagotto hermetic test environment: network access refused (#214)", http.StatusForbidden)
}

// describe renders a request so the failure says what was attempted: the
// CONNECT target for proxied HTTPS, else the URL plus the AWS service and
// operation when they can be read off the request.
func describe(req *http.Request) string {
	if req.Method == http.MethodConnect {
		return "HTTPS CONNECT " + req.Host
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s", req.Method, req.URL.String())
	if req.URL.Host == "" {
		b.WriteString(" (via AWS_ENDPOINT_URL)")
	}
	if m := sigV4Service.FindStringSubmatch(req.Header.Get("Authorization")); m != nil {
		fmt.Fprintf(&b, " service=%s", m[1])
	}
	if t := req.Header.Get("X-Amz-Target"); t != "" {
		fmt.Fprintf(&b, " target=%s", t)
	}
	if req.Body != nil {
		body, _ := io.ReadAll(io.LimitReader(req.Body, 4096))
		if m := queryAction.FindStringSubmatch(string(body)); m != nil {
			fmt.Fprintf(&b, " action=%s", m[1])
		}
	}
	return b.String()
}

// Requests returns what the recorder has seen, sorted.
func (r *Recorder) Requests() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := append([]string(nil), r.seen...)
	sort.Strings(out)
	return out
}

// Report writes the failure for a run that reached the recorder and reports
// whether there was anything to report.
func (r *Recorder) Report(w io.Writer, who string) bool {
	reqs := r.Requests()
	if len(reqs) == 0 {
		return false
	}
	_, _ = fmt.Fprintf(w, "\nHERMETIC GATE FAILED (#214): %s made %d network request(s) that would have left this machine.\n", who, len(reqs))
	_, _ = fmt.Fprintln(w, "A unit test must not reach AWS: with real credentials these are real control-plane calls")
	_, _ = fmt.Fprintln(w, "(on 2026-11-01 one wrote a real watch to a live account). Inject a fake (awscfg.SetLoaderForTest,")
	_, _ = fmt.Fprintln(w, "the package's client interfaces), or point the client at an httptest server or the Substrate emulator.")
	for _, q := range reqs {
		_, _ = fmt.Fprintf(w, "  - %s\n", q)
	}
	return true
}

// Main is a TestMain body: it installs the hermetic environment in this process,
// runs the tests, and fails the binary if any of them reached the recorder.
//
//	func TestMain(m *testing.M) { hermetic.Main(m) }
func Main(m *testing.M) {
	os.Exit(Run(m))
}

// Run is Main without the os.Exit, for a TestMain that has its own setup.
func Run(m *testing.M) int {
	rec, err := Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "hermetic: %v\n", err)
		return 2
	}
	defer rec.Close()
	for _, kv := range os.Environ() {
		if k, _, _ := strings.Cut(kv, "="); dropped(k) {
			_ = os.Unsetenv(k)
		}
	}
	for _, kv := range settings(rec.URL) {
		k, v, _ := strings.Cut(kv, "=")
		_ = os.Setenv(k, v)
	}
	code := m.Run()
	if rec.Report(os.Stderr, "this test binary") {
		return 1
	}
	return code
}
