package hermetic

import (
	"bytes"
	"net/http"
	"os"
	"strings"
	"testing"
)

// TestEnvStripsAmbientCredentials: every variable that could name a real
// principal or redirect the shared config is dropped, and the fakes are set.
func TestEnvStripsAmbientCredentials(t *testing.T) {
	base := []string{
		"AWS_PROFILE=real", "AWS_ACCESS_KEY_ID=AKIAREAL", "aws_session_token=x", "SPORE_PROFILE=real",
		"XDG_CONFIG_HOME=/home/me/.config", "HTTPS_PROXY=http://corp:3128", "NO_PROXY=*", "PATH=/usr/bin",
	}
	got := map[string]string{}
	for _, kv := range Env(base, "http://127.0.0.1:1") {
		k, v, _ := strings.Cut(kv, "=")
		if _, dup := got[k]; dup {
			t.Errorf("%s set twice", k)
		}
		got[k] = v
	}
	for k, want := range map[string]string{
		"AWS_ACCESS_KEY_ID": FakeAccessKeyID, "AWS_CONFIG_FILE": "/dev/null", "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
		"AWS_EC2_METADATA_DISABLED": "true", "AWS_ENDPOINT_URL": "http://127.0.0.1:1", "XDG_CONFIG_HOME": "/dev/null",
		"HTTPS_PROXY": "http://127.0.0.1:1", "NO_PROXY": loopback, "PATH": "/usr/bin", EnvMarker: "1",
	} {
		if got[k] != want {
			t.Errorf("%s = %q, want %q", k, got[k], want)
		}
	}
	for _, k := range []string{"AWS_PROFILE", "aws_session_token", "SPORE_PROFILE"} {
		if _, ok := got[k]; ok {
			t.Errorf("%s survived", k)
		}
	}
}

// TestRecorderRefusesAndReports: a request is refused with 403, recorded with
// its AWS operation, and reported; an idle recorder reports nothing.
func TestRecorderRefusesAndReports(t *testing.T) {
	rec, err := Start()
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Close()

	var idle bytes.Buffer
	if rec.Report(&idle, "idle") || idle.Len() != 0 {
		t.Fatalf("idle recorder reported: %q", idle.String())
	}

	req, _ := http.NewRequest(http.MethodPost, rec.URL+"/", strings.NewReader("Action=GetCallerIdentity&Version=2011-06-15"))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKID/20260101/us-east-1/sts/aws4_request, SignedHeaders=host, Signature=x")
	req.Header.Set("X-Amz-Target", "X.Op")
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403", resp.StatusCode)
	}

	var out bytes.Buffer
	if !rec.Report(&out, "probe.test") {
		t.Fatal("Report returned false after a request")
	}
	for _, want := range []string{"HERMETIC GATE FAILED (#214): probe.test made 1", "service=sts", "target=X.Op", "action=GetCallerIdentity"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("report lacks %q:\n%s", want, out.String())
		}
	}
}

// TestDescribeConnect names the target of a proxied HTTPS request.
func TestDescribeConnect(t *testing.T) {
	if got := describe(&http.Request{Method: http.MethodConnect, Host: "example.com:443"}); got != "HTTPS CONNECT example.com:443" {
		t.Errorf("describe = %q", got)
	}
}

// TestInstallAndTake is the half a consumer's own TestMain uses (#266):
// Install strips the ambient credentials from this process and sets the fakes,
// a variable set after it survives, and Take hands back (and forgets) a request
// the test made on purpose, so Report has nothing left to fail on.
func TestInstallAndTake(t *testing.T) {
	saved := os.Environ()
	t.Cleanup(func() {
		os.Clearenv()
		for _, kv := range saved {
			k, v, _ := strings.Cut(kv, "=")
			_ = os.Setenv(k, v)
		}
	})
	_ = os.Setenv("AWS_PROFILE", "real")
	_ = os.Setenv("SPORE_PROFILE", "real")

	rec, err := Install()
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Close()
	for _, k := range []string{"AWS_PROFILE", "SPORE_PROFILE"} {
		if v, ok := os.LookupEnv(k); ok {
			t.Errorf("%s = %q survived Install", k, v)
		}
	}
	if got := os.Getenv("AWS_ACCESS_KEY_ID"); got != FakeAccessKeyID {
		t.Errorf("AWS_ACCESS_KEY_ID = %q, want %q", got, FakeAccessKeyID)
	}
	if got := os.Getenv("AWS_ENDPOINT_URL"); got != rec.URL {
		t.Errorf("AWS_ENDPOINT_URL = %q, want the recorder %q", got, rec.URL)
	}
	if os.Getenv(EnvMarker) != "1" {
		t.Errorf("%s not set", EnvMarker)
	}

	resp, err := http.Post(rec.URL+"/", "application/x-www-form-urlencoded", strings.NewReader("Action=GetCallerIdentity"))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if got := rec.Take(); len(got) != 1 || !strings.Contains(got[0], "action=GetCallerIdentity") {
		t.Fatalf("Take = %v, want the one GetCallerIdentity request", got)
	}
	if got := rec.Take(); len(got) != 0 {
		t.Errorf("second Take = %v, want nothing", got)
	}
	var out bytes.Buffer
	if rec.Report(&out, "probe.test") {
		t.Errorf("Report after Take reported: %q", out.String())
	}
}
