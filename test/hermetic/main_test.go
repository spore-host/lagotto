package main

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/spore-host/lagotto/internal/hermetic"
)

// helperEnv selects what this test binary does when the wrapper under test runs
// it as the "test binary": reach out the way a leaking test would, or not.
const helperEnv = "LAGOTTO_HERMETIC_TEST_HELPER"

func TestHelperProcess(t *testing.T) {
	ctx := context.Background()
	switch os.Getenv(helperEnv) {
	case "":
		t.Skip("helper for TestRun_*; does nothing on its own")
	case "quiet":
		// Ambient credentials, profile and spore config must be gone and fakes
		// in their place.
		if os.Getenv("AWS_PROFILE") != "" || os.Getenv("SPORE_PROFILE") != "" ||
			os.Getenv("AWS_ACCESS_KEY_ID") != hermetic.FakeAccessKeyID ||
			os.Getenv("AWS_CONFIG_FILE") != "/dev/null" || os.Getenv("XDG_CONFIG_HOME") != "/dev/null" {
			os.Exit(3)
		}
	case "sdk":
		// The #214 leak: runWatch's EnsureTables + PutWatch, an SDK client from
		// the ambient default chain.
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-west-2"))
		if err != nil {
			os.Exit(4)
		}
		_, _ = dynamodb.NewFromConfig(cfg).PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("lagotto-watches"),
			Item:      map[string]types.AttributeValue{"watch_id": &types.AttributeValueMemberS{Value: "w-leak"}},
		})
	case "https":
		// A plain net/http request (a webhook notifier, say): not an SDK
		// client, so AWS_ENDPOINT_URL alone would not catch it.
		resp, err := http.Post("https://hooks.example.com/lagotto", "application/json", strings.NewReader("{}"))
		if err == nil {
			_ = resp.Body.Close()
		}
	}
}

func runHelper(t *testing.T, mode string) (int, string) {
	t.Helper()
	t.Setenv(helperEnv, mode)
	t.Setenv("AWS_PROFILE", "would-be-real") // must not reach the child
	t.Setenv("SPORE_PROFILE", "would-be-real")
	var out bytes.Buffer
	code := run(os.Args[0], []string{"-test.run=^TestHelperProcess$"}, &out, &out)
	return code, out.String()
}

// The wrapper must FAIL a test binary that reaches the network, and name the
// request — for the SDK shape of the #214 leak and for plain HTTPS.
func TestRun_FailsATestBinaryThatReachesTheNetwork(t *testing.T) {
	for mode, want := range map[string]string{
		"sdk":   "target=DynamoDB_20120810.PutItem",
		"https": "HTTPS CONNECT hooks.example.com:443",
	} {
		t.Run(mode, func(t *testing.T) {
			code, out := runHelper(t, mode)
			if code != 1 {
				t.Errorf("exit = %d, want 1 for a binary that reached the network\n%s", code, out)
			}
			if !strings.Contains(out, "HERMETIC GATE FAILED") || !strings.Contains(out, want) {
				t.Errorf("failure does not name the request %q:\n%s", want, out)
			}
		})
	}
}

// ...and must not fail one that stays local, or it is noise, not a gate.
func TestRun_PassesAQuietTestBinary(t *testing.T) {
	if code, out := runHelper(t, "quiet"); code != 0 {
		t.Errorf("exit = %d, want 0 for a binary that made no request\n%s", code, out)
	}
}

// The wrapper only protects the runs that use it. Every `go test` CI runs that
// is not the integration build tag (which exists to reach real AWS) must go
// through it, and so must `make test`, for both modules.
func TestGateIsWired(t *testing.T) {
	root := filepath.Join("..", "..")
	read := func(rel string) string {
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	// Discovered: every `go test` command line in CI, single-line `run:` or
	// inside a `run: |` block.
	ci := read(".github/workflows/ci.yml")
	runs := regexp.MustCompile(`(?m)^\s*(?:run:\s*)?(go test\b.*)$`).FindAllStringSubmatch(ci, -1)
	if len(runs) < 2 {
		t.Fatalf("found %d `go test` lines in ci.yml, want the root and lambda modules' — the discovery is broken, not the workflow clean", len(runs))
	}
	for _, m := range runs {
		if strings.Contains(m[1], "-tags") {
			continue
		}
		if !strings.Contains(m[1], `-exec "$RUNNER_TEMP/hermetic-exec"`) {
			t.Errorf("ci.yml runs %q outside the hermetic wrapper (#214)", m[1])
		}
	}
	if !regexp.MustCompile(`(?m)^\s*run:\s*go build -o "\$RUNNER_TEMP/hermetic-exec" \./test/hermetic$`).MatchString(ci) {
		t.Error("ci.yml does not build the hermetic wrapper it runs tests under")
	}
	for _, kv := range []string{"AWS_CONFIG_FILE: /dev/null", "AWS_SHARED_CREDENTIALS_FILE: /dev/null"} {
		if !strings.Contains(ci, kv) {
			t.Errorf("ci.yml's test job does not set %s: CI must run with no AWS config (#214)", kv)
		}
	}

	mk := read("Makefile")
	test := regexp.MustCompile(`(?ms)^test:[^\n]*\n(.*?)\n\n`).FindStringSubmatch(mk + "\n\n")
	if test == nil {
		t.Fatal("Makefile has no test target")
	}
	lines := regexp.MustCompile(`(?m)^\t.*\$\(GOTEST\).*$`).FindAllString(test[1], -1)
	if len(lines) < 2 {
		t.Fatalf("Makefile test target runs %d $(GOTEST) lines, want one per module (root and lambda/capacity-poller)", len(lines))
	}
	for _, l := range lines {
		if !strings.Contains(l, "-exec $(HERMETIC_EXEC)") {
			t.Errorf("Makefile test runs %q outside the hermetic wrapper (#214)", strings.TrimSpace(l))
		}
	}
	if !strings.Contains(test[1], "cd lambda/capacity-poller") {
		t.Error("Makefile test does not test the lambda/capacity-poller module")
	}
}
