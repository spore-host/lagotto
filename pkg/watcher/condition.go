package watcher

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Condition is a goal-driven watch's completion check (#70). The fleet
// supervisor evaluates it each poll cycle; when Done returns true the watch
// retires (StatusCompleted) and stops launching. Done should be cheap and
// side-effect-free — it's polled repeatedly.
type Condition interface {
	Done(ctx context.Context) (bool, error)
	// String returns the original spec, for display/logging.
	String() string
}

// ParseCondition parses a --until spec into a Condition. Supported forms:
//
//	s3-empty: s3://bucket/manifest-key minus s3://bucket/done-prefix/
//	http-200: https://host/path
//	shell:    <command>            (CLI daemon only — see AllowShell)
//
// The prefix before the first ':' selects the kind; the remainder is the
// kind-specific argument (leading space trimmed). s3Client may be nil when only
// http/shell specs are expected (it's required to evaluate an s3-empty spec).
func ParseCondition(spec string, s3Client S3Lister) (Condition, error) {
	kind, arg, ok := strings.Cut(spec, ":")
	if !ok {
		return nil, fmt.Errorf("invalid --until %q: want '<kind>: <arg>' (s3-empty, http-200, or shell)", spec)
	}
	kind = strings.TrimSpace(strings.ToLower(kind))
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, fmt.Errorf("invalid --until %q: missing argument after %q", spec, kind)
	}

	switch kind {
	case "s3-empty":
		return parseS3Empty(spec, arg, s3Client)
	case "http-200":
		return &httpCondition{spec: spec, url: arg}, nil
	case "shell":
		return &shellCondition{spec: spec, command: arg}, nil
	default:
		return nil, fmt.Errorf("invalid --until %q: unknown kind %q (want s3-empty, http-200, or shell)", spec, kind)
	}
}

// IsShellCondition reports whether spec is a shell condition. The hosted Lambda
// poller has no shell/sandbox, so it refuses shell conditions; the CLI daemon
// allows them. Used to gate at watch-create and poll time.
func IsShellCondition(spec string) bool {
	kind, _, ok := strings.Cut(spec, ":")
	return ok && strings.EqualFold(strings.TrimSpace(kind), "shell")
}

// S3Lister is the slice of the S3 API the s3-empty condition needs — an
// interface so tests inject a fake without real AWS. *s3.Client satisfies it.
type S3Lister interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// s3EmptyCondition is Done when (objects under the manifest location) minus
// (objects under the done prefix) is empty — i.e. every wanted item is done.
// Both sides are counted by S3 key prefix; "manifest" here is itself a prefix
// (the set of wanted keys), matching the pull-model completion state (#70).
type s3EmptyCondition struct {
	spec                   string
	client                 S3Lister
	wantBucket, wantPrefix string
	doneBucket, donePrefix string
}

func parseS3Empty(spec, arg string, client S3Lister) (Condition, error) {
	// arg form: "s3://wantBucket/wantPrefix minus s3://doneBucket/donePrefix"
	left, right, ok := strings.Cut(arg, " minus ")
	if !ok {
		return nil, fmt.Errorf("invalid s3-empty spec %q: want 's3://…/wanted minus s3://…/done'", spec)
	}
	wb, wp, err := parseS3URI(strings.TrimSpace(left))
	if err != nil {
		return nil, fmt.Errorf("s3-empty wanted: %w", err)
	}
	db, dp, err := parseS3URI(strings.TrimSpace(right))
	if err != nil {
		return nil, fmt.Errorf("s3-empty done: %w", err)
	}
	if client == nil {
		return nil, fmt.Errorf("s3-empty condition needs an S3 client")
	}
	return &s3EmptyCondition{
		spec: spec, client: client,
		wantBucket: wb, wantPrefix: wp,
		doneBucket: db, donePrefix: dp,
	}, nil
}

func (c *s3EmptyCondition) String() string { return c.spec }

func (c *s3EmptyCondition) Done(ctx context.Context) (bool, error) {
	want, err := c.count(ctx, c.wantBucket, c.wantPrefix)
	if err != nil {
		return false, fmt.Errorf("count wanted: %w", err)
	}
	done, err := c.count(ctx, c.doneBucket, c.donePrefix)
	if err != nil {
		return false, fmt.Errorf("count done: %w", err)
	}
	// Done when nothing remains: every wanted key has a corresponding done key.
	return done >= want, nil
}

// count returns the number of objects under bucket/prefix, paginating.
func (c *s3EmptyCondition) count(ctx context.Context, bucket, prefix string) (int, error) {
	var n int
	var token *string
	for {
		out, err := c.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return 0, err
		}
		n += int(aws.ToInt32(out.KeyCount))
		if out.IsTruncated == nil || !*out.IsTruncated || out.NextContinuationToken == nil {
			break
		}
		token = out.NextContinuationToken
	}
	return n, nil
}

// parseS3URI splits "s3://bucket/key-or-prefix" into (bucket, key). A missing
// key is allowed (bucket-root prefix).
func parseS3URI(uri string) (bucket, key string, err error) {
	rest, ok := strings.CutPrefix(uri, "s3://")
	if !ok {
		return "", "", fmt.Errorf("invalid S3 URI %q: want s3://bucket/key", uri)
	}
	bucket, key, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("invalid S3 URI %q: missing bucket", uri)
	}
	return bucket, key, nil
}

// httpCondition is Done when a GET to the URL returns a 2xx status.
type httpCondition struct {
	spec string
	url  string
}

func (c *httpCondition) String() string { return c.spec }

func (c *httpCondition) Done(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return false, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode >= 200 && resp.StatusCode < 300, nil
}

// shellCondition is Done when the command exits 0. CLI-daemon only — the hosted
// Lambda poller refuses shell conditions (no shell/sandbox); see IsShellCondition.
type shellCondition struct {
	spec    string
	command string
}

func (c *shellCondition) String() string { return c.spec }

func (c *shellCondition) Done(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	// #nosec G204 -- the command is an operator-supplied --until spec on their own
	// machine (CLI daemon only), equivalent to what they'd type in a shell.
	cmd := exec.CommandContext(ctx, "sh", "-c", c.command)
	if err := cmd.Run(); err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return false, nil // non-zero exit = not done (not an evaluator error)
		}
		return false, err // couldn't run the command at all
	}
	return true, nil
}
