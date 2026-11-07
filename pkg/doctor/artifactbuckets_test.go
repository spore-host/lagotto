package doctor

import (
	"errors"
	"strings"
	"testing"
)

func TestEvalArtifactBuckets(t *testing.T) {
	if _, ok := EvalArtifactBuckets(nil, nil); ok {
		t.Error("no buckets should say nothing")
	}

	c, ok := EvalArtifactBuckets(nil, errors.New("AccessDenied"))
	if !ok || c.Status != StatusWarn || !strings.Contains(c.Summary, "s3:ListAllMyBuckets") {
		t.Errorf("list error: %+v", c)
	}

	c, _ = EvalArtifactBuckets([]ArtifactBucket{{Region: "us-west-2", Name: "b-us-west-2", Poller: true}}, nil)
	if c.Status != StatusPass {
		t.Errorf("all with pollers: %+v", c)
	}

	c, _ = EvalArtifactBuckets([]ArtifactBucket{
		{Region: "us-west-2", Name: "b-us-west-2", Poller: true},
		{Region: "ca-central-1", Name: "b-ca", Managed: true},
		{Region: "eu-west-1", Name: "b-eu"},
		{Region: "ap-south-1", Name: "b-ap", Err: errors.New("throttled")},
	}, nil)
	if c.Status != StatusWarn || c.Name != CheckArtifactBuckets {
		t.Fatalf("orphans must WARN, never FAIL: %+v", c)
	}
	if !strings.Contains(c.Summary, "2 artifact bucket(s) in a region with no poller") || !strings.Contains(c.Summary, "1 could not be checked") {
		t.Errorf("summary %q", c.Summary)
	}
	all := strings.Join(c.Detail, "\n")
	for _, want := range []string{
		"b-ca: no poller in ca-central-1; 'lagotto deploy --teardown --region ca-central-1' deletes it",
		"b-eu: no poller in eu-west-1, and untagged",
		"b-ap: could not check",
	} {
		if !strings.Contains(all, want) {
			t.Errorf("detail lacks %q:\n%s", want, all)
		}
	}
	if strings.Contains(all, "b-us-west-2") {
		t.Errorf("bucket with a poller reported:\n%s", all)
	}
}
