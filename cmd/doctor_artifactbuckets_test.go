package cmd

import (
	"context"
	"errors"
	"testing"

	"github.com/spore-host/lagotto/pkg/doctor"
)

type fakeArtifactLister struct {
	regions []string
	err     error
}

func (f fakeArtifactLister) ArtifactBucketRegions(context.Context, string) ([]string, error) {
	return f.regions, f.err
}

type fakeArtifactProbe struct {
	pollers map[string]bool
	managed map[string]bool
	tagAsks []string
}

func (f *fakeArtifactProbe) PollerExists(_ context.Context, r string) (bool, error) {
	return f.pollers[r], nil
}

func (f *fakeArtifactProbe) Managed(_ context.Context, r, _ string) (bool, error) {
	f.tagAsks = append(f.tagAsks, r)
	return f.managed[r], nil
}

func TestAppendArtifactBucketCheck(t *testing.T) {
	ctx := context.Background()

	r := &doctor.Report{}
	appendArtifactBucketCheck(ctx, r, "123456789012", fakeArtifactLister{}, &fakeArtifactProbe{})
	if len(r.Checks) != 0 {
		t.Errorf("no buckets added %+v", r.Checks)
	}

	r = &doctor.Report{}
	appendArtifactBucketCheck(ctx, r, "123456789012", fakeArtifactLister{err: errors.New("denied")}, &fakeArtifactProbe{})
	if len(r.Checks) != 1 || r.Checks[0].Status != doctor.StatusWarn || r.Failed() {
		t.Errorf("list failure: %+v", r.Checks)
	}

	// us-west-2 has a poller; ca-central-1 does not, so it is an orphan. The
	// tag is asked only for the orphan.
	p := &fakeArtifactProbe{pollers: map[string]bool{"us-west-2": true}, managed: map[string]bool{"ca-central-1": true}}
	r = &doctor.Report{}
	appendArtifactBucketCheck(ctx, r, "123456789012", fakeArtifactLister{regions: []string{"us-west-2", "ca-central-1"}}, p)
	if len(r.Checks) != 1 || r.Checks[0].Name != doctor.CheckArtifactBuckets || r.Checks[0].Status != doctor.StatusWarn || r.Failed() {
		t.Fatalf("orphan: %+v", r.Checks)
	}
	if len(p.tagAsks) != 1 || p.tagAsks[0] != "ca-central-1" {
		t.Errorf("tag looked up for %v, want only the orphan region", p.tagAsks)
	}
}
