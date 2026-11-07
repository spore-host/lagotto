package cmd

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/doctor"
)

// artifactBucketLister lists the regions of the account's default-named
// artifact buckets; *deploy.Deployer satisfies it.
type artifactBucketLister interface {
	ArtifactBucketRegions(ctx context.Context, accountID string) ([]string, error)
}

// artifactBucketProbe reports, for one region, whether the poller exists and
// (asked only when it does not) whether that region's bucket is lagotto-managed.
type artifactBucketProbe interface {
	PollerExists(ctx context.Context, region string) (bool, error)
	Managed(ctx context.Context, region, accountID string) (bool, error)
}

// appendArtifactBucketCheck adds doctor's orphan artifact bucket check (#264)
// to report. It looks in every region, not just the current one: an orphan
// bucket is by definition in a region nobody is using any more.
func appendArtifactBucketCheck(ctx context.Context, report *doctor.Report, accountID string,
	lister artifactBucketLister, probe artifactBucketProbe) {
	regions, listErr := lister.ArtifactBucketRegions(ctx, accountID)
	var buckets []doctor.ArtifactBucket
	for _, r := range regions {
		b := doctor.ArtifactBucket{Region: r, Name: deploy.DefaultBucketName(accountID, r)}
		b.Poller, b.Err = probe.PollerExists(ctx, r)
		if b.Err == nil && !b.Poller {
			b.Managed, b.Err = probe.Managed(ctx, r, accountID)
		}
		buckets = append(buckets, b)
	}
	if c, ok := doctor.EvalArtifactBuckets(buckets, listErr); ok {
		report.Checks = append(report.Checks, c)
	}
}

// regionalArtifactProbe answers artifactBucketProbe with a Deployer built for
// the bucket's region: the poller is a regional Lambda, and S3 answers a
// bucket's tagging only from the bucket's own region.
type regionalArtifactProbe struct{ cfg aws.Config }

func (p regionalArtifactProbe) deployer(region string) *deploy.Deployer {
	c := p.cfg.Copy()
	c.Region = region
	return deploy.New(c)
}

func (p regionalArtifactProbe) PollerExists(ctx context.Context, region string) (bool, error) {
	info, err := p.deployer(region).InspectPoller(ctx)
	return info.Exists, err
}

func (p regionalArtifactProbe) Managed(ctx context.Context, region, accountID string) (bool, error) {
	return p.deployer(region).ArtifactBucketManaged(ctx, region, accountID)
}
