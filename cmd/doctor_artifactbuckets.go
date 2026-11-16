package cmd

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/spore-host/lagotto/pkg/deploy"
	"github.com/spore-host/lagotto/pkg/doctor"
	"github.com/spore-host/lagotto/pkg/inventory"
)

// appendArtifactBucketCheck adds doctor's orphan artifact bucket check (#264)
// to report. It looks in every region, not just the current one: an orphan
// bucket is by definition in a region nobody is using any more. The buckets
// are read by the footprint's reader (inventory.GatherArtifactBuckets, #325),
// asking a bucket's tags only when it is an orphan.
func appendArtifactBucketCheck(ctx context.Context, report *doctor.Report, accountID string,
	lister inventory.ArtifactBucketLister, probe inventory.ArtifactBucketProbe) {
	buckets, listErr := inventory.GatherArtifactBuckets(ctx, accountID, lister, probe, false)
	if c, ok := doctor.EvalArtifactBuckets(buckets, listErr); ok {
		report.Checks = append(report.Checks, c)
	}
}

// regionalArtifactProbe answers inventory.ArtifactBucketProbe with a Deployer
// built for the bucket's region: the poller is a regional Lambda, and S3
// answers a bucket's tagging only from the bucket's own region.
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

func (p regionalArtifactProbe) BucketTags(ctx context.Context, region, accountID string) (map[string]string, error) {
	return p.deployer(region).ArtifactBucketTags(ctx, region, accountID)
}
