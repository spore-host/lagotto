package doctor

import (
	"fmt"
	"strings"

	"github.com/spore-host/lagotto/pkg/inventory"
)

// CheckArtifactBuckets names the orphan artifact bucket check (#264).
const CheckArtifactBuckets = "artifact-buckets"

// ArtifactBucket is one default-named lagotto-lambda-<account>-<region>
// bucket and what doctor learned about its region. It is the footprint's own
// record of the bucket (pkg/inventory, #325), read by the same
// inventory.GatherArtifactBuckets, so doctor and `lagotto footprint` cannot
// disagree about which bucket is an orphan.
type ArtifactBucket = inventory.ArtifactBucket

// EvalArtifactBuckets reports artifact buckets in regions with no poller: left
// by a `deploy --teardown` from before #264, which retained the bucket, or by
// a deploy that never finished. Pure, like Evaluate. Every outcome is WARN at
// most: an orphan bucket costs a few cents of storage, never the poller's
// correctness.
//
// ok is false when there is nothing to say: no artifact bucket at all.
func EvalArtifactBuckets(buckets []ArtifactBucket, listErr error) (Check, bool) {
	c := Check{Name: CheckArtifactBuckets}
	if listErr != nil {
		c.Status = StatusWarn
		c.Summary = fmt.Sprintf("could not list lagotto-lambda-* artifact buckets (s3:ListAllMyBuckets): %v", listErr)
		return c, true
	}
	if len(buckets) == 0 {
		return Check{}, false
	}
	var orphans, unknown []string
	for _, b := range buckets {
		switch {
		case b.Err != nil:
			unknown = append(unknown, fmt.Sprintf("%s: could not check the poller or tag in %s: %v", b.Name, b.Region, b.Err))
		case !b.Orphan():
		case b.Managed:
			orphans = append(orphans, fmt.Sprintf("%s: no poller in %s; 'lagotto deploy --teardown --region %s' deletes it",
				b.Name, b.Region, b.Region))
		default:
			orphans = append(orphans, fmt.Sprintf("%s: no poller in %s, and untagged (made by a lagotto before #264), so teardown retains it; "+
				"delete every object version and then the bucket by hand, or run 'lagotto deploy --region %s' to tag it and then 'lagotto deploy --teardown --region %s'",
				b.Name, b.Region, b.Region, b.Region))
		}
	}
	if len(orphans) == 0 && len(unknown) == 0 {
		c.Status = StatusPass
		c.Summary = fmt.Sprintf("%d artifact bucket(s), each in a region with a poller", len(buckets))
		return c, true
	}
	c.Status = StatusWarn
	var parts []string
	if len(orphans) > 0 {
		parts = append(parts, fmt.Sprintf("%d artifact bucket(s) in a region with no poller", len(orphans)))
	}
	if len(unknown) > 0 {
		parts = append(parts, fmt.Sprintf("%d could not be checked", len(unknown)))
	}
	c.Summary = strings.Join(parts, "; ")
	c.Detail = append(orphans, unknown...)
	return c, true
}
