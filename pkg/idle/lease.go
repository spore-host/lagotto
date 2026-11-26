// Package idle is lagotto's leave-no-trace idle teardown (lagotto#326, part of
// spore-host/spore-host#752): the teardown lease its creators take, the
// poller's per-region idle verdict, and the teardown the poller runs once a
// region has been idle for the grace period, including the plan for removing
// itself.
//
// # The lease
//
// Every lagotto operation that creates something (deploy, watch, watch
// --local, launch, and the poller when it fires or re-arms a scheduled
// launch) calls [MarkActive] first. It marks lagotto active in the region's
// spore-host-lifecycle table (github.com/spore-host/libs/lifecycle) and
// waits while a teardown holds the lease there, so a teardown that is
// deleting can never race a creator.
//
// # Report mode
//
// This release only reports. The poller computes the verdict, takes the
// lease, builds the full teardown plan, re-checks every step against the
// live account and logs what it would delete and how it would remove itself,
// and gives the lease back. It deletes nothing: [Config.Reap] is false in the
// poller, and lagotto#328 turns it on.
package idle

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/tags"

	"github.com/spore-host/lagotto/pkg/awsarn"
)

// Tool is the tool lagotto's lease items are kept under.
const Tool = tags.ToolLagotto

// ControlRegion is the region whose spore-host-lifecycle table holds lagotto's
// global-scope lifecycle item (the account-wide teardown's lease, and the
// creators' marks) in region's partition: its IAM home region, so that every
// CLI in the partition reads the same one. That is us-east-1 in aws,
// us-gov-west-1 in aws-us-gov and cn-north-1 in aws-cn (lagotto#347: every ARN
// lagotto's IAM policies name is in the account's partition, so the creators'
// marks, under lagotto-cli-user, and the poller's grants reach the lifecycle
// tables there). An empty region is the commercial partition's.
//
// Any other partition is an error, so the account-wide teardown fails closed
// there: lagotto does not support the ISO partitions or the European Sovereign
// Cloud (awsarn.Partition refuses them, and setup and deploy write no IAM
// there).
func ControlRegion(region string) (string, error) {
	if region == "" {
		return controlRegions[awsarn.AWS], nil
	}
	p, err := awsarn.Partition(region)
	if err != nil {
		return "", fmt.Errorf("no control region for %s: lagotto's account-wide lease works only in the aws, aws-us-gov and aws-cn partitions (%w)", region, err)
	}
	return controlRegions[p], nil
}

// controlRegions is each supported partition's control region.
var controlRegions = map[string]string{
	awsarn.AWS:      "us-east-1",
	awsarn.GovCloud: "us-gov-west-1",
	awsarn.China:    "cn-north-1",
}

// PollerOwner is the lease owner a region's poller writes, before RunOwner
// makes it unique.
func PollerOwner(region string) string { return "lagotto-poller/" + region }

// RunOwner is base made unique to this process (a poller's container, a CLI
// run): a lease another one wrote never passes for its own.
func RunOwner(base string) string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%s/%d", base, time.Now().UnixNano())
	}
	return base + "/" + hex.EncodeToString(b)
}

// NewClient returns a lifecycle client that reaches each region's
// spore-host-lifecycle table with cfg's credentials, with lagotto's global
// scope in ControlRegion(cfg.Region) (none in the ISO partitions or the
// European Sovereign Cloud: every use of the global scope then fails, closed). owner names the caller in a
// lease (PollerOwner(region), or the CLI caller's ARN).
func NewClient(cfg aws.Config, owner string, opts ...func(*lifecycle.Options)) (*lifecycle.Client, error) {
	control, _ := ControlRegion(cfg.Region)
	o := lifecycle.Options{
		DynamoDB: func(region string) (lifecycle.API, error) {
			c := cfg.Copy()
			c.Region = region
			return dynamodb.NewFromConfig(c), nil
		},
		ControlRegion: control,
		Owner:         owner,
		// An idle Observe (the poller's) leaves a creator's mark this young
		// alone: a creator it cannot see is running.
		MarkBound: CreatorMarkBound,
		// A creator waits out a lease longer than it takes to go stale, so
		// one a teardown that was killed left is taken over, not a refusal.
		WaitTimeout: CreatorWait,
	}
	for _, f := range opts {
		f(&o)
	}
	return lifecycle.New(o)
}

// ErrLeaseUnavailable is what [MarkActive] reports when it could not reach the
// lease table because the caller's IAM predates it (an AccessDenied). The
// caller warns and carries on, as a lagotto from before lagotto#326 did:
// refusing would break every watch until an administrator re-ran `lagotto
// setup`. lagotto#328, which turns on deletion, is where this becomes a
// refusal.
var ErrLeaseUnavailable = errors.New("the teardown lease table is not accessible")

// TeardownInProgressError is [MarkActive]'s refusal: a lagotto teardown held
// the lease in the region for the whole wait.
type TeardownInProgressError struct {
	Region, What string
	Wait         *lifecycle.WaitError
}

func (e *TeardownInProgressError) Error() string {
	holder := ""
	if e.Wait.Owner != "" {
		holder = fmt.Sprintf(" (lease held by %s, last heartbeat %s)", e.Wait.Owner, e.Wait.Heartbeat.UTC().Format(time.RFC3339))
	}
	where, footprint := "in "+e.Region, "--region "+e.Region
	if e.Region == lifecycle.ScopeGlobal {
		where, footprint = "account-wide (its IAM roles and policy)", "--all-regions"
	}
	return fmt.Sprintf("refusing to %s: lagotto is being torn down %s%s, and the teardown still held the "+
		"spore-host-lifecycle lease after %s (blocked by %s). Nothing was created. Run it again once the teardown "+
		"has finished; 'lagotto footprint %s' shows what is left",
		e.What, where, holder, e.Wait.Waited.Round(time.Second), e.Wait.Blocker, footprint)
}

func (e *TeardownInProgressError) Unwrap() error { return e.Wait }

// MarkActive records that lagotto is about to create something in region,
// waiting out a teardown that holds the lease there. what names the operation
// for the refusal ("create watch w-1", "deploy the poller").
//
// It returns nil when the caller may create; a *TeardownInProgressError when a
// teardown held the lease for the whole wait; an error wrapping
// [ErrLeaseUnavailable] when the caller is not allowed to reach the table (it
// should warn and carry on); and any other failure wrapped, which refuses.
//
// It records activity with no end of its own (lifecycle MarkActive: in use
// for CreatorMarkBound), for the poller, which is its region's backstop and
// sees what it created; a CLI creator uses Mark, and ends it.
func MarkActive(ctx context.Context, c *lifecycle.Client, region, what string) error {
	return markErr(c.MarkActive(ctx, Tool, region), region, what)
}

// Mark is MarkActive, returning the token of the creator mark it wrote, for
// lifecycle's MarkDone once the creator has finished (lagotto#340).
func Mark(ctx context.Context, c *lifecycle.Client, region, what string) (string, error) {
	token, err := c.Mark(ctx, Tool, region)
	return token, markErr(err, region, what)
}

// EndMark ends a creator's mark (lifecycle MarkDone), tried again on any
// failure: a mark left keeps lagotto in use for CreatorMarkBound. A retry
// after a removal that applied finds the mark gone, which is not an error.
func EndMark(ctx context.Context, c *lifecycle.Client, scope, token string) error {
	var err error
	for attempt := 0; attempt < readAttempts; attempt++ {
		if attempt > 0 {
			if serr := endMarkSleep(ctx, time.Second); serr != nil {
				return errors.Join(err, serr)
			}
		}
		if _, err = c.MarkDone(ctx, Tool, scope, token); err == nil {
			return nil
		}
	}
	return err
}

// endMarkSleep is EndMark's pause between tries. A seam for tests.
var endMarkSleep = sleep

// CreatorWait is how long a creator waits for a teardown's lease
// (lifecycle Options.WaitTimeout): longer than the lease takes to go stale
// (lifecycle DefaultStaleAfter, 20 min), so a lease that a teardown killed
// mid-run left is taken over by the creator, never a refusal.
const CreatorWait = lifecycle.DefaultStaleAfter + 2*time.Minute

// RefreshMark re-stamps a creator's mark (lifecycle RefreshMark): a creator
// that may run longer than CreatorMarkBound calls it well within it. False
// means the mark is gone, or a teardown holds the scope.
func RefreshMark(ctx context.Context, c *lifecycle.Client, scope, token string) (bool, error) {
	return c.RefreshMark(ctx, Tool, scope, token)
}

// CreatorMarkBound is how long a creator's mark keeps lagotto's scope (a
// region, or the global one) in use (lagotto#340): the bound lagotto judges
// with (lifecycle InUse and IdleSince), and the one an idle Observe leaves a
// mark alone for. A creator ends its mark when it finishes (MarkDone); one
// still there after this is a creator that died, and the scope is idle from
// then.
const CreatorMarkBound = time.Hour

func markErr(err error, region, what string) error {
	var we *lifecycle.WaitError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &we):
		return &TeardownInProgressError{Region: region, What: what, Wait: we}
	case isAccessDenied(err):
		return fmt.Errorf("%w in %s: %v", ErrLeaseUnavailable, region, err)
	}
	return fmt.Errorf("refusing to %s: could not mark lagotto active in the spore-host-lifecycle table in %s: %w. Nothing was created", what, region, err)
}

// LeaseWarning is what a creator prints when [MarkActive] reported
// [ErrLeaseUnavailable].
func LeaseWarning(err error) string {
	return fmt.Sprintf("warning: %v.\n  Continuing without the teardown lease (lagotto#326). An administrator's 'lagotto setup' "+
		"updates the lagotto-cli-user policy to grant it.", err)
}

func isAccessDenied(err error) bool {
	var ae smithy.APIError
	if !errors.As(err, &ae) {
		return false
	}
	code := ae.ErrorCode()
	return strings.HasPrefix(code, "AccessDenied") || code == "UnauthorizedOperation"
}
