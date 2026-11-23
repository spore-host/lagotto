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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/smithy-go"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/tags"
)

// Tool is the tool lagotto's lease items are kept under.
const Tool = tags.ToolLagotto

// ControlRegion is the region whose spore-host-lifecycle table holds lagotto's
// global-scope lifecycle item (the account-wide teardown's lease, and the
// creators' marks): the commercial partition's IAM home region, us-east-1, so
// that every CLI reads the same one. An empty region is the commercial
// partition's.
//
// Any other partition is an error, so the account-wide teardown fails closed
// there: the ISO partitions and the European Sovereign Cloud cannot reach
// us-east-1, and in aws-us-gov and aws-cn lagotto's IAM policies reach no
// lifecycle table (runtimeiam.RegionalARN names the aws partition only;
// lagotto#347).
func ControlRegion(region string) (string, error) {
	switch {
	case region == "":
		return "us-east-1", nil
	case strings.Contains(region, "-iso"), strings.HasPrefix(region, "eusc-"):
		return "", fmt.Errorf("no control region for %s: lagotto's account-wide lease works only in the aws partition", region)
	case strings.HasPrefix(region, "us-gov-"), strings.HasPrefix(region, "cn-"):
		return "", fmt.Errorf("no control region for %s: lagotto's IAM policies name only the aws partition's lifecycle tables (lagotto#347)", region)
	}
	for _, p := range []string{"us-", "eu-", "ap-", "ca-", "sa-", "me-", "af-", "il-", "mx-"} {
		if strings.HasPrefix(region, p) {
			return "us-east-1", nil
		}
	}
	return "", fmt.Errorf("no control region for %s: lagotto's account-wide lease works only in the aws partition", region)
}

// PollerOwner is the lease owner a region's poller writes.
func PollerOwner(region string) string { return "lagotto-poller/" + region }

// NewClient returns a lifecycle client that reaches each region's
// spore-host-lifecycle table with cfg's credentials, with lagotto's global
// scope in ControlRegion(cfg.Region) (none outside the aws partition: every
// use of the global scope then fails, closed). owner names the caller in a
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
func MarkActive(ctx context.Context, c *lifecycle.Client, region, what string) error {
	err := c.MarkActive(ctx, Tool, region)
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
