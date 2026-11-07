package watcher

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
)

// holderReleaseAPI is the slice of EC2 that Release calls, so tests can count
// calls through a fake. *ec2.Client satisfies it.
type holderReleaseAPI interface {
	CancelCapacityReservation(ctx context.Context, in *ec2.CancelCapacityReservationInput, optFns ...func(*ec2.Options)) (*ec2.CancelCapacityReservationOutput, error)
	DescribeCapacityReservations(ctx context.Context, in *ec2.DescribeCapacityReservationsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeCapacityReservationsOutput, error)
}

// holderCreateAPI is the slice of EC2 that Hold calls, so tests can count
// reservations created through a fake. *ec2.Client satisfies it.
type holderCreateAPI interface {
	CreateCapacityReservation(ctx context.Context, in *ec2.CreateCapacityReservationInput, optFns ...func(*ec2.Options)) (*ec2.CreateCapacityReservationOutput, error)
}

// Holder creates On-Demand Capacity Reservations to hold capacity.
type Holder struct {
	cfg aws.Config
	// releaseClient returns the EC2 client Release uses for a region.
	// Indirected for tests; defaults to ec2.NewFromConfig with the region set.
	releaseClient func(region string) holderReleaseAPI
	// createClient returns the EC2 client Hold uses for a region. Indirected
	// for tests; defaults to ec2.NewFromConfig with the region set.
	createClient func(region string) holderCreateAPI
	// launchableAZs returns the AZs in a region that have a default subnet,
	// the same lookup the spawner uses (#150). Hold only reserves in one of
	// them, so the reservation can be launched into without creating a subnet
	// first (#209). Nil, or an empty result (lookup failed), skips the check.
	launchableAZs func(ctx context.Context, region string) map[string]bool
	// forgetLaunchableAZs drops the region's cached launchable set after a
	// failed hold that skipped AZs with no default subnet, so the next cycle
	// re-reads it (#256). Nil = nothing cached (test holders).
	forgetLaunchableAZs func(region string)
	// describeReservation backs the #239 landing check: a created reservation
	// counts as held only once DescribeCapacityReservations shows it "active".
	// Nil = no landing check, so a hold is matched at once, as before #239 (the
	// test holders that predate it). Set in NewHolder; needs only
	// ec2:DescribeCapacityReservations, already granted.
	describeReservation func(ctx context.Context, region, reservationID string) (landingCheck, error)
}

// NewHolder creates a Holder.
func NewHolder(cfg aws.Config) *Holder {
	subnets := newDefaultSubnetAZLookup(cfg)
	h := &Holder{cfg: cfg, launchableAZs: subnets.lookup, forgetLaunchableAZs: subnets.forget}
	h.releaseClient = func(region string) holderReleaseAPI {
		c := h.cfg.Copy()
		c.Region = region
		return ec2.NewFromConfig(c)
	}
	h.createClient = func(region string) holderCreateAPI {
		c := h.cfg.Copy()
		c.Region = region
		return ec2.NewFromConfig(c)
	}
	h.describeReservation = func(ctx context.Context, region, id string) (landingCheck, error) {
		return describeReservationState(ctx, h.releaseClient(region), id)
	}
	return h
}

// chooseHoldAZ picks the AZ to reserve in: the first candidate AZ (in the
// match's preference order) that has a default subnet. A reservation is only
// useful if an instance can be launched into it, and the launch lagotto prints
// (HoldLaunchCommand) names --az but no subnet, so it needs the AZ's default
// subnet. An opted-in Local Zone has none, and truffle offers types there (#209).
//
// If the subnet lookup is unavailable or returns nothing (an API error, or an
// account with no default VPC), the first candidate is used, as before: the
// check must not block holds it cannot evaluate. If the lookup succeeds and no
// candidate has a default subnet, it returns an error (a *NoSubnetError, #256)
// and nothing is reserved.
//
// noSubnet lists the candidates skipped for want of a default subnet, so a
// failed reservation can say they were (#256).
func (h *Holder) chooseHoldAZ(ctx context.Context, m *MatchResult) (az string, noSubnet []string, err error) {
	candidates := m.CandidateAZs
	if len(candidates) == 0 {
		candidates = []string{m.AvailabilityZone}
	}
	if h.launchableAZs == nil {
		return candidates[0], nil, nil
	}
	usable := h.launchableAZs(ctx, m.Region)
	if len(usable) == 0 {
		return candidates[0], nil, nil
	}
	kept, noSubnet := splitLaunchableAZs(usable, candidates)
	for _, az := range kept {
		if az != "" {
			return az, noSubnet, nil
		}
	}
	h.forgetSubnets(m.Region)
	return "", noSubnet, withNoSubnet(fmt.Errorf("not reserving %s in %s: none of the offered AZs %v has a default subnet, "+
		"so the reservation could not be launched into without creating a subnet first "+
		"(an opted-in Local Zone has no default subnet)", m.InstanceType, m.Region, candidates), noSubnet, true)
}

// forgetSubnets drops the region's cached default-subnet set, if any.
func (h *Holder) forgetSubnets(region string) {
	if h.forgetLaunchableAZs != nil {
		h.forgetLaunchableAZs(region)
	}
}

// HoldDuration is how long a hold's capacity reservation lasts: its EndDate is
// creation + HoldDuration. The reservation is billed at the on-demand rate for
// all of it whether or not an instance is launched into it, which is what the
// hold exposure line (#239, exposure.go) states.
const HoldDuration = 30 * time.Minute

// Hold creates a targeted ODCR for the matched instance type, in the first
// candidate AZ that has a default subnet (#209); m.AvailabilityZone is set to
// that AZ. The reservation expires after 30 minutes — the user must launch
// within that window. Updates the MatchResult with the reservation ID on success.
func (h *Holder) Hold(ctx context.Context, w *Watch, m *MatchResult) error {
	if m.AvailabilityZone == "" && len(m.CandidateAZs) == 0 {
		return fmt.Errorf("cannot create capacity reservation without availability zone")
	}
	az, noSubnet, err := h.chooseHoldAZ(ctx, m)
	if err != nil {
		m.ActionTaken = "hold_failed"
		return err
	}
	if az == "" {
		return fmt.Errorf("cannot create capacity reservation without availability zone")
	}
	// The match payload, the email and `lagotto status` report the reservation
	// at m.AvailabilityZone, so it must be the AZ actually reserved.
	m.AvailabilityZone = az

	client := h.createClient(m.Region)

	endDate := time.Now().UTC().Add(HoldDuration)

	result, err := client.CreateCapacityReservation(ctx, &ec2.CreateCapacityReservationInput{
		InstanceType:          aws.String(m.InstanceType),
		InstancePlatform:      ec2types.CapacityReservationInstancePlatformLinuxUnix,
		AvailabilityZone:      aws.String(az),
		InstanceCount:         aws.Int32(1),
		EndDateType:           ec2types.EndDateTypeLimited,
		EndDate:               aws.Time(endDate),
		InstanceMatchCriteria: ec2types.InstanceMatchCriteriaTargeted,
		TagSpecifications: []ec2types.TagSpecification{
			{
				ResourceType: ec2types.ResourceTypeCapacityReservation,
				Tags: []ec2types.Tag{
					{Key: aws.String("lagotto:watch-id"), Value: aws.String(w.WatchID)},
					{Key: aws.String("lagotto:managed"), Value: aws.String("true")},
				},
			},
		},
	})
	if err != nil {
		m.ActionTaken = "hold_failed"
		if len(noSubnet) > 0 {
			h.forgetSubnets(m.Region)
		}
		return withNoSubnet(fmt.Errorf("create capacity reservation: %w", err), noSubnet, false)
	}

	m.ReservationID = *result.CapacityReservation.CapacityReservationId
	// The state at create (#239): "active" is held now; anything else ("pending")
	// is checked on later cycles before the watch counts it as held.
	m.ReservationState = string(result.CapacityReservation.State)
	m.ActionTaken = "held"
	return nil
}

// ReleaseOutcome reports what Release found.
type ReleaseOutcome struct {
	// Released is true when this call cancelled the reservation.
	Released bool
	// AlreadyEnded is true when the reservation was already cancelled, expired
	// or failed, or no longer exists, so there was nothing to release. That is
	// success, not an error: the reservation has stopped billing either way.
	AlreadyEnded bool
	// State is the reservation's state when AlreadyEnded ("cancelled",
	// "expired", "failed"), or "not found".
	State string
}

// ErrReleaseNotPermitted is returned (wrapped) when the caller's credentials
// may not call ec2:CancelCapacityReservation.
var ErrReleaseNotPermitted = errors.New("not permitted to cancel the capacity reservation")

// Release cancels a capacity reservation created by Hold. Cancelling a watch
// must release any still-active ODCR so it stops billing, rather than waiting
// out the 30-minute auto-expiry (#41); releasing a fired hold early is #203.
//
// It is idempotent: a reservation that already ended (cancelled by an earlier
// release, or expired at its EndDate) is reported as AlreadyEnded with no
// error, so running `lagotto cancel` twice, or after the window closed, is
// fine. An empty id is a no-op.
func (h *Holder) Release(ctx context.Context, region, reservationID string) (ReleaseOutcome, error) {
	if reservationID == "" {
		return ReleaseOutcome{}, nil
	}
	client := h.releaseClient(region)

	_, err := client.CancelCapacityReservation(ctx, &ec2.CancelCapacityReservationInput{
		CapacityReservationId: aws.String(reservationID),
	})
	if err == nil {
		return ReleaseOutcome{Released: true}, nil
	}
	if isAccessDenied(err) {
		return ReleaseOutcome{}, fmt.Errorf("%w %s: %v", ErrReleaseNotPermitted, reservationID, err)
	}
	if isReservationNotFound(err) {
		return ReleaseOutcome{AlreadyEnded: true, State: "not found"}, nil
	}

	// The cancel failed for another reason. If the reservation has already
	// ended, that is why (EC2 refuses to cancel a reservation that is no longer
	// active); ask rather than guess from the error text.
	out, derr := client.DescribeCapacityReservations(ctx, &ec2.DescribeCapacityReservationsInput{
		CapacityReservationIds: []string{reservationID},
	})
	if derr != nil {
		if isReservationNotFound(derr) {
			return ReleaseOutcome{AlreadyEnded: true, State: "not found"}, nil
		}
		return ReleaseOutcome{}, fmt.Errorf("cancel capacity reservation %s: %w", reservationID, err)
	}
	if out == nil || len(out.CapacityReservations) == 0 {
		return ReleaseOutcome{AlreadyEnded: true, State: "not found"}, nil
	}
	switch st := out.CapacityReservations[0].State; st {
	case ec2types.CapacityReservationStateCancelled, ec2types.CapacityReservationStateExpired, ec2types.CapacityReservationStateFailed:
		return ReleaseOutcome{AlreadyEnded: true, State: string(st)}, nil
	}
	return ReleaseOutcome{}, fmt.Errorf("cancel capacity reservation %s: %w", reservationID, err)
}

func apiErrorCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

func isAccessDenied(err error) bool {
	switch apiErrorCode(err) {
	case "UnauthorizedOperation", "AccessDenied", "AccessDeniedException":
		return true
	}
	return false
}

func isReservationNotFound(err error) bool {
	return apiErrorCode(err) == "InvalidCapacityReservationId.NotFound"
}
