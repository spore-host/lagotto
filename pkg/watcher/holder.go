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

// Holder creates On-Demand Capacity Reservations to hold capacity.
type Holder struct {
	cfg aws.Config
	// releaseClient returns the EC2 client Release uses for a region.
	// Indirected for tests; defaults to ec2.NewFromConfig with the region set.
	releaseClient func(region string) holderReleaseAPI
}

// NewHolder creates a Holder.
func NewHolder(cfg aws.Config) *Holder {
	h := &Holder{cfg: cfg}
	h.releaseClient = func(region string) holderReleaseAPI {
		c := h.cfg.Copy()
		c.Region = region
		return ec2.NewFromConfig(c)
	}
	return h
}

// Hold creates a targeted ODCR for the matched instance type and AZ.
// The reservation expires after 30 minutes — the user must launch within that window.
// Updates the MatchResult with the reservation ID on success.
func (h *Holder) Hold(ctx context.Context, w *Watch, m *MatchResult) error {
	if m.AvailabilityZone == "" {
		return fmt.Errorf("cannot create capacity reservation without availability zone")
	}

	// Create a region-specific EC2 client
	cfg := h.cfg.Copy()
	cfg.Region = m.Region
	client := ec2.NewFromConfig(cfg)

	endDate := time.Now().UTC().Add(30 * time.Minute)

	result, err := client.CreateCapacityReservation(ctx, &ec2.CreateCapacityReservationInput{
		InstanceType:          aws.String(m.InstanceType),
		InstancePlatform:      ec2types.CapacityReservationInstancePlatformLinuxUnix,
		AvailabilityZone:      aws.String(m.AvailabilityZone),
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
		return fmt.Errorf("create capacity reservation: %w", err)
	}

	m.ReservationID = *result.CapacityReservation.CapacityReservationId
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
