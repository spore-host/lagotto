package cmd

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// stsIdentityAPI is the slice of STS we use, for testability.
type stsIdentityAPI interface {
	GetCallerIdentity(ctx context.Context, in *sts.GetCallerIdentityInput, optFns ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error)
}

// getWatchOwned fetches a watch by ID and authorizes the caller as its owner.
//
// Watches are addressable by a guessable ID, and the tables can be shared across
// an account/team, so cancel/extend/status/history-by-id must not act on a watch
// the caller doesn't own. We compare the watch's UserID (the creator's caller
// ARN) to the current caller's ARN and, on mismatch, return the SAME
// "not found" error as a missing watch — so a non-owner can't even probe which
// IDs exist (no existence oracle). (#41)
func getWatchOwned(ctx context.Context, store *watcher.Store, stsClient stsIdentityAPI, watchID string) (*watcher.Watch, error) {
	w, err := store.GetWatch(ctx, watchID)
	if err != nil {
		return nil, fmt.Errorf("get watch: %w", err)
	}
	notFound := fmt.Errorf("watch %s not found", watchID)
	if w == nil {
		return nil, notFound
	}

	identity, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, fmt.Errorf("get caller identity: %w", err)
	}
	caller := aws.ToString(identity.Arn)

	// A watch with no recorded owner (legacy) is only accessible if we also can't
	// determine a caller — otherwise treat the owner as authoritative.
	if w.UserID != "" && w.UserID != caller {
		return nil, notFound
	}
	return w, nil
}
