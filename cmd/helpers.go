package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// shortOwner renders a watch's UserID (the creator's caller ARN) as a compact,
// human-readable owner for list output (#1): the ARN's trailing resource
// segment — e.g. "user/alice" or "assumed-role/Dev/alice-session" — which is the
// part after the last ':'. Returns "-" for an empty owner (legacy watches).
func shortOwner(arn string) string {
	if arn == "" {
		return "-"
	}
	if i := strings.LastIndex(arn, ":"); i >= 0 && i+1 < len(arn) {
		return arn[i+1:]
	}
	return arn
}

// stsIdentityAPI is the slice of STS we use, for testability.
type stsIdentityAPI interface {
	GetCallerIdentity(ctx context.Context, in *sts.GetCallerIdentityInput, optFns ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error)
}

// getWatchOwned fetches a watch by ID and authorizes the caller as its owner.
//
// The ID the user types is the short one ("w-1a2b3c4d"); the record's key is
// "<caller's principal id>/w-1a2b3c4d" (watcher.OwnerKey), and the
// lagotto-cli-user policy lets IAM serve only keys under the caller's own
// prefix. A legacy key (no prefix, from before `lagotto setup` moved it) is
// tried next and checked here as before. A non-owner gets the SAME "not
// found" as a missing watch, so IDs cannot be probed (#41).
func getWatchOwned(ctx context.Context, store *watcher.Store, stsClient stsIdentityAPI, watchID string) (*watcher.Watch, error) {
	w, _, err := resolveOwnedWatch(ctx, stsClient, watchID, func(key string) (*watcher.Watch, string, error) {
		w, err := store.GetWatch(ctx, key)
		if err != nil {
			return nil, "", fmt.Errorf("get watch: %w", err)
		}
		return w, "", nil
	})
	return w, err
}

// getWatchOwnedVersioned is getWatchOwned with a strongly consistent read that
// also returns the record's raw updated_at, the version `update`'s conditional
// write compares against (#257).
func getWatchOwnedVersioned(ctx context.Context, store *watcher.Store, stsClient stsIdentityAPI, watchID string) (*watcher.Watch, string, error) {
	return resolveOwnedWatch(ctx, stsClient, watchID, func(key string) (*watcher.Watch, string, error) {
		return store.GetWatchVersioned(ctx, key)
	})
}

// watchKeys are the keys a user-typed watch id may name, in the order to try:
// a full key as given; otherwise the caller's own key, then the legacy one.
func watchKeys(arg, callerID string) []string {
	if !watcher.IsLegacyKey(arg) {
		return []string{arg}
	}
	if callerID == "" {
		return []string{arg}
	}
	return []string{watcher.OwnerKey(callerID, arg), arg}
}

func resolveOwnedWatch(ctx context.Context, stsClient stsIdentityAPI, arg string, get func(key string) (*watcher.Watch, string, error)) (*watcher.Watch, string, error) {
	identity, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, "", fmt.Errorf("get caller identity: %w", err)
	}
	notFound := fmt.Errorf("watch %s not found", watcher.ShortID(arg))
	for _, key := range watchKeys(arg, aws.ToString(identity.UserId)) {
		w, version, err := get(key)
		if err != nil {
			if isAccessDenied(err) {
				continue // IAM refused a key outside the caller's prefix: not theirs
			}
			return nil, "", err
		}
		if w == nil {
			continue
		}
		owned, aerr := authorizeIdentity(identity, key, w)
		if aerr != nil {
			continue
		}
		return owned, version, nil
	}
	return nil, "", notFound
}

// authorizeWatchOwner returns w if the caller owns it, else the same "not
// found" error as a missing (nil) watch.
func authorizeWatchOwner(ctx context.Context, stsClient stsIdentityAPI, watchID string, w *watcher.Watch) (*watcher.Watch, error) {
	notFound := fmt.Errorf("watch %s not found", watchID)
	if w == nil {
		return nil, notFound
	}

	identity, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, fmt.Errorf("get caller identity: %w", err)
	}
	w, err = authorizeIdentity(identity, watchID, w)
	if err != nil {
		return nil, notFound
	}
	return w, nil
}

// authorizeIdentity is the ownership decision for the watch read at key. A
// scoped key is the caller's only when its prefix is the caller's principal
// id (IAM refuses the read otherwise; this is the same rule, client side). A
// legacy key falls back to watcher.OwnedBy.
func authorizeIdentity(identity *sts.GetCallerIdentityOutput, key string, w *watcher.Watch) (*watcher.Watch, error) {
	callerID := aws.ToString(identity.UserId)
	if owner := watcher.KeyOwner(key); owner != "" {
		if owner != callerID || w.WatchID != key {
			return nil, errNotOwner
		}
		return w, nil
	}
	if !watcher.OwnedBy(w.OwnerID, w.UserID, callerID, aws.ToString(identity.Arn)) {
		return nil, errNotOwner
	}
	// A watch created before owner ids were recorded passed on its ARN: note
	// the caller's id on it, so the next write that carries it (`lagotto
	// update`) records it.
	if w.OwnerID == "" && w.UserID != "" && callerID != "" {
		w.OwnerID = callerID
		w.OwnerIDBackfill = true
	}
	return w, nil
}

var errNotOwner = errors.New("not the owner")

// isAccessDenied reports an IAM denial from an AWS call.
func isAccessDenied(err error) bool {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "AccessDeniedException", "AccessDenied", "UnauthorizedOperation":
			return true
		}
	}
	return false
}

// listCallerWatches is every watch the caller owns, optionally only those in
// status: the ones under the caller's own key prefix (the owner index), plus,
// until `lagotto setup` has moved them, legacy ones recorded under the
// caller's ARN. Once the owner-scoped policy is in place IAM refuses the
// legacy query, which then contributes nothing.
func listCallerWatches(ctx context.Context, store *watcher.Store, identity *sts.GetCallerIdentityOutput, status watcher.WatchStatus) ([]watcher.Watch, error) {
	callerID, arn := aws.ToString(identity.UserId), aws.ToString(identity.Arn)
	var out []watcher.Watch
	if callerID != "" {
		own, err := store.ListWatchesByOwner(ctx, callerID, status)
		switch {
		case err == nil:
			out = append(out, own...)
		case isMissingIndex(err):
			// A table from before the owner index: `lagotto setup` adds it.
		default:
			return nil, err
		}
	}
	legacy, err := store.ListWatchesByUser(ctx, arn, status)
	if err != nil {
		if isAccessDenied(err) || isMissingIndex(err) {
			return out, nil
		}
		return nil, err
	}
	for _, w := range legacy {
		if watcher.IsLegacyKey(w.WatchID) && watcher.OwnedBy(w.OwnerID, w.UserID, callerID, arn) {
			out = append(out, w)
		}
	}
	return out, nil
}

// isMissingIndex reports DynamoDB's refusal of a Query on an index the table
// does not have.
func isMissingIndex(err error) bool {
	var ae smithy.APIError
	return errors.As(err, &ae) && ae.ErrorCode() == "ValidationException" &&
		strings.Contains(strings.ToLower(ae.ErrorMessage()), "index")
}
