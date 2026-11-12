package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// ownerKeysDynamo is the DynamoDB `setup` uses to add the owner index and move
// legacy records under their owners' prefixes. *dynamodb.Client satisfies it.
type ownerKeysDynamo interface {
	watcher.MigrateAPI
	DescribeTable(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	UpdateTable(context.Context, *dynamodb.UpdateTableInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTableOutput, error)
}

// principalIDs looks up IAM users' and roles' unique ids. *iam.Client
// satisfies it.
type principalIDs interface {
	GetUser(context.Context, *iam.GetUserInput, ...func(*iam.Options)) (*iam.GetUserOutput, error)
	GetRole(context.Context, *iam.GetRoleInput, ...func(*iam.Options)) (*iam.GetRoleOutput, error)
}

// Seams for tests.
var (
	newOwnerKeysDynamo = func(cfg aws.Config) ownerKeysDynamo { return dynamodb.NewFromConfig(cfg) }
	newPrincipalIDs    = func(cfg aws.Config) principalIDs { return iam.NewFromConfig(cfg) }
	// ownerIndexWait bounds how long setup waits for a new owner index to
	// finish building before giving up (re-run setup to continue).
	ownerIndexWait = 15 * time.Minute
	ownerIndexPoll = 10 * time.Second
)

// resolverFor derives owner ids from creator ARNs with IAM reads.
func resolverFor(ids principalIDs) watcher.OwnerResolver {
	return func(ctx context.Context, arn string) (string, error) {
		return watcher.ResolveOwnerFromARN(ctx, arn,
			func(ctx context.Context, name string) (string, error) {
				out, err := ids.GetUser(ctx, &iam.GetUserInput{UserName: aws.String(name)})
				if err != nil {
					return "", fmt.Errorf("look up user %s: %w", name, err)
				}
				return aws.ToString(out.User.UserId), nil
			},
			func(ctx context.Context, name string) (string, error) {
				out, err := ids.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(name)})
				if err != nil {
					return "", fmt.Errorf("look up role %s: %w", name, err)
				}
				return aws.ToString(out.Role.RoleId), nil
			})
	}
}

// ensureOwnerKeys is setup's owner-scoping step, run before the owner-scoped
// lagotto-cli-user policy is written: add the owner index to a watches table
// that predates it (and wait for it to build), then move every legacy record
// under its owner's prefix. It is idempotent and resumable; re-run setup to
// move what a run reports it left.
func ensureOwnerKeys(ctx context.Context, db ownerKeysDynamo, ids principalIDs, tables watcher.MigrateTables, out io.Writer) (watcher.MigrateReport, error) {
	created, err := watcher.EnsureOwnerIndex(ctx, db, tables.Watches)
	if created || errors.Is(err, watcher.ErrOwnerIndexNotReady) {
		_, _ = fmt.Fprintf(out, "Building %s on %s (lagotto list reads it)...\n", watcher.OwnerIndexName, tables.Watches)
		err = waitOwnerIndex(ctx, db, tables.Watches)
	}
	if err != nil {
		return watcher.MigrateReport{}, err
	}
	rep, err := watcher.MigrateOwnerKeys(ctx, db, tables, resolverFor(ids), time.Now())
	if err != nil {
		return rep, fmt.Errorf("move records under their owners' keys: %w", err)
	}
	if n := rep.Watches + rep.Groups + rep.History + rep.Scheduled; n > 0 {
		_, _ = fmt.Fprintf(out, "Moved %d record(s) under their owners' keys (%d watches, %d groups, %d history rows, %d scheduled launches).\n",
			n, rep.Watches, rep.Groups, rep.History, rep.Scheduled)
	}
	if rep.Fragments > 0 {
		_, _ = fmt.Fprintf(out, "Removed %d stale fragment(s) left at moved watches' old keys.\n", rep.Fragments)
	}
	if rep.Remaining() {
		_, _ = fmt.Fprintf(out, "Left %d legacy record(s) in place; the poller still services them, but no CLI user can reach them:\n  %s\n"+
			"  Re-run 'lagotto setup' to retry the ones that were busy.\n",
			len(rep.Skipped), strings.Join(rep.Skipped, "\n  "))
	}
	return rep, nil
}

func waitOwnerIndex(ctx context.Context, db ownerKeysDynamo, table string) error {
	ctx, cancel := context.WithTimeout(ctx, ownerIndexWait)
	defer cancel()
	t := time.NewTicker(ownerIndexPoll)
	defer t.Stop()
	for {
		_, err := watcher.EnsureOwnerIndex(ctx, db, table)
		if !errors.Is(err, watcher.ErrOwnerIndexNotReady) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w; re-run 'lagotto setup' once it is ACTIVE", err)
		case <-t.C:
		}
	}
}
