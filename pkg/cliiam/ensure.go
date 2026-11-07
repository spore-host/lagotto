package cliiam

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"

	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// maxPolicyVersions is IAM's limit on the versions a managed policy keeps.
// CreatePolicyVersion fails with LimitExceeded once there are this many.
const maxPolicyVersions = 5

// PolicyAPI is the slice of the IAM API [EnsurePolicy] needs. *iam.Client
// satisfies it.
type PolicyAPI interface {
	GetPolicy(ctx context.Context, in *iam.GetPolicyInput, optFns ...func(*iam.Options)) (*iam.GetPolicyOutput, error)
	GetPolicyVersion(ctx context.Context, in *iam.GetPolicyVersionInput, optFns ...func(*iam.Options)) (*iam.GetPolicyVersionOutput, error)
	CreatePolicy(ctx context.Context, in *iam.CreatePolicyInput, optFns ...func(*iam.Options)) (*iam.CreatePolicyOutput, error)
	CreatePolicyVersion(ctx context.Context, in *iam.CreatePolicyVersionInput, optFns ...func(*iam.Options)) (*iam.CreatePolicyVersionOutput, error)
	ListPolicyVersions(ctx context.Context, in *iam.ListPolicyVersionsInput, optFns ...func(*iam.Options)) (*iam.ListPolicyVersionsOutput, error)
	DeletePolicyVersion(ctx context.Context, in *iam.DeletePolicyVersionInput, optFns ...func(*iam.Options)) (*iam.DeletePolicyVersionOutput, error)
}

// ErrUnmanagedPolicy is returned when a policy named [PolicyName] exists without
// lagotto's managed tag: lagotto did not create it, so it neither rewrites nor
// deletes it.
var ErrUnmanagedPolicy = errors.New("cliiam: a policy named " + PolicyName + " exists but lacks the " +
	ManagedTagKey + "=" + ManagedTagValue + " tag, so lagotto did not create it")

// Outcome is what [EnsurePolicy] did.
type Outcome struct {
	ARN string
	// Created: the policy did not exist and was created.
	Created bool
	// Updated: a new default version was written.
	Updated bool
	// DeletedVersion is the version removed to make room (IAM keeps at most 5).
	DeletedVersion string
}

func isNoSuchEntity(err error) bool {
	var nse *iamtypes.NoSuchEntityException
	return errors.As(err, &nse)
}

func hasManagedTag(tags []iamtypes.Tag) bool {
	for _, t := range tags {
		if aws.ToString(t.Key) == ManagedTagKey && aws.ToString(t.Value) == ManagedTagValue {
			return true
		}
	}
	return false
}

// ReadPolicy returns the default version's document of the deployed CLI-user
// policy (URL-decoded JSON as IAM returns it, which runtimeiam.ParsePolicy
// accepts). found is false when the policy does not exist.
func ReadPolicy(ctx context.Context, client PolicyAPI, accountID string) (doc string, found bool, err error) {
	p, err := client.GetPolicy(ctx, &iam.GetPolicyInput{PolicyArn: aws.String(PolicyARN(accountID))})
	if err != nil {
		if isNoSuchEntity(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("cliiam: get policy %s: %w", PolicyName, err)
	}
	if p.Policy == nil || !hasManagedTag(p.Policy.Tags) {
		return "", true, ErrUnmanagedPolicy
	}
	v, err := client.GetPolicyVersion(ctx, &iam.GetPolicyVersionInput{
		PolicyArn: aws.String(PolicyARN(accountID)), VersionId: p.Policy.DefaultVersionId,
	})
	if err != nil {
		return "", true, fmt.Errorf("cliiam: read the default version of %s: %w", PolicyName, err)
	}
	if v.PolicyVersion == nil {
		return "", true, fmt.Errorf("cliiam: the default version of %s has no document", PolicyName)
	}
	return aws.ToString(v.PolicyVersion.Document), true, nil
}

// EnsurePolicy creates [PolicyName] with doc, or, when it exists with a
// different default document, writes doc as a new default version. A deployed
// document that is semantically identical (runtimeiam.DiffPolicy) is left
// alone, so a re-run of setup does not churn versions. When the policy already
// has five versions, the oldest non-default one is deleted first.
func EnsurePolicy(ctx context.Context, client PolicyAPI, accountID, doc string) (Outcome, error) {
	out := Outcome{ARN: PolicyARN(accountID)}
	deployed, found, err := ReadPolicy(ctx, client, accountID)
	if err != nil {
		return out, err
	}
	if !found {
		if _, err := client.CreatePolicy(ctx, &iam.CreatePolicyInput{
			PolicyName:     aws.String(PolicyName),
			Path:           aws.String(PolicyPath),
			Description:    aws.String(PolicyDescription),
			PolicyDocument: aws.String(doc),
			Tags: []iamtypes.Tag{
				{Key: aws.String("Application"), Value: aws.String("lagotto")},
				{Key: aws.String(ManagedTagKey), Value: aws.String(ManagedTagValue)},
			},
		}); err != nil {
			return out, fmt.Errorf("cliiam: create policy %s: %w", PolicyName, err)
		}
		out.Created = true
		return out, nil
	}

	if diff, derr := runtimeiam.DiffPolicy(deployed, doc); derr == nil && diff.Empty() {
		return out, nil
	}

	deleted, err := makeRoomForVersion(ctx, client, out.ARN)
	out.DeletedVersion = deleted
	if err != nil {
		return out, err
	}
	if _, err := client.CreatePolicyVersion(ctx, &iam.CreatePolicyVersionInput{
		PolicyArn: aws.String(out.ARN), PolicyDocument: aws.String(doc), SetAsDefault: true,
	}); err != nil {
		return out, fmt.Errorf("cliiam: write a new version of %s: %w", PolicyName, err)
	}
	out.Updated = true
	return out, nil
}

// listVersions returns every version of the policy.
func listVersions(ctx context.Context, client iam.ListPolicyVersionsAPIClient, arn string) ([]iamtypes.PolicyVersion, error) {
	var all []iamtypes.PolicyVersion
	for p := iam.NewListPolicyVersionsPaginator(client, &iam.ListPolicyVersionsInput{PolicyArn: aws.String(arn)}); p.HasMorePages(); {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		all = append(all, page.Versions...)
	}
	return all, nil
}

// makeRoomForVersion deletes the oldest non-default version when the policy
// is at IAM's version limit, and returns its ID ("" when nothing was deleted).
func makeRoomForVersion(ctx context.Context, client PolicyAPI, arn string) (string, error) {
	versions, err := listVersions(ctx, client, arn)
	if err != nil {
		return "", fmt.Errorf("cliiam: list versions of %s: %w", PolicyName, err)
	}
	if len(versions) < maxPolicyVersions {
		return "", nil
	}
	var candidates []iamtypes.PolicyVersion
	for _, v := range versions {
		if !v.IsDefaultVersion {
			candidates = append(candidates, v)
		}
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("cliiam: %s has %d versions and none is deletable", PolicyName, len(versions))
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return aws.ToTime(candidates[i].CreateDate).Before(aws.ToTime(candidates[j].CreateDate))
	})
	oldest := aws.ToString(candidates[0].VersionId)
	if _, err := client.DeletePolicyVersion(ctx, &iam.DeletePolicyVersionInput{
		PolicyArn: aws.String(arn), VersionId: aws.String(oldest),
	}); err != nil && !isNoSuchEntity(err) {
		return "", fmt.Errorf("cliiam: delete old version %s of %s: %w", oldest, PolicyName, err)
	}
	return oldest, nil
}
