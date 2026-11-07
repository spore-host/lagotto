package cmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"

	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

func stubPollerRegions(t *testing.T, regions []string, err error) {
	t.Helper()
	old := findPollerRegions
	findPollerRegions = func(context.Context, aws.Config) ([]string, error) { return regions, err }
	t.Cleanup(func() { findPollerRegions = old })
}

// TestTeardownIAMRolesRefusesWhileAPollerExists: the roles are shared by every
// region's poller, so `teardown --iam-roles` must refuse while any exists, and
// name the command that removes each one.
func TestTeardownIAMRolesRefusesWhileAPollerExists(t *testing.T) {
	stubPollerRegions(t, []string{"us-east-1", "us-west-2"}, nil)
	err := checkNoPollerAnywhere(context.Background(), aws.Config{})
	if err == nil {
		t.Fatal("did not refuse with pollers in two regions")
	}
	for _, want := range []string{"us-east-1, us-west-2", "lagotto deploy --teardown --region us-east-1", "lagotto deploy --teardown --region us-west-2"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal lacks %q:\n%v", want, err)
		}
	}
}

// TestTeardownIAMRolesFailsClosed: a region that cannot be checked is not
// treated as having no poller.
func TestTeardownIAMRolesFailsClosed(t *testing.T) {
	stubPollerRegions(t, nil, errors.New("could not check for a poller in: eu-west-1 (AccessDenied)"))
	err := checkNoPollerAnywhere(context.Background(), aws.Config{})
	if err == nil || !strings.Contains(err.Error(), "eu-west-1") {
		t.Errorf("err = %v, want a refusal naming the unchecked region", err)
	}
}

func TestTeardownIAMRolesProceedsWithNoPoller(t *testing.T) {
	stubPollerRegions(t, nil, nil)
	if err := checkNoPollerAnywhere(context.Background(), aws.Config{}); err != nil {
		t.Errorf("refused with no poller anywhere: %v", err)
	}
}

// cmdRoleDeleter is a minimal IAM for deleteCLIOwnedRoles: roles with no
// policies, so only DeleteRole matters.
type cmdRoleDeleter struct{ exists map[string]bool }

func (f *cmdRoleDeleter) ListRolePolicies(_ context.Context, in *iam.ListRolePoliciesInput, _ ...func(*iam.Options)) (*iam.ListRolePoliciesOutput, error) {
	if !f.exists[aws.ToString(in.RoleName)] {
		return nil, &iamtypes.NoSuchEntityException{}
	}
	return &iam.ListRolePoliciesOutput{}, nil
}

func (f *cmdRoleDeleter) DeleteRolePolicy(context.Context, *iam.DeleteRolePolicyInput, ...func(*iam.Options)) (*iam.DeleteRolePolicyOutput, error) {
	return &iam.DeleteRolePolicyOutput{}, nil
}

func (f *cmdRoleDeleter) ListAttachedRolePolicies(context.Context, *iam.ListAttachedRolePoliciesInput, ...func(*iam.Options)) (*iam.ListAttachedRolePoliciesOutput, error) {
	return &iam.ListAttachedRolePoliciesOutput{}, nil
}

func (f *cmdRoleDeleter) DetachRolePolicy(context.Context, *iam.DetachRolePolicyInput, ...func(*iam.Options)) (*iam.DetachRolePolicyOutput, error) {
	return &iam.DetachRolePolicyOutput{}, nil
}

func (f *cmdRoleDeleter) DeleteRole(_ context.Context, in *iam.DeleteRoleInput, _ ...func(*iam.Options)) (*iam.DeleteRoleOutput, error) {
	delete(f.exists, aws.ToString(in.RoleName))
	return &iam.DeleteRoleOutput{}, nil
}

func TestDeleteCLIOwnedRolesReportsEach(t *testing.T) {
	f := &cmdRoleDeleter{exists: map[string]bool{runtimeiam.RoleName: true, runtimeiam.SchedulerInvokeRoleName: true}}
	var out bytes.Buffer
	if err := deleteCLIOwnedRoles(context.Background(), f, &out); err != nil {
		t.Fatal(err)
	}
	for _, name := range runtimeiam.CLIOwnedRoles {
		if !strings.Contains(out.String(), "Deleted IAM role "+name) {
			t.Errorf("output does not report deleting %s:\n%s", name, out.String())
		}
	}

	out.Reset()
	if err := deleteCLIOwnedRoles(context.Background(), f, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "No lagotto IAM roles to delete") {
		t.Errorf("a re-run did not say there was nothing to delete:\n%s", out.String())
	}
}

// TestTeardownHasIAMRolesFlag: the deleter is reachable from the CLI.
func TestTeardownHasIAMRolesFlag(t *testing.T) {
	if teardownCmd.Flags().Lookup("iam-roles") == nil {
		t.Fatal("lagotto teardown has no --iam-roles flag; the CLI-owned IAM roles have no deleter (#229)")
	}
}
