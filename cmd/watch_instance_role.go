package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spore-host/lagotto/pkg/runtimeiam"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// InstanceRoleCheck is the verdict of the create-time instance-role
// authorization check, carried into `-o json` alongside the watch the same way
// the quota report is.
type InstanceRoleCheck struct {
	RoleName string `json:"role_name"`
	// Authorized is true when the deployed poller policy covers every action the
	// launcher needs on this role.
	Authorized bool `json:"authorized"`
	// MissingActions is what the poller would be refused, in the order the
	// launcher attempts them. Empty when Authorized or when unknown.
	MissingActions []string `json:"missing_actions,omitempty"`
	// Checked is false when no hosted poller was found, or the policy could not be
	// read — in which case nothing was proven either way.
	Checked bool `json:"checked"`
	// Note explains an unchecked or warned verdict.
	Note string `json:"note,omitempty"`
}

// checkInstanceRoleAuthorizedFromConfig resolves the clients
// [checkInstanceRoleAuthorized] needs and calls it. Split out so the decision
// logic stays injectable in tests while the call site in `lagotto watch` remains
// one line.
//
// A caller identity that cannot be resolved yields "" and so skips the check: the
// policy's resource ARNs are account-scoped, and guessing an account would be
// worse than not checking.
func checkInstanceRoleAuthorizedFromConfig(ctx context.Context, cfg aws.Config, out io.Writer, w *watcher.Watch) (*InstanceRoleCheck, error) {
	if watchSpawnConfigIAMRole(w) == "" && watchSageMakerRole(w) == "" {
		return nil, nil // the overwhelmingly common case — don't call STS for nothing
	}
	accountID := ""
	if id, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}); err == nil {
		accountID = aws.ToString(id.Account)
	}
	return checkInstanceRoleAuthorized(ctx, iam.NewFromConfig(cfg), out, w, accountID)
}

// watchSpawnConfigIAMRole returns the iam_role a watch's stored spawn config
// names, or "" when it names none.
//
// An empty iam_role is the common case and needs no check: the watch then
// launches with lagotto's own instance role (watcher.DefaultInstanceRoleName),
// which the poller policy always covers.
func watchSpawnConfigIAMRole(w *watcher.Watch) string {
	if w == nil || len(w.LaunchConfigJSON) == 0 {
		return ""
	}
	var file watcher.SpawnConfigFile
	if err := json.Unmarshal(w.LaunchConfigJSON, &file); err != nil {
		return ""
	}
	return strings.TrimSpace(file.IAMRole)
}

// watchSageMakerRole returns the RoleArn of a SageMaker watch's job spec, or "".
func watchSageMakerRole(w *watcher.Watch) string {
	if w == nil || w.Service != watcher.ServiceSageMaker || len(w.SageMakerJobJSON) == 0 {
		return ""
	}
	arn, err := watcher.SageMakerJobRoleARN(w.SageMakerJobJSON)
	if err != nil {
		return ""
	}
	return arn
}

// checkInstanceRoleAuthorized decides, at watch-creation time, whether the hosted
// poller will be able to launch with a watch's named iam_role (#170).
//
// Why this exists: a watch whose spawn_config names a custom role waits for
// capacity — potentially for hours, which is the entire point of the watch — then
// dies at launch with `AccessDenied: iam:GetRole on resource: role <name>`,
// because the poller was not allowed to use that role.
// Three real watches matched g6.8xlarge in under a minute and all three died that
// way, and the failure reads as `spawn_failed` with a raw AWS error, which is not
// recognisable as "an IAM grant is missing".
//
// It refuses only on evidence, never on a guess, because a watch may instead be
// serviced by a local `poll --daemon` running under the user's own credentials:
//
//	policy present, role covered      -> nil (silent, proceed)
//	policy present, role NOT covered  -> error (refuse before the watch is stored)
//	no role / no policy deployed      -> nil (no hosted poller to be wrong about)
//	any other error (e.g. AccessDenied) -> nil + warning (nothing can be proven)
func checkInstanceRoleAuthorized(ctx context.Context, client runtimeiam.IAMAPI, out io.Writer, w *watcher.Watch, accountID string) (*InstanceRoleCheck, error) {
	roleName := watchSpawnConfigIAMRole(w)
	smRole := watchSageMakerRole(w)
	if (roleName == "" && smRole == "") || client == nil || accountID == "" {
		return nil, nil
	}
	if roleName == "" {
		roleName = smRole // reported under the same field
	}

	res := &InstanceRoleCheck{RoleName: roleName}

	doc, err := runtimeiam.ReadRuntimePolicy(ctx, client)
	switch {
	case err == nil:
	case errors.Is(err, runtimeiam.ErrRoleNotFound), errors.Is(err, runtimeiam.ErrPolicyNotFound):
		// No hosted poller: a local `poll --daemon` uses the caller's own
		// credentials, so the poller policy says nothing about whether this works.
		res.Note = "no hosted poller runtime policy found; not checked (a local 'poll --daemon' uses your own credentials)"
		return res, nil
	default:
		res.Note = fmt.Sprintf("could not read the hosted poller's runtime policy: %v", err)
		_, _ = fmt.Fprintf(out, "warning: this watch's spawn_config names iam_role %q, but the hosted poller's policy could not be read, so\n"+
			"  whether it can use that role is unknown: %v\n", roleName, err)
		return res, nil
	}

	grants, err := runtimeiam.ParsePolicy(doc)
	if err != nil {
		res.Note = fmt.Sprintf("could not parse the hosted poller's runtime policy: %v", err)
		_, _ = fmt.Fprintf(out, "warning: could not parse the hosted poller's runtime policy, so iam_role %q was not verified: %v\n", roleName, err)
		return res, nil
	}

	res.Checked = true
	if smRole != "" {
		if runtimeiam.AllowsPassRoleTo(grants, smRole, "sagemaker.amazonaws.com") {
			res.Authorized = true
			return res, nil
		}
		res.MissingActions = []string{runtimeiam.InstanceRolePassAction}
		return res, fmt.Errorf("this watch's SageMaker job names RoleArn %q, but the hosted poller is not authorized to pass it to SageMaker.\n"+
			"  The watch would wait, then fail at submit with AccessDenied.\n"+
			"\n"+
			"  An administrator authorizes it:  lagotto setup --sagemaker-role %s\n"+
			"\n"+
			"  Every lagotto user can then submit jobs with that role", smRole, smRole)
	}
	missing := runtimeiam.MissingInstanceRoleGrants(grants, accountID, roleName)
	if len(missing) == 0 {
		res.Authorized = true
		return res, nil
	}
	res.MissingActions = missing

	// Refused here, BEFORE store.PutWatch, so no doomed watch is ever persisted.
	return res, fmt.Errorf("this watch's spawn_config names iam_role %q, but the hosted poller is not authorized to use it.\n"+
		"  The watch would wait for capacity, match, and then die at launch with AccessDenied.\n"+
		"  Missing: %s\n"+
		"\n"+
		"  An administrator authorizes it:  lagotto setup --instance-role %s\n"+
		"\n"+
		"  Setup creates the role's instance profile and adds spawn's spored/SSM baseline; the poller may then\n"+
		"  only pass the role to EC2. Every lagotto user can launch with an authorized role",
		roleName, strings.Join(missing, ", "), roleName)
}
