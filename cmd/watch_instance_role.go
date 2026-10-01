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
	if watchSpawnConfigIAMRole(w) == "" {
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
// An empty iam_role is the common case and needs no check: spawn's
// CreateOrGetInstanceProfile then derives "spawn-instance-<hash>", which the
// poller policy already covers by prefix.
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

// checkInstanceRoleAuthorized decides, at watch-creation time, whether the hosted
// poller will be able to launch with a watch's named iam_role (#170).
//
// Why this exists: a watch whose spawn_config names a custom role waits for
// capacity — potentially for hours, which is the entire point of the watch — then
// dies at launch with `AccessDenied: iam:GetRole on resource: role <name>`,
// because the poller's policy scopes those actions to spawn-instance*/spored*.
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
	if roleName == "" || client == nil || accountID == "" {
		return nil, nil
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
		fmt.Fprintf(out, "warning: this watch's spawn_config names iam_role %q, but the hosted poller's policy could not be read, so\n"+
			"  whether it can use that role is unknown: %v\n", roleName, err)
		return res, nil
	}

	grants, err := runtimeiam.ParsePolicy(doc)
	if err != nil {
		res.Note = fmt.Sprintf("could not parse the hosted poller's runtime policy: %v", err)
		fmt.Fprintf(out, "warning: could not parse the hosted poller's runtime policy, so iam_role %q was not verified: %v\n", roleName, err)
		return res, nil
	}

	res.Checked = true
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
		"  Authorize it:  lagotto setup --instance-role %s\n"+
		"\n"+
		"  That lets the poller read the role, write its inline policy, attach managed policies to it, and pass\n"+
		"  it to EC2 — spawn adds its spored/SSM baseline to every role it launches with. If you only ever run a\n"+
		"  local 'poll --daemon' under your own credentials, this check does not apply to you",
		roleName, strings.Join(missing, ", "), roleName)
}
