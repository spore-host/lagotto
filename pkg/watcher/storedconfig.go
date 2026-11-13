package watcher

// Stored launch configs the poller will refuse (#309).
//
// lagotto 0.70.0 refuses inline IAM (iam_policy, iam_policy_file,
// iam_policy_document) when a watch or scheduled launch is created, and the
// poller refuses it again before every launch (ValidateLaunchPrivileges). A
// watch or launch armed under an earlier release still carries what it was
// armed with, and the poller refuses it at the worst moment: when capacity
// finally appears, or when a Capacity Block opens. The same holds for an
// iam_role the deployed poller is not authorized to pass. These helpers find
// such records before that, for `lagotto list` and `lagotto doctor`, and give
// the one fix that works for each.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/spore-host/lagotto/pkg/failure"
)

// Finding kinds.
const (
	FindingInlineIAM        = "inline-iam"        // iam_policy, iam_policy_file or iam_policy_document
	FindingRoleUnusable     = "role-unusable"     // an iam_role no rule allows (reserved, an ARN, a wildcard)
	FindingRoleUnauthorized = "role-unauthorized" // an iam_role the deployed poller may not pass
	FindingOtherRefusal     = "refused"           // any other rule the poller enforces at launch
)

// LaunchConfigFinding is one stored watch or scheduled launch whose launch
// config the poller will refuse, with what to run instead.
type LaunchConfigFinding struct {
	// ID is the watch or scheduled launch id (its full key).
	ID string `json:"id"`
	// Kind is a Finding* constant.
	Kind string `json:"kind"`
	// Role is the iam_role the config names, if any.
	Role string `json:"role,omitempty"`
	// Reason is the poller's refusal, word for word.
	Reason string `json:"reason"`
	// Fix is the commands that resolve it, in order.
	Fix []string `json:"fix"`
}

// StoredConfigRole returns the iam_role a stored launch config names, "" for
// none or for a config that cannot be read.
func StoredConfigRole(raw []byte) string {
	var file SpawnConfigFile
	if len(raw) == 0 || json.Unmarshal(raw, &file) != nil {
		return ""
	}
	return strings.TrimSpace(file.IAMRole)
}

// CheckStoredLaunchConfig applies to a stored launch config the rules the
// poller applies before it launches one (Spawner.Spawn, LaunchScheduled), and
// returns the finding, nil when the rules pass. id is the watch or scheduled
// launch, for the fix text. Unknown keys are tolerated, as the poller
// tolerates them. Whether a named iam_role is authorized is a separate check
// ([RoleUnauthorizedFinding]): it needs the deployed policy.
func CheckStoredLaunchConfig(id string, raw []byte) *LaunchConfigFinding {
	if len(raw) == 0 {
		return nil
	}
	var file SpawnConfigFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return &LaunchConfigFinding{ID: id, Kind: FindingOtherRefusal, Reason: fmt.Sprintf("unmarshal spawn config: %v", err),
			Fix: []string{fmt.Sprintf("lagotto update %s --spawn-config <file>", ShortID(id))}}
	}
	role := strings.TrimSpace(file.IAMRole)
	err := file.ValidateFSxBucket()
	if err == nil {
		err = file.ValidateLaunchPrivileges()
	}
	if err == nil {
		return nil
	}
	f := &LaunchConfigFinding{ID: id, Role: role, Reason: err.Error()}
	switch {
	case len(file.IAMPolicies) > 0 || strings.TrimSpace(file.IAMPolicyFile) != "" || strings.TrimSpace(file.IAMPolicyDocument) != "":
		f.Kind = FindingInlineIAM
		f.Fix = InlineIAMFix(id, role)
	case role != "" && ValidateInstanceRoleReference(role) != nil:
		f.Kind = FindingRoleUnusable
		f.Fix = []string{
			"in the spawn config, set iam_role: <role> to a plain role name an administrator authorized ('lagotto setup --instance-role <role>')",
			fmt.Sprintf("lagotto update %s --spawn-config <file>", ShortID(id)),
		}
	default:
		f.Kind = FindingOtherRefusal
		f.Fix = []string{fmt.Sprintf("fix the spawn config, then: lagotto update %s --spawn-config <file>", ShortID(id))}
	}
	return f
}

// InlineIAMFix is how a config with inline IAM is replaced: an administrator
// provides and authorizes a role, the config names it, and the stored record
// is updated in place. role is the iam_role the config already names, if any.
func InlineIAMFix(id, role string) []string {
	if role == "" {
		role = "<role>"
	}
	return []string{
		fmt.Sprintf("an administrator creates a role with the permissions the workload needs and authorizes it: lagotto setup --instance-role %s", role),
		fmt.Sprintf("in the spawn config, remove iam_policy, iam_policy_file and iam_policy_document, and set iam_role: %s", role),
		fmt.Sprintf("lagotto update %s --spawn-config <file>", ShortID(id)),
	}
}

// RoleUnauthorizedFinding is the finding for a config whose iam_role the
// deployed poller policy does not authorize; missing are the actions it
// lacks.
func RoleUnauthorizedFinding(id, role string, missing []string) *LaunchConfigFinding {
	return &LaunchConfigFinding{
		ID: id, Kind: FindingRoleUnauthorized, Role: role,
		Reason: fmt.Sprintf("iam_role %q is not authorized for the hosted poller (missing %s); the launch would fail with AccessDenied",
			role, strings.Join(missing, ", ")),
		Fix: []string{
			fmt.Sprintf("an administrator authorizes it: lagotto setup --instance-role %s", role),
			fmt.Sprintf("or name a role that is authorized in the spawn config's iam_role, then: lagotto update %s --spawn-config <file>", ShortID(id)),
		},
	}
}

// Lines renders a finding for a terminal: the id and reason, then each fix.
func (f *LaunchConfigFinding) Lines() []string {
	out := []string{fmt.Sprintf("%s: %s", ShortID(f.ID), f.Reason)}
	for i, step := range f.Fix {
		out = append(out, fmt.Sprintf("  %d. %s", i+1, step))
	}
	return out
}

// IsConfigRefused reports whether err is the poller refusing a stored launch
// config on its own rules (failure.ErrConfigRefused), before any AWS call.
func IsConfigRefused(err error) bool { return errors.Is(err, failure.ErrConfigRefused) }

// refusalReason is err's text without the sentinel's own prefix, which says
// nothing the subject does not.
func refusalReason(err error) string {
	return strings.TrimPrefix(err.Error(), failure.ErrConfigRefused.Error()+": ")
}

// NotifyConfigRefused tells a watch's owner that the poller stopped it,
// without launching anything, because its stored launch config breaks a rule
// the poller enforces before every launch (#309): the refusal word for word,
// and the fix. It replaces the match notification, which would say capacity
// was found and nothing more. Same channels and webhook re-validation as
// Notify; once, because the poller marks the watch failed.
func (n *Notifier) NotifyConfigRefused(ctx context.Context, w *Watch, m *MatchResult, err error) error {
	if len(w.NotifyChannels) == 0 {
		return nil
	}
	reason := refusalReason(err)
	f := CheckStoredLaunchConfig(w.WatchID, w.LaunchConfigJSON)
	fix := []string{"fix the spawn config, then create a new watch (this one has stopped): lagotto watch ... --spawn-config <file>"}
	if f != nil && f.Kind == FindingInlineIAM {
		fix = InlineIAMFix(w.WatchID, f.Role)
		fix[len(fix)-1] = "create a new watch with that spawn config (this one has stopped): lagotto watch ... --spawn-config <file>"
	}
	subject, body := configRefusedSubjectBody(w, m, reason, fix)
	payload, jerr := json.Marshal(map[string]any{
		"watch_id": w.WatchID, "event": "config_refused", "pattern": w.InstanceTypePattern,
		"instance_type": m.InstanceType, "region": m.Region, "reason": reason, "fix": fix,
	})
	if jerr != nil {
		return fmt.Errorf("marshal refusal payload: %w", jerr)
	}
	return n.fanOut(ctx, w, subject, body, payload)
}

// configRefusedSubjectBody renders the refusal email. Pure, for tests.
func configRefusedSubjectBody(w *Watch, m *MatchResult, reason string, fix []string) (string, string) {
	subject := fmt.Sprintf("[lagotto] watch %s stopped: its launch config was refused", ShortID(w.WatchID))
	var steps strings.Builder
	for i, s := range fix {
		fmt.Fprintf(&steps, "  %d. %s\n", i+1, s)
	}
	body := fmt.Sprintf(`Your watch %s found %s in %s, but lagotto did not launch it, and the watch has stopped (status: failed).

The hosted poller checks every stored launch config before it launches, and refused this one:

  %s

Nothing was launched and nothing is billing. To launch with this config:

%s
Watch Pattern: %s
`, w.WatchID, m.InstanceType, m.Region, reason, steps.String(), w.InstanceTypePattern)
	return subject, body
}
