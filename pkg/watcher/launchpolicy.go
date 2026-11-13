package watcher

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	spawnaws "github.com/spore-host/spawn/pkg/aws"
)

// DefaultInstanceRoleName is the IAM role, and the identically-named instance
// profile, that a watch's instance launches with when its spawn_config names no
// iam_role. `lagotto setup` creates it (with spawn's spored/SSM baseline and
// nothing else), the hosted poller may only pass it to EC2, and `lagotto
// teardown --iam-roles` deletes it. runtimeiam.InstanceRoleName is the same
// name; a runtimeiam test holds them equal.
//
// It is not spawn's shared spored-instance-role: every bare `spawn launch` in
// the account rewrites that role, and anything an account attaches to it would
// reach every lagotto user. A role lagotto alone owns grants exactly what
// lagotto wrote.
const DefaultInstanceRoleName = "lagotto-instance-role"

// launchPrivilegeHint is how a refused IAM setting is replaced: an
// administrator provides the role, and the watch names it.
const launchPrivilegeHint = "the hosted poller never creates or changes IAM roles. Ask an administrator to create a role " +
	"with the permissions the workload needs and authorize it with 'lagotto setup --instance-role NAME', then set iam_role: NAME"

// instanceRoleNamePattern is the subset of IAM's role-name grammar a watch may
// name. No wildcard, no path, no ARN: the name is matched exactly against the
// roles an administrator authorized.
var instanceRoleNamePattern = regexp.MustCompile(`^[A-Za-z0-9+=,.@_-]{1,64}$`)

// ReservedInstanceRole returns why a role name may never be a lagotto
// instance role, or "" when it may be. These are roles whose grants other
// tools own or whose trust is not EC2:
//
//   - spored-instance-role / spored*: spawn's shared default role, used by
//     every bare `spawn launch` in the account and rewritten by each one.
//   - spawn's nested-spawn roles, which let an instance launch instances;
//     spawn creates them only through its own audited path.
//   - lagotto's own poller and scheduler roles.
//
// [DefaultInstanceRoleName] is not reserved here; it is simply not something a
// watch needs to name.
func ReservedInstanceRole(name string) string {
	n := strings.ToLower(strings.TrimSpace(name))
	switch {
	case strings.HasPrefix(n, "spored"):
		return "it is spawn's shared default instance role (spored*), which every 'spawn launch' in the account uses"
	case spawnaws.IsNestedSpawnRole(strings.TrimSpace(name)):
		return "it is a spawn nested-spawn role, which lets an instance launch instances"
	case strings.HasPrefix(n, "lagotto-capacity-poller"):
		return "it is one of lagotto's own poller roles"
	}
	return ""
}

// ValidateInstanceRoleReference checks a role name a watch, or an
// administrator's `setup --instance-role`, names: a plain IAM role name that
// is not reserved ([ReservedInstanceRole]).
func ValidateInstanceRoleReference(name string) error {
	t := strings.TrimSpace(name)
	switch {
	case t == "":
		return fmt.Errorf("instance role name is empty")
	case strings.ContainsAny(t, "*?"):
		return fmt.Errorf("instance role name %q contains a wildcard; name the role exactly", name)
	case strings.Contains(t, "/") || strings.HasPrefix(t, "arn:"):
		return fmt.Errorf("%q looks like an ARN or path; pass just the role name", name)
	case !instanceRoleNamePattern.MatchString(t):
		return fmt.Errorf("%q is not a valid IAM role name", name)
	}
	if why := ReservedInstanceRole(t); why != "" {
		return fmt.Errorf("iam_role %q is not allowed: %s", t, why)
	}
	return nil
}

// ValidateLaunchPrivileges refuses every spawn_config setting that would have
// the poller author IAM, read its own filesystem, or launch with a role an
// administrator did not provide. It runs when a watch or scheduled launch is
// CREATED (after [SpawnConfigFile.MakeSelfContained]), and again by the poller
// before every launch, because a stored item can be edited in DynamoDB
// directly. At launch the error is wrapped in failure.ErrConfigRefused, so the
// watch stops as terminal before any AWS call.
//
// Refused:
//
//   - iam_policy, iam_policy_file, iam_policy_document: each makes spawn create
//     a role, or rewrite an existing one, with a policy the watch supplies. The
//     poller would be writing IAM on behalf of a user who cannot.
//   - an iam_role that is reserved or malformed ([ValidateInstanceRoleReference]).
//     Whether the role is one an administrator authorized is enforced by IAM:
//     the poller may pass only those roles (runtimeiam.PolicyDocument).
//   - user_data_file, or a user_data of the "@path" form: a stored config is
//     self-contained (#132), so a path here can only name a file on the
//     poller's own host, e.g. the Lambda's environment and credentials.
//   - a key_name that is not a plain EC2 key-pair name.
//
// Allowed, and why: command, user_data (inline), ami and key_name give code
// execution or a login on an instance, which is what a watch is for. What that
// instance can do in AWS is bounded by its role, which is now always one an
// administrator provided.
func (s *SpawnConfigFile) ValidateLaunchPrivileges() error {
	var refused []string
	if len(s.IAMPolicies) > 0 {
		refused = append(refused, "iam_policy")
	}
	if strings.TrimSpace(s.IAMPolicyFile) != "" {
		refused = append(refused, "iam_policy_file")
	}
	if strings.TrimSpace(s.IAMPolicyDocument) != "" {
		refused = append(refused, "iam_policy_document")
	}
	if len(refused) > 0 {
		return fmt.Errorf("spawn config sets %s: %s", strings.Join(refused, ", "), launchPrivilegeHint)
	}
	if s.IAMRole != "" {
		if err := ValidateInstanceRoleReference(s.IAMRole); err != nil {
			return err
		}
	}
	if strings.TrimSpace(s.UserDataFile) != "" || strings.HasPrefix(s.UserData, "@") {
		return fmt.Errorf("spawn config references a user-data file; a stored watch must carry its user data inline " +
			"(lagotto reads the file when the watch is created, on your machine), and the poller never reads its own filesystem")
	}
	if err := validateKeyName(s.KeyName); err != nil {
		return err
	}
	return nil
}

// keyNamePattern: EC2 key-pair names are up to 255 ASCII characters; this
// refuses control characters and leading/trailing space.
var keyNamePattern = regexp.MustCompile(`^[\x21-\x7e]([\x20-\x7e]{0,253}[\x21-\x7e])?$`)

func validateKeyName(name string) error {
	if name == "" {
		return nil
	}
	if !keyNamePattern.MatchString(name) {
		return fmt.Errorf("key_name %q is not a valid EC2 key pair name", name)
	}
	return nil
}

// InstanceProfileName is the instance profile a launch from this config uses:
// the iam_role it names (spawn's convention: the profile is named after its
// role), or [DefaultInstanceRoleName]. Always non-empty, so spawn's launcher
// never runs its own IAM setup on the poller's path.
func (s *SpawnConfigFile) InstanceProfileName() string {
	if r := strings.TrimSpace(s.IAMRole); r != "" {
		return r
	}
	return DefaultInstanceRoleName
}

// roleARNPattern is an IAM role ARN, with an optional path. Account is group 1,
// the path-and-name group 2.
var roleARNPattern = regexp.MustCompile(`^arn:aws[a-z-]*:iam::([0-9]{12}):role/([A-Za-z0-9+=,.@_/-]{1,512})$`)

// ParseRoleARN splits an IAM role ARN into its account and its name (the last
// path segment), refusing wildcards and anything that is not a role ARN.
func ParseRoleARN(arn string) (account, name string, err error) {
	t := strings.TrimSpace(arn)
	if strings.ContainsAny(t, "*?") {
		return "", "", fmt.Errorf("role ARN %q contains a wildcard", arn)
	}
	m := roleARNPattern.FindStringSubmatch(t)
	if m == nil {
		return "", "", fmt.Errorf("%q is not an IAM role ARN (arn:aws:iam::<account>:role/<name>)", arn)
	}
	path := m[2]
	name = path[strings.LastIndex(path, "/")+1:]
	if name == "" || strings.Contains(path, "//") {
		return "", "", fmt.Errorf("%q is not an IAM role ARN", arn)
	}
	return m[1], name, nil
}

// SageMakerJobRoleARN returns the execution role a stored SageMaker job spec
// names, validated: present, an IAM role ARN, and not a reserved role. Called
// when the watch is created and again by the poller before each submit.
// Whether an administrator authorized the role is enforced by IAM: the poller
// may pass to SageMaker only the roles `lagotto setup --sagemaker-role` named.
func SageMakerJobRoleARN(jobJSON []byte) (string, error) {
	var spec struct {
		RoleArn string `json:"RoleArn"`
	}
	if err := json.Unmarshal(jobJSON, &spec); err != nil {
		return "", fmt.Errorf("parse sagemaker job: %w", err)
	}
	if strings.TrimSpace(spec.RoleArn) == "" {
		return "", fmt.Errorf("sagemaker job has no RoleArn: name an execution role an administrator authorized with 'lagotto setup --sagemaker-role ARN'")
	}
	_, name, err := ParseRoleARN(spec.RoleArn)
	if err != nil {
		return "", fmt.Errorf("sagemaker job RoleArn: %w", err)
	}
	if why := ReservedInstanceRole(name); why != "" {
		return "", fmt.Errorf("sagemaker job RoleArn %q is not allowed: %s", spec.RoleArn, why)
	}
	return strings.TrimSpace(spec.RoleArn), nil
}
