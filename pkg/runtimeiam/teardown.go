package runtimeiam

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spore-host/libs/tags"

	"github.com/spore-host/lagotto/pkg/awsarn"
)

// The IAM for leave-no-trace idle teardown (lagotto#325, for lagotto#326):
// once nothing lagotto manages has been active in a region for the grace
// period, the poller removes what lagotto created there, and one-shot
// EventBridge Scheduler jobs remove what the poller cannot remove from inside
// itself.
//
// The poller's share is in [PolicyDocumentFor]. The jobs run as the Scheduler
// invoke role, whose teardown grants are a second inline policy,
// [SchedulerTeardownPolicyName], so the InvokeLambda policy `lagotto doctor`
// evaluates keeps its one statement.
//
// Both roles also carry [guardStatements]: explicit Denies no Allow can
// outweigh, on spore.host's own live service and on the permissions boundary
// that is the one permanent piece of a spore.host deployment.
const (
	// LifecycleTableName is the shared spore.host lease table (libs#77), one
	// per region, where a tool claims the right to tear itself down.
	LifecycleTableName = "spore-host-lifecycle"
	// TeardownScheduleGroup is the schedule group the one-shot teardown jobs
	// are created in (lagotto#326), apart from lagotto's own group, which the
	// poller deletes.
	TeardownScheduleGroup = "lagotto-teardown"
	// SchedulerTeardownPolicyName is the inline policy on the Scheduler invoke
	// role that lets a teardown job delete the poller.
	SchedulerTeardownPolicyName = "LagottoTeardown"
	// artifactBucketPrefix is the prefix of the bucket `lagotto deploy`
	// creates. It must agree with deploy.DefaultBucketName, which imports
	// this package; pkg/deploy's tests assert the two agree.
	artifactBucketPrefix = "lagotto-lambda-"
	// pollerLogGroupName is the poller's log group. pkg/deploy owns the same
	// constant (deploy.PollerLogGroupName); its tests assert they agree.
	pollerLogGroupName = "/aws/lambda/" + pollerFunctionName
	// BoundaryPolicyName is spawn's permissions boundary, the one piece of a
	// spore.host deployment that is never torn down
	// (spore-host/spore-host#752). Path and name.
	BoundaryPolicyName = "spore-host/spawn-instance-boundary"
)

// ArtifactBucketARN is the ARN pattern of every region's artifact bucket in
// accountID, in partition: lagotto-lambda-<account>-<region>, with the region
// a wildcard.
func ArtifactBucketARN(partition, accountID string) string {
	return awsarn.S3(partition, artifactBucketPrefix+accountID+"-*")
}

// BoundaryPolicyARN is [BoundaryPolicyName]'s ARN in accountID, in partition.
func BoundaryPolicyARN(partition, accountID string) string {
	return awsarn.IAM(partition, accountID, "policy/"+BoundaryPolicyName)
}

// serviceGuardCondition matches a resource tagged spore-host:role=service,
// spore.host's own live service (spore-host#750). lagotto never writes that
// tag (pkg/managedtags), so the Deny it gates never touches lagotto's
// resources. A service whose resources carry no aws:ResourceTag leaves the key
// absent, StringEquals is then false, and the Deny does not apply: Action "*"
// is therefore exactly "every action of every service that supports it".
func serviceGuardCondition() map[string]interface{} {
	return map[string]interface{}{"StringEquals": map[string]string{"aws:ResourceTag/" + tags.Role: tags.RoleService}}
}

// boundaryWriteActions are every IAM write that takes a managed policy as its
// resource. Wildcards, so a write IAM adds later is denied too; on the
// boundary's ARN no read can match them.
var boundaryWriteActions = []string{
	"iam:Create*", "iam:Delete*", "iam:Put*", "iam:Set*", "iam:Tag*", "iam:Untag*",
	"iam:Update*", "iam:Attach*", "iam:Detach*",
}

// guardStatements are the explicit Denies on both CLI-owned poller roles
// (lagotto#325): nothing on a resource tagged spore-host:role=service, and no
// IAM write on spawn's permissions boundary. An explicit Deny wins over every
// Allow, so a later grant, written in error or wider than meant, cannot reach
// either.
func guardStatements(partition, accountID string) []statement {
	return []statement{
		{Effect: "Deny", Action: []string{"*"}, Resource: "*", Condition: serviceGuardCondition()},
		{Effect: "Deny", Action: boundaryWriteActions, Resource: BoundaryPolicyARN(partition, accountID)},
	}
}

// SchedulerTeardownPolicyDocument is [SchedulerTeardownPolicyName], the
// second inline policy on the Scheduler invoke role: what lagotto#326's
// one-shot teardown jobs, which run as that role, delete once the poller has
// planned its own removal: the poller function and its log group (in any
// region, as the role is shared). Nothing in IAM: the account-wide resources
// are the CLI's to remove (lagotto#340). Plus [guardStatements].
//
// Like the InvokeLambda policy it is a function of the account (and its
// partition) alone, so every region's setup or deploy writes the same bytes
// (#218).
func SchedulerTeardownPolicyDocument(partition, accountID string) (string, error) {
	if partition == "" || accountID == "" {
		return "", fmt.Errorf("runtimeiam: partition and accountID are required for the scheduler teardown policy")
	}
	logGroup := RegionalARN(partition, "logs", accountID, "log-group:"+pollerLogGroupName)
	statements := []statement{
		{Effect: "Allow", Action: []string{"lambda:DeleteFunction"}, Resource: SchedulerInvokeFunctionARN(partition, accountID)},
		// Both forms: CloudWatch Logs authorizes some actions against the
		// group's ARN and some against it with a trailing ":*".
		{Effect: "Allow", Action: []string{"logs:DeleteLogGroup"}, Resource: []string{logGroup, logGroup + ":*"}},
	}
	statements = append(statements, guardStatements(partition, accountID)...)
	b, err := json.Marshal(map[string]interface{}{"Version": "2012-10-17", "Statement": statements})
	if err != nil {
		return "", fmt.Errorf("runtimeiam: build scheduler teardown policy: %w", err)
	}
	return string(b), nil
}

// isServiceGuard reports whether g is the spore-host:role=service Deny of
// [guardStatements]. It never applies to lagotto's own resources, which never
// carry that tag, so the local evaluators ([Allows], the drift check) must not
// read it as denying everything lagotto is granted.
func isServiceGuard(g Grant) bool {
	if !strings.EqualFold(g.Effect, "Deny") || g.Action != "*" || g.Resource != "*" {
		return false
	}
	raw, err := json.Marshal(serviceGuardCondition())
	if err != nil {
		return false
	}
	return g.Condition == canonicalJSON(raw)
}
