package deploy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	schedulertypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"gopkg.in/yaml.v3"

	cfn "github.com/spore-host/lagotto/deployment/cloudformation"
)

// The #226 fixtures are REAL historical lagotto templates (git show of
// deployment/cloudformation/lagotto-stack.yaml at the named commit):
//
//   - 4262315 (2026-07-18) is the shape of the owner's us-east-1 stack: topic,
//     SAM function, schedule, and BOTH account-wide IAM roles, which the
//     current template no longer declares.
//   - 5f7149a (2026-06-17) also owns the two DynamoDB tables, and its SAM
//     function declares Policies, so the transform GENERATES a role
//     (CapacityPollerFunctionRole) that appears only in the processed template.
func fixture(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

const acct = "123456789012"

// --- a fake account ---------------------------------------------------------

// world is a fake account holding every resource type a legacy stack can own,
// keyed by physical ID. It implements the lambda, scheduler, sns, iam and
// dynamodb seams; CloudFormation deleting a resource removes it here.
type world struct {
	functions map[string]*lambda.GetFunctionOutput
	schedules map[string]*scheduler.GetScheduleOutput
	topics    map[string]map[string]string
	roles     map[string]*worldRole
	tables    map[string]*dynamodbtypes.TableDescription
	deleted   []string
}

type worldRole struct {
	role     iamtypes.Role
	inline   map[string]string
	attached []string
}

func notFound(what string) error { return fmt.Errorf("ResourceNotFound: %s", what) }

func (w *world) delete(r cfntypes.StackResourceSummary) {
	id := deref(r.PhysicalResourceId)
	w.deleted = append(w.deleted, deref(r.LogicalResourceId))
	switch deref(r.ResourceType) {
	case "AWS::Lambda::Function":
		delete(w.functions, id)
	case "AWS::Scheduler::Schedule":
		delete(w.schedules, id)
	case "AWS::SNS::Topic":
		delete(w.topics, id)
	case "AWS::IAM::Role":
		delete(w.roles, id)
	case "AWS::DynamoDB::Table":
		delete(w.tables, id)
	}
}

func (w *world) GetFunction(_ context.Context, in *lambda.GetFunctionInput, _ ...func(*lambda.Options)) (*lambda.GetFunctionOutput, error) {
	if f, ok := w.functions[deref(in.FunctionName)]; ok {
		return f, nil
	}
	return nil, &lambdatypes.ResourceNotFoundException{Message: aws.String("Function not found")}
}
func (w *world) CreateFunction(context.Context, *lambda.CreateFunctionInput, ...func(*lambda.Options)) (*lambda.CreateFunctionOutput, error) {
	return nil, errors.New("unexpected CreateFunction")
}
func (w *world) UpdateFunctionCode(context.Context, *lambda.UpdateFunctionCodeInput, ...func(*lambda.Options)) (*lambda.UpdateFunctionCodeOutput, error) {
	return nil, errors.New("unexpected UpdateFunctionCode")
}
func (w *world) UpdateFunctionConfiguration(context.Context, *lambda.UpdateFunctionConfigurationInput, ...func(*lambda.Options)) (*lambda.UpdateFunctionConfigurationOutput, error) {
	return nil, errors.New("unexpected UpdateFunctionConfiguration")
}
func (w *world) DeleteFunction(context.Context, *lambda.DeleteFunctionInput, ...func(*lambda.Options)) (*lambda.DeleteFunctionOutput, error) {
	return nil, errors.New("unexpected DeleteFunction")
}
func (w *world) TagResource(context.Context, *lambda.TagResourceInput, ...func(*lambda.Options)) (*lambda.TagResourceOutput, error) {
	return nil, errors.New("unexpected TagResource")
}

type worldSched struct{ *world }

func (w worldSched) GetSchedule(_ context.Context, in *scheduler.GetScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.GetScheduleOutput, error) {
	if s, ok := w.schedules[deref(in.Name)]; ok {
		return s, nil
	}
	return nil, &schedulertypes.ResourceNotFoundException{Message: aws.String("Schedule not found")}
}
func (w worldSched) CreateSchedule(context.Context, *scheduler.CreateScheduleInput, ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	return nil, errors.New("unexpected CreateSchedule")
}
func (w worldSched) UpdateSchedule(context.Context, *scheduler.UpdateScheduleInput, ...func(*scheduler.Options)) (*scheduler.UpdateScheduleOutput, error) {
	return nil, errors.New("unexpected UpdateSchedule")
}
func (w worldSched) DeleteSchedule(context.Context, *scheduler.DeleteScheduleInput, ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error) {
	return nil, errors.New("unexpected DeleteSchedule")
}
func (w worldSched) ListSchedules(context.Context, *scheduler.ListSchedulesInput, ...func(*scheduler.Options)) (*scheduler.ListSchedulesOutput, error) {
	return nil, errors.New("unexpected ListSchedules")
}

type worldSNS struct{ *world }

func (w worldSNS) GetTopicAttributes(_ context.Context, in *sns.GetTopicAttributesInput, _ ...func(*sns.Options)) (*sns.GetTopicAttributesOutput, error) {
	if a, ok := w.topics[deref(in.TopicArn)]; ok {
		return &sns.GetTopicAttributesOutput{Attributes: a}, nil
	}
	return nil, notFound("topic")
}
func (w worldSNS) CreateTopic(context.Context, *sns.CreateTopicInput, ...func(*sns.Options)) (*sns.CreateTopicOutput, error) {
	return nil, errors.New("unexpected CreateTopic")
}
func (w worldSNS) SetTopicAttributes(context.Context, *sns.SetTopicAttributesInput, ...func(*sns.Options)) (*sns.SetTopicAttributesOutput, error) {
	return nil, errors.New("unexpected SetTopicAttributes")
}
func (w worldSNS) TagResource(context.Context, *sns.TagResourceInput, ...func(*sns.Options)) (*sns.TagResourceOutput, error) {
	return nil, errors.New("unexpected TagResource")
}
func (w worldSNS) DeleteTopic(context.Context, *sns.DeleteTopicInput, ...func(*sns.Options)) (*sns.DeleteTopicOutput, error) {
	return nil, errors.New("unexpected DeleteTopic")
}

func (w *world) GetRole(_ context.Context, in *iam.GetRoleInput, _ ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	if r, ok := w.roles[deref(in.RoleName)]; ok {
		role := r.role
		return &iam.GetRoleOutput{Role: &role}, nil
	}
	return nil, &iamtypes.NoSuchEntityException{Message: aws.String("role not found")}
}
func (w *world) ListRolePolicies(_ context.Context, in *iam.ListRolePoliciesInput, _ ...func(*iam.Options)) (*iam.ListRolePoliciesOutput, error) {
	r, ok := w.roles[deref(in.RoleName)]
	if !ok {
		return nil, &iamtypes.NoSuchEntityException{Message: aws.String("role not found")}
	}
	var names []string
	for n := range r.inline {
		names = append(names, n)
	}
	return &iam.ListRolePoliciesOutput{PolicyNames: names}, nil
}
func (w *world) GetRolePolicy(_ context.Context, in *iam.GetRolePolicyInput, _ ...func(*iam.Options)) (*iam.GetRolePolicyOutput, error) {
	r, ok := w.roles[deref(in.RoleName)]
	if !ok {
		return nil, &iamtypes.NoSuchEntityException{Message: aws.String("role not found")}
	}
	return &iam.GetRolePolicyOutput{PolicyDocument: aws.String(r.inline[deref(in.PolicyName)])}, nil
}
func (w *world) ListAttachedRolePolicies(_ context.Context, in *iam.ListAttachedRolePoliciesInput, _ ...func(*iam.Options)) (*iam.ListAttachedRolePoliciesOutput, error) {
	r, ok := w.roles[deref(in.RoleName)]
	if !ok {
		return nil, &iamtypes.NoSuchEntityException{Message: aws.String("role not found")}
	}
	var out []iamtypes.AttachedPolicy
	for _, a := range r.attached {
		out = append(out, iamtypes.AttachedPolicy{PolicyArn: aws.String(a)})
	}
	return &iam.ListAttachedRolePoliciesOutput{AttachedPolicies: out}, nil
}

func (w *world) DescribeTable(_ context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	if t, ok := w.tables[deref(in.TableName)]; ok {
		return &dynamodb.DescribeTableOutput{Table: t}, nil
	}
	return nil, &dynamodbtypes.ResourceNotFoundException{Message: aws.String("table not found")}
}

// resourcesOf is a stack's ListStackResources view, from (logicalID, type,
// physicalID) triples.
func resourcesOf(triples ...string) []cfntypes.StackResourceSummary {
	var out []cfntypes.StackResourceSummary
	for i := 0; i+2 < len(triples); i += 3 {
		out = append(out, cfntypes.StackResourceSummary{
			LogicalResourceId:  aws.String(triples[i]),
			ResourceType:       aws.String(triples[i+1]),
			PhysicalResourceId: aws.String(triples[i+2]),
			ResourceStatus:     cfntypes.ResourceStatusUpdateComplete,
		})
	}
	return out
}

// populate creates every listed resource in the world, with distinctive state.
func (w *world) populate(rs []cfntypes.StackResourceSummary) {
	for _, r := range rs {
		id := deref(r.PhysicalResourceId)
		switch deref(r.ResourceType) {
		case "AWS::Lambda::Function":
			w.functions[id] = &lambda.GetFunctionOutput{Configuration: &lambdatypes.FunctionConfiguration{
				FunctionArn: aws.String(PollerFunctionARN("us-east-1", acct)), Role: aws.String("arn:aws:iam::" + acct + ":role/lagotto-capacity-poller-role"),
				Runtime: lambdatypes.RuntimeProvidedal2023, Handler: aws.String("bootstrap"), Timeout: aws.Int32(900), MemorySize: aws.Int32(512),
				CodeSha256:  aws.String("W8ZesHJJ"),
				Environment: &lambdatypes.EnvironmentResponse{Variables: map[string]string{"SCHEDULE_NAME": "lagotto-capacity-poller"}},
			}}
		case "AWS::Scheduler::Schedule":
			w.schedules[id] = &scheduler.GetScheduleOutput{Name: aws.String(id), State: schedulertypes.ScheduleStateEnabled,
				ScheduleExpression: aws.String("rate(5 minutes)"),
				Target:             &schedulertypes.Target{Arn: aws.String(PollerFunctionARN("us-east-1", acct)), RoleArn: aws.String(schedulerInvokeRoleARN(acct))}}
		case "AWS::SNS::Topic":
			w.topics[id] = map[string]string{"TopicArn": id, "DisplayName": "Lagotto Capacity Alerts", "SubscriptionsConfirmed": "1"}
		case "AWS::IAM::Role":
			w.roles[id] = &worldRole{
				role:     iamtypes.Role{RoleName: aws.String(id), RoleId: aws.String("AROA" + strings.ToUpper(id[:4])), Arn: aws.String("arn:aws:iam::" + acct + ":role/" + id), AssumeRolePolicyDocument: aws.String("%7B%7D")},
				inline:   map[string]string{"InvokeLambda": `{"Statement":[]}`},
				attached: []string{"arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"},
			}
		case "AWS::DynamoDB::Table":
			w.tables[id] = &dynamodbtypes.TableDescription{TableName: aws.String(id), TableId: aws.String("tbl-" + id), TableArn: aws.String("arn:aws:dynamodb:us-east-1:" + acct + ":table/" + id)}
		}
	}
}

// samProcess is a stand-in for the AWS::Serverless transform, enough for the
// retain checks: the processed template is JSON; an AWS::Serverless::Function
// becomes an AWS::Lambda::Function, and one that declares Policies also
// GENERATES <ID>Role. passthrough says whether the transform copies the
// resource's DeletionPolicy/UpdateReplacePolicy onto that generated role (the
// real transform's behaviour is exactly what the processed-template check
// exists not to assume).
func samProcess(passthrough bool) func(string) string {
	return func(body string) string {
		var doc map[string]any
		if err := yaml.Unmarshal([]byte(strings.NewReplacer("!", "").Replace(body)), &doc); err != nil {
			return "{unparseable"
		}
		res, _ := doc["Resources"].(map[string]any)
		out := map[string]any{}
		for id, v := range res {
			r, _ := v.(map[string]any)
			p := map[string]any{"Type": r["Type"]}
			for _, k := range retainPolicyKeys {
				if r[k] != nil {
					p[k] = r[k]
				}
			}
			if r["Type"] == "AWS::Serverless::Function" {
				p["Type"] = "AWS::Lambda::Function"
				props, _ := r["Properties"].(map[string]any)
				if props["Policies"] != nil && props["Role"] == nil {
					gen := map[string]any{"Type": "AWS::IAM::Role"}
					if passthrough {
						for _, k := range retainPolicyKeys {
							if r[k] != nil {
								gen[k] = r[k]
							}
						}
					}
					out[id+"Role"] = gen
				}
			}
			out[id] = p
		}
		b, _ := json.Marshal(map[string]any{"Resources": out})
		return string(b)
	}
}

// policyOnlyChanges is the change set CloudFormation reports for a retain-only
// overlay: a Static DeletionPolicy + UpdateReplacePolicy detail per resource.
func policyOnlyChanges(rs []cfntypes.StackResourceSummary) []cfntypes.Change {
	var out []cfntypes.Change
	for _, r := range rs {
		out = append(out, cfntypes.Change{Type: cfntypes.ChangeTypeResource, ResourceChange: &cfntypes.ResourceChange{
			Action: cfntypes.ChangeActionModify, LogicalResourceId: r.LogicalResourceId, ResourceType: r.ResourceType,
			Replacement: cfntypes.ReplacementFalse,
			Details: []cfntypes.ResourceChangeDetail{
				{Evaluation: cfntypes.EvaluationTypeStatic, ChangeSource: cfntypes.ChangeSourceDirectModification,
					Target: &cfntypes.ResourceTargetDefinition{Attribute: cfntypes.ResourceAttributeDeletionPolicy, RequiresRecreation: cfntypes.RequiresRecreationNever}},
				{Evaluation: cfntypes.EvaluationTypeStatic, ChangeSource: cfntypes.ChangeSourceDirectModification,
					Target: &cfntypes.ResourceTargetDefinition{Attribute: cfntypes.ResourceAttributeUpdateReplacePolicy, RequiresRecreation: cfntypes.RequiresRecreationNever}},
			},
		}})
	}
	return out
}

// addDetail appends a detail to the change for logicalID.
func addDetail(changes []cfntypes.Change, logicalID string, det cfntypes.ResourceChangeDetail) {
	for _, c := range changes {
		if deref(c.ResourceChange.LogicalResourceId) == logicalID {
			c.ResourceChange.Details = append(c.ResourceChange.Details, det)
			return
		}
	}
	panic("no change for " + logicalID)
}

// dynamicDetail is CloudFormation re-resolving a dependent's !GetAtt.
func dynamicDetail(property, causing string) cfntypes.ResourceChangeDetail {
	return cfntypes.ResourceChangeDetail{
		Evaluation: cfntypes.EvaluationTypeDynamic, ChangeSource: cfntypes.ChangeSourceResourceAttribute,
		CausingEntity: aws.String(causing),
		Target: &cfntypes.ResourceTargetDefinition{Attribute: cfntypes.ResourceAttributeProperties,
			Name: aws.String(property), RequiresRecreation: cfntypes.RequiresRecreationNever},
	}
}

// us-east-1, per the owner's live list-stack-resources (#226).
func usEast1Resources() []cfntypes.StackResourceSummary {
	return resourcesOf(
		"CapacityPollerFunction", "AWS::Lambda::Function", PollerFunctionName,
		"CapacityPollerRole", "AWS::IAM::Role", "lagotto-capacity-poller-role",
		"CapacityPollerSchedule", "AWS::Scheduler::Schedule", PollerScheduleName,
		"LagottoCapacityAlertsTopic", "AWS::SNS::Topic", AlertsTopicARN("us-east-1", acct),
		"SchedulerInvokeRole", "AWS::IAM::Role", "lagotto-capacity-poller-scheduler-invoke",
	)
}

// 5f7149a's stack: also the two tables, and the SAM-generated function role.
func tablesStackResources() []cfntypes.StackResourceSummary {
	return resourcesOf(
		"CapacityPollerFunction", "AWS::Lambda::Function", PollerFunctionName,
		"CapacityPollerFunctionRole", "AWS::IAM::Role", "lagotto-CapacityPollerFunctionRole-ABC123",
		"CapacityPollerSchedule", "AWS::Scheduler::Schedule", PollerScheduleName,
		"LagottoCapacityAlertsTopic", "AWS::SNS::Topic", AlertsTopicARN("us-east-1", acct),
		"LagottoMatchHistoryTable", "AWS::DynamoDB::Table", "lagotto-match-history",
		"LagottoWatchesTable", "AWS::DynamoDB::Table", "lagotto-watches",
		"SchedulerInvokeRole", "AWS::IAM::Role", "lagotto-capacity-poller-scheduler-invoke",
	)
}

// legacyStack wires a Deployer to a stack built from template that owns rs,
// every one of them live in the world, with a SAM transform and a policy-only
// change set.
func legacyStack(t *testing.T, template string, rs []cfntypes.StackResourceSummary) (*Deployer, *fakeCFN, *world) {
	t.Helper()
	w := &world{functions: map[string]*lambda.GetFunctionOutput{}, schedules: map[string]*scheduler.GetScheduleOutput{},
		topics: map[string]map[string]string{}, roles: map[string]*worldRole{}, tables: map[string]*dynamodbtypes.TableDescription{}}
	w.populate(rs)
	c := &fakeCFN{
		stackName: "lagotto",
		status:    cfntypes.StackStatusUpdateComplete,
		template:  template,
		params: []cfntypes.Parameter{
			{ParameterKey: aws.String("LambdaCodeBucket"), ParameterValue: aws.String("lagotto-lambda-" + acct + "-us-east-1")},
			{ParameterKey: aws.String("LambdaCodeKey"), ParameterValue: aws.String("lagotto/capacity-poller-v0.44.0.zip")},
			{ParameterKey: aws.String("Environment"), ParameterValue: aws.String("production")},
		},
		tags:             []cfntypes.Tag{{Key: aws.String("Application"), Value: aws.String("lagotto")}},
		resources:        rs,
		onDeleteResource: w.delete,
		process:          samProcess(true),
		changes:          policyOnlyChanges(rs),
	}
	d := &Deployer{cfn: c, lambda: w, sns: worldSNS{w}, sched: worldSched{w}, ddb: w, sleep: func(time.Duration) {}}
	return d, c, w
}

// snapshotWorld is every resource's state, for an unchanged-after comparison.
func snapshotWorld(t *testing.T, d *Deployer, w *world, rs []cfntypes.StackResourceSummary) map[string]string {
	t.Helper()
	snap := map[string]string{}
	for _, r := range rs {
		o := OwnedResource{LogicalID: deref(r.LogicalResourceId), PhysicalID: deref(r.PhysicalResourceId), Type: deref(r.ResourceType)}
		fp, err := d.fingerprint(context.Background(), w, o)
		if err != nil {
			t.Fatalf("fingerprint %s: %v", o.LogicalID, err)
		}
		snap[o.LogicalID] = fp
	}
	return snap
}

// --- the #226 gates ---------------------------------------------------------

// TestMigrate_KeepsEveryOwnedResource is the #226 outage: a legacy stack that
// owns the account-wide IAM roles (us-east-1, 4262315) or the DynamoDB tables
// (5f7149a) must keep ALL of them, unchanged. The fake CloudFormation deletes a
// resource the way the real one does — dropped from the processed template by an
// update, or lacking a processed DeletionPolicy: Retain at delete-stack — so
// retaining a fixed three logical IDs under lagotto's current template deletes
// the roles and tables here exactly as it would have live.
func TestMigrate_KeepsEveryOwnedResource(t *testing.T) {
	for _, tc := range []struct {
		name     string
		template string
		rs       []cfntypes.StackResourceSummary
	}{
		{"us-east-1 shape: both shared IAM roles (4262315)", fixture(t, "legacy-stack-4262315.yaml"), usEast1Resources()},
		{"tables and a SAM-generated role (5f7149a)", fixture(t, "legacy-stack-5f7149a.yaml"), tablesStackResources()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, c, w := legacyStack(t, tc.template, tc.rs)
			before := snapshotWorld(t, d, w, tc.rs)

			report, err := d.MigrateFromStack(context.Background(), "lagotto", w)
			if err != nil {
				t.Fatalf("MigrateFromStack: %v", err)
			}
			if len(w.deleted) != 0 {
				t.Fatalf("CloudFormation deleted %v — every resource the stack owned must be retained (#226)", w.deleted)
			}
			after := snapshotWorld(t, d, w, tc.rs)
			for id, fp := range before {
				if after[id] != fp {
					t.Errorf("%s changed:\nbefore %s\nafter  %s", id, fp, after[id])
				}
			}
			if c.status != cfntypes.StackStatusDeleteComplete {
				t.Errorf("stack status %s: the stack was not deleted", c.status)
			}
			if report == nil || len(report.Retained) != len(tc.rs) {
				t.Errorf("report = %+v, want all %d owned resources", report, len(tc.rs))
			}

			// The change set: the stack's OWN template plus only the policies,
			// every parameter at UsePreviousValue, the stack's own tags.
			if len(c.createCSCalls) != 1 || len(c.updateCalls) != 0 {
				t.Fatalf("CreateChangeSet x%d, UpdateStack x%d; want one change set and no direct UpdateStack", len(c.createCSCalls), len(c.updateCalls))
			}
			cs := c.createCSCalls[0]
			if err := verifyRetainOverlay(tc.template, deref(cs.TemplateBody)); err != nil {
				t.Errorf("the change set's template is not the stack's own plus retain policies: %v", err)
			}
			if len(cs.Parameters) != 3 {
				t.Errorf("change set carries %d parameters, want the stack's 3", len(cs.Parameters))
			}
			for _, p := range cs.Parameters {
				if !aws.ToBool(p.UsePreviousValue) || p.ParameterValue != nil {
					t.Errorf("parameter %s is not UsePreviousValue-only", deref(p.ParameterKey))
				}
			}
			if len(cs.Tags) != 1 || deref(cs.Tags[0].Key) != "Application" {
				t.Errorf("change set tags = %v, want the stack's own", cs.Tags)
			}
		})
	}
}

// TestMigrate_RefusesAPropertyChange: a change set that would change any
// property is refused and deleted unexecuted; nothing is updated or deleted.
func TestMigrate_RefusesAPropertyChange(t *testing.T) {
	d, c, w := legacyStack(t, fixture(t, "legacy-stack-4262315.yaml"), usEast1Resources())
	addDetail(c.changes, "CapacityPollerFunction", cfntypes.ResourceChangeDetail{
		Evaluation: cfntypes.EvaluationTypeStatic, ChangeSource: cfntypes.ChangeSourceDirectModification,
		Target: &cfntypes.ResourceTargetDefinition{Attribute: cfntypes.ResourceAttributeProperties,
			Name: aws.String("Environment"), RequiresRecreation: cfntypes.RequiresRecreationNever},
	})

	_, err := d.MigrateFromStack(context.Background(), "lagotto", w)
	if err == nil {
		t.Fatal("MigrateFromStack accepted a change set with a property change")
	}
	if !strings.Contains(err.Error(), "REFUSED") || !strings.Contains(err.Error(), "Environment") {
		t.Errorf("refusal does not name the property change: %v", err)
	}
	if c.executeCSCalls != 0 || len(c.updateCalls) != 0 || len(c.deleteCalls) != 0 {
		t.Errorf("execute x%d, UpdateStack x%d, DeleteStack x%d after a refusal; want none", c.executeCSCalls, len(c.updateCalls), len(c.deleteCalls))
	}
	if c.deleteCSCalls != 1 {
		t.Errorf("DeleteChangeSet x%d, want the refused change set deleted", c.deleteCSCalls)
	}
	if len(w.deleted) != 0 {
		t.Errorf("resources deleted: %v", w.deleted)
	}
}

// TestMigrate_RefusesDynamicReevaluationThatDiffersFromLive: the schedule's
// Target !GetAtt's the function, so retaining the function makes CloudFormation
// re-evaluate the schedule at execution. Drift detection reports the stack's
// stored schedule (State DISABLED) differs from the live one (ENABLED, a watch
// is active): re-applying it would switch the poller off. Refused.
func TestMigrate_RefusesDynamicReevaluationThatDiffersFromLive(t *testing.T) {
	d, c, w := legacyStack(t, fixture(t, "legacy-stack-4262315.yaml"), usEast1Resources())
	addDetail(c.changes, "CapacityPollerSchedule", dynamicDetail("Target", "CapacityPollerFunction.Arn"))
	c.drifts = []cfntypes.StackResourceDrift{{
		LogicalResourceId: aws.String("CapacityPollerSchedule"), StackResourceDriftStatus: cfntypes.StackResourceDriftStatusModified,
		PropertyDifferences: []cfntypes.PropertyDifference{{PropertyPath: aws.String("/State"), ExpectedValue: aws.String("DISABLED"), ActualValue: aws.String("ENABLED")}},
	}}

	_, err := d.MigrateFromStack(context.Background(), "lagotto", w)
	if err == nil {
		t.Fatal("MigrateFromStack executed a change set whose Dynamic re-evaluation would revert the live schedule")
	}
	for _, want := range []string{"REFUSED", "CapacityPollerSchedule", "/State", "DISABLED", "ENABLED"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q: %v", want, err)
		}
	}
	if len(c.detectDriftCalls) != 1 || len(c.detectDriftCalls[0].LogicalResourceIds) != 1 || c.detectDriftCalls[0].LogicalResourceIds[0] != "CapacityPollerSchedule" {
		t.Errorf("DetectStackDrift calls = %+v, want one, for exactly the re-evaluated resource", c.detectDriftCalls)
	}
	if c.executeCSCalls != 0 || len(c.updateCalls) != 0 || len(c.deleteCalls) != 0 || len(w.deleted) != 0 {
		t.Errorf("execute x%d, UpdateStack x%d, DeleteStack x%d, deleted %v; want nothing", c.executeCSCalls, len(c.updateCalls), len(c.deleteCalls), w.deleted)
	}
}

// TestMigrate_DynamicReevaluationInSyncProceeds: the owner's us-east-1 case —
// the function re-evaluates Role/Environment and the schedule Target, and every
// stored value matches live, so the re-evaluation is a no-op.
func TestMigrate_DynamicReevaluationInSyncProceeds(t *testing.T) {
	d, c, w := legacyStack(t, fixture(t, "legacy-stack-4262315.yaml"), usEast1Resources())
	addDetail(c.changes, "CapacityPollerFunction", dynamicDetail("Role", "CapacityPollerRole.Arn"))
	addDetail(c.changes, "CapacityPollerFunction", dynamicDetail("Environment", "LagottoCapacityAlertsTopic"))
	addDetail(c.changes, "CapacityPollerSchedule", dynamicDetail("Target", "CapacityPollerFunction.Arn"))
	c.drifts = []cfntypes.StackResourceDrift{
		{LogicalResourceId: aws.String("CapacityPollerFunction"), StackResourceDriftStatus: cfntypes.StackResourceDriftStatusInSync},
		{LogicalResourceId: aws.String("CapacityPollerSchedule"), StackResourceDriftStatus: cfntypes.StackResourceDriftStatusInSync},
	}
	if _, err := d.MigrateFromStack(context.Background(), "lagotto", w); err != nil {
		t.Fatalf("MigrateFromStack: %v", err)
	}
	if len(w.deleted) != 0 {
		t.Errorf("deleted %v", w.deleted)
	}
}

// TestMigrate_DynamicCodeReevaluationRefused: drift detection cannot see a
// function's code, so a re-evaluation of Code is refused outright.
func TestMigrate_DynamicCodeReevaluationRefused(t *testing.T) {
	d, c, w := legacyStack(t, fixture(t, "legacy-stack-4262315.yaml"), usEast1Resources())
	addDetail(c.changes, "CapacityPollerFunction", dynamicDetail("Code", "SomeBucket"))
	c.drifts = []cfntypes.StackResourceDrift{{LogicalResourceId: aws.String("CapacityPollerFunction"), StackResourceDriftStatus: cfntypes.StackResourceDriftStatusInSync}}
	if _, err := d.MigrateFromStack(context.Background(), "lagotto", w); err == nil || !strings.Contains(err.Error(), "Code") {
		t.Fatalf("err = %v, want a refusal naming Code", err)
	}
	if c.executeCSCalls != 0 {
		t.Error("the change set was executed")
	}
}

// TestMigrate_ExecuteThatChangesALiveResourceStopsTheDelete: lagotto's own
// fingerprint is the backstop behind drift detection — if executing the change
// set changed a live resource anyway, the stack is not deleted.
func TestMigrate_ExecuteThatChangesALiveResourceStopsTheDelete(t *testing.T) {
	d, c, w := legacyStack(t, fixture(t, "legacy-stack-4262315.yaml"), usEast1Resources())
	c.onExecute = func() { w.schedules[PollerScheduleName].State = schedulertypes.ScheduleStateDisabled }
	_, err := d.MigrateFromStack(context.Background(), "lagotto", w)
	if err == nil || !strings.Contains(err.Error(), "CHANGED") {
		t.Fatalf("err = %v, want the changed schedule reported", err)
	}
	if len(c.deleteCalls) != 0 {
		t.Error("the stack was deleted although a live resource changed")
	}
}

// TestMigrate_SAMProcessedTemplateVerified: the transform generates
// CapacityPollerFunctionRole, which exists only in the processed template. If
// the transform does not carry DeletionPolicy onto it, the change set's
// processed template shows it unretained and the migration refuses BEFORE
// executing anything — checking the Original stage alone would delete the role.
func TestMigrate_SAMProcessedTemplateVerified(t *testing.T) {
	d, c, w := legacyStack(t, fixture(t, "legacy-stack-5f7149a.yaml"), tablesStackResources())
	c.process = samProcess(false)

	_, err := d.MigrateFromStack(context.Background(), "lagotto", w)
	if err == nil {
		t.Fatal("MigrateFromStack proceeded although the processed template does not retain the SAM-generated role")
	}
	if !strings.Contains(err.Error(), "CapacityPollerFunctionRole") || !strings.Contains(err.Error(), "processed template") {
		t.Errorf("refusal does not name the unretained generated resource: %v", err)
	}
	if c.executeCSCalls != 0 || len(c.updateCalls) != 0 || len(c.deleteCalls) != 0 || len(w.deleted) != 0 {
		t.Errorf("execute x%d, UpdateStack x%d, DeleteStack x%d, deleted %v; want nothing", c.executeCSCalls, len(c.updateCalls), len(c.deleteCalls), w.deleted)
	}
	var sawProcessedCS bool
	for _, g := range c.getTemplateCalls {
		if g.ChangeSetName != nil && g.TemplateStage == cfntypes.TemplateStageProcessed {
			sawProcessedCS = true
		}
	}
	if !sawProcessedCS {
		t.Error("the change set's Processed template was never read")
	}
}

// TestMigrate_StackProcessedTemplateGatesTheDelete: the post-execute check reads
// the STACK's processed template; one that shows a resource unretained stops the
// delete even if the change set looked right.
func TestMigrate_StackProcessedTemplateGatesTheDelete(t *testing.T) {
	d, c, w := legacyStack(t, fixture(t, "legacy-stack-4262315.yaml"), usEast1Resources())
	executed := false
	c.onExecute = func() { executed = true }
	sam := samProcess(true)
	c.process = func(body string) string {
		out := sam(body)
		if executed {
			// CloudFormation applied something other than what it previewed.
			out = strings.Replace(out, `"DeletionPolicy":"Retain","Type":"AWS::IAM::Role"`, `"Type":"AWS::IAM::Role"`, 1)
		}
		return out
	}
	_, err := d.MigrateFromStack(context.Background(), "lagotto", w)
	if err == nil || !strings.Contains(err.Error(), "ABORTED") {
		t.Fatalf("err = %v, want the delete aborted", err)
	}
	if len(c.deleteCalls) != 0 || len(w.deleted) != 0 {
		t.Errorf("DeleteStack x%d, deleted %v", len(c.deleteCalls), w.deleted)
	}
}

// TestMigrate_RefusesUnverifiableResourceType: survival of a type lagotto
// cannot read is unprovable, so the migration refuses before changing anything.
func TestMigrate_RefusesUnverifiableResourceType(t *testing.T) {
	rs := append(usEast1Resources(), resourcesOf("PollerLogGroup", "AWS::Logs::LogGroup", "/aws/lambda/x")...)
	d, c, w := legacyStack(t, fixture(t, "legacy-stack-4262315.yaml"), rs)
	_, err := d.MigrateFromStack(context.Background(), "lagotto", w)
	if err == nil || !strings.Contains(err.Error(), "AWS::Logs::LogGroup") {
		t.Fatalf("err = %v, want a refusal naming the type", err)
	}
	if len(c.createCSCalls) != 0 {
		t.Error("a change set was created")
	}
}

// TestMigrate_MissingResourceBeforehandRefuses: a resource that cannot be read
// before the migration cannot be proven to survive it.
func TestMigrate_MissingResourceBeforehandRefuses(t *testing.T) {
	d, c, w := legacyStack(t, fixture(t, "legacy-stack-4262315.yaml"), usEast1Resources())
	delete(w.roles, "lagotto-capacity-poller-role")
	if _, err := d.MigrateFromStack(context.Background(), "lagotto", w); err == nil || !strings.Contains(err.Error(), "CapacityPollerRole") {
		t.Fatalf("err = %v, want a refusal naming the unreadable role", err)
	}
	if len(c.createCSCalls) != 0 {
		t.Error("a change set was created")
	}
}

// TestMigrate_PostDeleteVerificationFailsLoudly: a resource gone after the
// delete (CloudFormation ignoring a Retain) is reported in full, including the
// consequence that a re-created schedule comes back DISABLED.
func TestMigrate_PostDeleteVerificationFailsLoudly(t *testing.T) {
	d, c, w := legacyStack(t, fixture(t, "legacy-stack-4262315.yaml"), usEast1Resources())
	// The role goes "with the stack" despite its Retain.
	d.cfn = &deleteHook{fakeCFN: c, after: func() { delete(w.roles, "lagotto-capacity-poller-scheduler-invoke") }}
	_, err := d.MigrateFromStack(context.Background(), "lagotto", w)
	if err == nil {
		t.Fatal("MigrateFromStack returned nil after a role disappeared")
	}
	for _, want := range []string{"lagotto-capacity-poller-scheduler-invoke", "GONE", "DISABLED", "lagotto deploy", "lagotto setup"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %q: %v", want, err)
		}
	}
}

// deleteHook runs after a DeleteStack.
type deleteHook struct {
	*fakeCFN
	after func()
}

func (h *deleteHook) DeleteStack(ctx context.Context, in *cloudformation.DeleteStackInput, opt ...func(*cloudformation.Options)) (*cloudformation.DeleteStackOutput, error) {
	out, err := h.fakeCFN.DeleteStack(ctx, in, opt...)
	h.after()
	return out, err
}

// TestMigrate_NoChangesStillVerifies: CloudFormation answers "didn't contain
// changes" when the policies are already in place. That is not a green light:
// the processed-template gate still decides.
func TestMigrate_NoChangesStillVerifies(t *testing.T) {
	d, c, w := legacyStack(t, fixture(t, "legacy-stack-4262315.yaml"), usEast1Resources())
	c.csStatus = cfntypes.ChangeSetStatusFailed
	c.csReason = "The submitted information didn't contain changes. Submit different information to create a change set."
	_, err := d.MigrateFromStack(context.Background(), "lagotto", w)
	if err == nil || !strings.Contains(err.Error(), "ABORTED") {
		t.Fatalf("err = %v: the stack's template has no Retain, so the delete must be aborted", err)
	}
	if len(c.deleteCalls) != 0 || c.executeCSCalls != 0 {
		t.Error("deleted or executed")
	}
}

func TestMigrate_RefusesUnstableStatus(t *testing.T) {
	for _, status := range []cfntypes.StackStatus{
		cfntypes.StackStatusUpdateInProgress, cfntypes.StackStatusUpdateRollbackFailed,
		cfntypes.StackStatusRollbackComplete, cfntypes.StackStatusDeleteFailed,
	} {
		d, c, w := legacyStack(t, fixture(t, "legacy-stack-4262315.yaml"), usEast1Resources())
		c.status = status
		_, err := d.MigrateFromStack(context.Background(), "lagotto", w)
		if err == nil || !strings.Contains(err.Error(), string(status)) {
			t.Errorf("status %s: err = %v", status, err)
		}
		if len(c.createCSCalls) != 0 || len(c.deleteCalls) != 0 {
			t.Errorf("status %s: the stack was touched", status)
		}
	}
}

func TestMigrate_AbsentStackIsAClearError(t *testing.T) {
	d, c, w := legacyStack(t, fixture(t, "legacy-stack-4262315.yaml"), usEast1Resources())
	c.missing = true
	if _, err := d.MigrateFromStack(context.Background(), "lagotto", w); err == nil || !strings.Contains(err.Error(), "nothing to migrate from") {
		t.Errorf("err = %v", err)
	}
}

func TestMigrate_GetTemplateFailureDoesNotDelete(t *testing.T) {
	d, c, w := legacyStack(t, fixture(t, "legacy-stack-4262315.yaml"), usEast1Resources())
	c.getTemplateErr = fmt.Errorf("AccessDenied: not authorized to perform cloudformation:GetTemplate")
	if _, err := d.MigrateFromStack(context.Background(), "lagotto", w); err == nil {
		t.Fatal("MigrateFromStack succeeded without reading the template")
	}
	if len(c.deleteCalls) != 0 || len(c.createCSCalls) != 0 {
		t.Error("touched the stack")
	}
}

// --- vetChangeSet: every refusal class --------------------------------------

func TestVetChangeSet(t *testing.T) {
	rs := usEast1Resources()
	var owned []OwnedResource
	for _, r := range rs {
		owned = append(owned, OwnedResource{LogicalID: deref(r.LogicalResourceId), PhysicalID: deref(r.PhysicalResourceId), Type: deref(r.ResourceType)})
	}
	mutate := func(f func(rc *cfntypes.ResourceChange)) []cfntypes.Change {
		ch := policyOnlyChanges(rs)
		f(ch[0].ResourceChange)
		return ch
	}
	if dyn, p := vetChangeSet(policyOnlyChanges(rs), owned); len(p) != 0 || len(dyn) != 0 {
		t.Fatalf("policy-only change set: dynamic %v, problems %v", dyn, p)
	}
	for name, changes := range map[string][]cfntypes.Change{
		"add":         mutate(func(rc *cfntypes.ResourceChange) { rc.Action = cfntypes.ChangeActionAdd }),
		"remove":      mutate(func(rc *cfntypes.ResourceChange) { rc.Action = cfntypes.ChangeActionRemove }),
		"replacement": mutate(func(rc *cfntypes.ResourceChange) { rc.Replacement = cfntypes.ReplacementConditional }),
		"no details":  mutate(func(rc *cfntypes.ResourceChange) { rc.Details = nil }),
		"tags":        mutate(func(rc *cfntypes.ResourceChange) { rc.Details[0].Target.Attribute = cfntypes.ResourceAttributeTags }),
		"static property": mutate(func(rc *cfntypes.ResourceChange) {
			rc.Details[0].Target.Attribute = cfntypes.ResourceAttributeProperties
		}),
		"dynamic from a parameter": mutate(func(rc *cfntypes.ResourceChange) {
			d := dynamicDetail("Environment", "LambdaCodeKey")
			d.ChangeSource = cfntypes.ChangeSourceParameterReference
			rc.Details = append(rc.Details, d)
		}),
		"dynamic needing recreation": mutate(func(rc *cfntypes.ResourceChange) {
			d := dynamicDetail("Target", "X")
			d.Target.RequiresRecreation = cfntypes.RequiresRecreationConditionally
			rc.Details = append(rc.Details, d)
		}),
		"unowned resource": mutate(func(rc *cfntypes.ResourceChange) { rc.LogicalResourceId = aws.String("Stranger") }),
	} {
		if _, p := vetChangeSet(changes, owned); len(p) == 0 {
			t.Errorf("%s: accepted", name)
		}
	}
}

// --- the overlay ------------------------------------------------------------

// TestRetainOverlay_RealTemplates: on every fixture and the current template,
// the overlay retains every resource and verifyRetainOverlay proves nothing
// else changed. For YAML, every original line also survives, in order: only
// policy lines are inserted.
func TestRetainOverlay_RealTemplates(t *testing.T) {
	for name, body := range map[string]string{
		"4262315": fixture(t, "legacy-stack-4262315.yaml"),
		"5f7149a": fixture(t, "legacy-stack-5f7149a.yaml"),
		"current": cfn.StackTemplate,
	} {
		out, err := withRetainPolicies(body)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if missing, err := resourcesMissingRetain(out, nil); err != nil || len(missing) > 0 {
			t.Errorf("%s: missing %v (%v)", name, missing, err)
		}
		in, o := strings.Split(body, "\n"), strings.Split(out, "\n")
		i := 0
		for _, line := range o {
			if i < len(in) && line == in[i] {
				i++
				continue
			}
			if tl := strings.TrimSpace(line); tl != "DeletionPolicy: Retain" && tl != "UpdateReplacePolicy: Retain" {
				t.Errorf("%s: unexpected inserted line %q", name, line)
			}
		}
		if i != len(in) {
			t.Errorf("%s: only %d of %d original lines survived in order", name, i, len(in))
		}
		for _, intrinsic := range []string{"!GetAtt", "!Ref", "Transform: AWS::Serverless-2016-10-31"} {
			if strings.Count(out, intrinsic) != strings.Count(body, intrinsic) {
				t.Errorf("%s: %q count changed", name, intrinsic)
			}
		}
		twice, err := withRetainPolicies(out)
		if err != nil || twice != out {
			t.Errorf("%s: overlay is not idempotent (%v)", name, err)
		}
	}
}

func TestRetainOverlay_RewritesAnExistingPolicy(t *testing.T) {
	body := "Resources:\n  T:\n    Type: AWS::DynamoDB::Table\n    DeletionPolicy: Delete # was\n    Properties:\n      TableName: x\n"
	out, err := withRetainPolicies(body)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "Delete") || strings.Count(out, "DeletionPolicy") != 1 {
		t.Errorf("existing policy not rewritten exactly once:\n%s", out)
	}
}

func TestRetainOverlay_JSON(t *testing.T) {
	body := `{"Transform":"AWS::Serverless-2016-10-31","Resources":{"F":{"Type":"AWS::Serverless::Function","Properties":{"MemorySize":512,"Role":{"Fn::GetAtt":["R","Arn"]}}},"R":{"Type":"AWS::IAM::Role","DeletionPolicy":"Delete"}}}`
	out, err := withRetainPolicies(body)
	if err != nil {
		t.Fatal(err)
	}
	if missing, _ := resourcesMissingRetain(out, nil); len(missing) != 0 {
		t.Errorf("missing %v:\n%s", missing, out)
	}
	if !strings.Contains(out, `"MemorySize": 512`) || !strings.Contains(out, `"Fn::GetAtt"`) {
		t.Errorf("properties not preserved:\n%s", out)
	}
}

func TestRetainOverlay_RefusesFlowStyle(t *testing.T) {
	if _, err := withRetainPolicies("Resources:\n  T: {Type: AWS::SNS::Topic}\n"); err == nil {
		t.Error("a flow-style resource was edited")
	}
}

// TestVerifyRetainOverlay_CatchesAnyOtherChange: the check that stands behind
// the text edit.
func TestVerifyRetainOverlay_CatchesAnyOtherChange(t *testing.T) {
	body := fixture(t, "legacy-stack-4262315.yaml")
	out, err := withRetainPolicies(body)
	if err != nil {
		t.Fatal(err)
	}
	for name, tampered := range map[string]string{
		"property": strings.Replace(out, "MemorySize: 512", "MemorySize: 1024", 1),
		"tag":      strings.Replace(out, "!GetAtt CapacityPollerRole.Arn", "CapacityPollerRole.Arn", 1),
	} {
		if tampered == out {
			t.Fatalf("%s: tamper did not apply", name)
		}
		if err := verifyRetainOverlay(body, tampered); err == nil {
			t.Errorf("%s change not caught", name)
		}
	}
}

// TestStackTemplateHasNoRetainPolicies: Retain must NEVER be committed to
// lagotto-stack.yaml. The template is kept for IaC consumers, and for them a
// `delete-stack` that silently leaves orphaned resources behind is worse than
// no template at all. Retain belongs only in the transient overlay the
// migration builds.
func TestStackTemplateHasNoRetainPolicies(t *testing.T) {
	if strings.Contains(cfn.StackTemplate, "DeletionPolicy") || strings.Contains(cfn.StackTemplate, "UpdateReplacePolicy") {
		t.Error("deployment/cloudformation/lagotto-stack.yaml declares a deletion policy; it must not")
	}
}

// --- the automatic warnings -------------------------------------------------

func TestLegacyStackAdoptionWarning_SaysWhatMatters(t *testing.T) {
	msg := LegacyStackAdoptionWarning("lagotto", acct, "us-west-2")
	for _, want := range []string{
		`"lagotto"`, acct + "/us-west-2", "ADOPTED", "nothing was duplicated",
		"still believes it owns", "IAM roles", "EVERY region", "would DELETE your live",
		"delete-stack --stack-name", "--migrate-from-cloudformation", "every resource the stack owns",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("the adoption warning does not mention %q:\n%s", want, msg)
		}
	}
}

// TestLegacyStackTeardownWarning: after a teardown the stack may STILL own the
// shared IAM roles, so the advice is to look before deleting.
func TestLegacyStackTeardownWarning(t *testing.T) {
	msg := LegacyStackTeardownWarning("lagotto")
	for _, want := range []string{"list-stack-resources --stack-name lagotto", "lagotto-capacity-poller-role",
		"lagotto-capacity-poller-scheduler-invoke", "delete-stack --stack-name lagotto"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the teardown warning does not mention %q:\n%s", want, msg)
		}
	}
	if strings.Contains(msg, "empty shell") {
		t.Errorf("the teardown warning still calls the stack an empty shell:\n%s", msg)
	}
}
