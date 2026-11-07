package iamcoverage

import (
	"go/types"
	"strings"
)

// An operation that can carry a role or tags is also authorized as a second
// action: RunInstances with an instance profile needs iam:PassRole, and with
// TagSpecifications needs ec2:CreateTags.
//
// This is spawn's rule table (spawn internal/iamcoverage/implied.go, #1377 and
// #1397), where each rule cites the Service Authorization Reference entry and
// the service page it came from. spawn also traces each call's input to decide
// whether the field may be set; lagotto does not. Here a rule applies wherever
// its operation is reachable, an over-approximation: the inventory can list
// ec2:CreateTags for a command whose RunInstances never sets tags, never the
// reverse. A rule whose field the input type does not have is still an error,
// so an SDK rename cannot turn a rule off silently.

type impliedRule struct {
	Op      string // "<prefix>:<Op>", or "<package path>.<method>"
	Field   string // dotted field of the *<Op>Input; a list is followed to its element
	Actions []string
}

func tagOnCreate(prefix, field, action string, ops ...string) []impliedRule {
	out := make([]impliedRule, len(ops))
	for i, op := range ops {
		out[i] = impliedRule{Op: prefix + ":" + op, Field: field, Actions: []string{action}}
	}
	return out
}

func passRole(op, field string) impliedRule {
	return impliedRule{Op: op, Field: field, Actions: []string{"iam:PassRole"}}
}

var impliedRules = concatRules(
	[]impliedRule{
		passRole("ec2:RunInstances", "IamInstanceProfile"),
		passRole("lambda:CreateFunction", "Role"),
		passRole("lambda:UpdateFunctionConfiguration", "Role"),
		passRole("scheduler:CreateSchedule", "Target.RoleArn"),
		passRole("scheduler:UpdateSchedule", "Target.RoleArn"),
		passRole("events:PutRule", "RoleArn"),
		passRole("events:PutTargets", "Targets.RoleArn"),
		passRole("ssm:SendCommand", "ServiceRoleArn"),
		passRole("sns:Subscribe", "Attributes"),
		passRole("sns:CreateTopic", "Attributes"),
		passRole("iam:AddRoleToInstanceProfile", "RoleName"),
	},
	tagOnCreate("sqs", "Tags", "sqs:TagQueue", "CreateQueue"),
	tagOnCreate("iam", "Tags", "iam:TagRole", "CreateRole"),
	tagOnCreate("iam", "Tags", "iam:TagInstanceProfile", "CreateInstanceProfile"),
	tagOnCreate("iam", "Tags", "iam:TagPolicy", "CreatePolicy"),
	tagOnCreate("ec2", "TagSpecifications", "ec2:CreateTags",
		"RunInstances", "CreateSecurityGroup", "CreateVolume", "CreateImage", "CopyImage",
		"RegisterImage", "CreateKeyPair", "ImportKeyPair", "AllocateAddress",
		"CreatePlacementGroup", "AuthorizeSecurityGroupIngress", "AuthorizeSecurityGroupEgress",
		"PurchaseCapacityBlock", "CreateCapacityReservation"),
	tagOnCreate("dynamodb", "Tags", "dynamodb:TagResource", "CreateTable"),
	tagOnCreate("lambda", "Tags", "lambda:TagResource", "CreateFunction"),
	tagOnCreate("events", "Tags", "events:TagResource", "PutRule"),
	tagOnCreate("sns", "Tags", "sns:TagResource", "CreateTopic"),
	tagOnCreate("sts", "Tags", "sts:TagSession", "AssumeRole"),
	tagOnCreate("s3", "Tagging", "s3:PutObjectTagging", "PutObject", "CopyObject", "CreateMultipartUpload"),
	tagOnCreate("sagemaker", "Tags", "sagemaker:AddTags", "CreateTrainingJob"),
	// FSx authorizes tags passed on a create as fsx:TagResource as well (Amazon
	// FSx for Lustre user guide, "Grant permission to tag resources during
	// creation"). spawn's buildFSxCreateInput always passes Tags (#242).
	tagOnCreate("fsx", "Tags", "fsx:TagResource", "CreateFileSystem"),
	[]impliedRule{
		{Op: "s3:CreateBucket", Field: "CreateBucketConfiguration.Tags", Actions: []string{"s3:TagResource"}},
		{Op: sdkRoot + "feature/s3/transfermanager.UploadObject", Field: "Tagging", Actions: []string{"s3:PutObjectTagging"}},
	},
)

func concatRules(parts ...[]impliedRule) []impliedRule {
	var out []impliedRule
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

var impliedByOp = func() map[string][]impliedRule {
	m := map[string][]impliedRule{}
	for _, r := range impliedRules {
		m[r.Op] = append(m[r.Op], r)
	}
	return m
}()

// applyImplied adds the actions of every rule for op's operation.
func applyImplied(op foundOp) foundOp {
	rules := impliedByOp[op.key]
	if len(rules) == 0 || op.err != "" {
		return op
	}
	for _, r := range rules {
		if op.input != nil && !hasFieldPath(op.input, strings.Split(r.Field, ".")) {
			op.err = "implied-action rule names field " + r.Field + ", which " + types.TypeString(op.input, nil) +
				" does not have (fix impliedRules)"
			op.actions = nil
			return op
		}
		for _, a := range r.Actions {
			if !containsString(op.actions, a) {
				op.actions = append(append([]string(nil), op.actions...), a)
			}
		}
	}
	return op
}

func containsString(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// hasFieldPath reports whether t (a pointer to a struct) has the dotted field.
// A list along the path is followed to its element type.
func hasFieldPath(t types.Type, path []string) bool {
	for _, name := range path {
		for {
			switch u := t.Underlying().(type) {
			case *types.Pointer:
				t = u.Elem()
				continue
			case *types.Slice:
				t = u.Elem()
				continue
			}
			break
		}
		st, ok := t.Underlying().(*types.Struct)
		if !ok {
			return false
		}
		found := false
		for i := 0; i < st.NumFields(); i++ {
			if f := st.Field(i); f.Name() == name {
				t, found = f.Type(), true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
