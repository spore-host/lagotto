package runtimeiam

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

const invokeTestAccount = "123456789012"

// pollerARN is the function ARN a region's poller schedule targets, built the
// way pkg/deploy.PollerFunctionARN builds it.
func pollerARN(region, account string) string {
	return fmt.Sprintf("arn:aws:lambda:%s:%s:function:%s", region, account, pollerFunctionName)
}

// liveSingleRegionInvokePolicy is the document #218 found on the shared role in
// a real account: written by a us-west-2 deploy, it leaves the account's
// us-east-1 schedule with nothing it may invoke.
const liveSingleRegionInvokePolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Action":"lambda:InvokeFunction",` +
	`"Resource":"arn:aws:lambda:us-west-2:123456789012:function:lagotto-capacity-poller"}]}`

// TestEnsureRoles_InvokePolicyConvergesAcrossRegions is the #218 gate, driven
// through the real write path. The invoke role is IAM-global and shared by every
// region's poller, so deploying region A and then region B must leave a policy
// that still lets A's schedule invoke A's poller — and B's deploy must write the
// same bytes A's did, or the two regions take turns breaking each other.
func TestEnsureRoles_InvokePolicyConvergesAcrossRegions(t *testing.T) {
	regions := []string{"us-east-1", "us-west-2", "eu-west-1"}
	key := SchedulerInvokeRoleName + "/" + SchedulerInvokePolicyName

	f := &fakeIAM{}
	written := make([]string, 0, len(regions))
	for _, region := range regions {
		if err := EnsureRoles(context.Background(), f, region, invokeTestAccount); err != nil {
			t.Fatalf("EnsureRoles(%s): %v", region, err)
		}
		doc, ok := f.inline[key]
		if !ok {
			t.Fatalf("deploy in %s wrote no %s", region, key)
		}
		written = append(written, doc)
	}

	for i := 1; i < len(written); i++ {
		if written[i] != written[0] {
			t.Errorf("the invoke policy depends on the deploying region — %s wrote\n  %s\nbut %s wrote\n  %s",
				regions[0], written[0], regions[i], written[i])
		}
	}

	// Whatever region deployed last, every region's poller must be invokable.
	final := written[len(written)-1]
	for _, region := range regions {
		cov, err := EvaluateInvokePolicy(final, pollerARN(region, invokeTestAccount))
		if err != nil {
			t.Fatalf("EvaluateInvokePolicy: %v", err)
		}
		if !cov.ThisRegion || !cov.AllRegions {
			t.Errorf("after deploying %v the shared invoke policy does not let %s's schedule invoke its poller (%+v):\n  %s",
				regions, region, cov, final)
		}
	}

	// Still scoped: lagotto's own function, in this account, and nothing else.
	for _, other := range []string{
		pollerARN("us-east-1", "999999999999"),
		"arn:aws:lambda:us-east-1:123456789012:function:some-other-function",
	} {
		cov, err := EvaluateInvokePolicy(final, other)
		if err != nil {
			t.Fatalf("EvaluateInvokePolicy: %v", err)
		}
		if cov.ThisRegion {
			t.Errorf("the invoke policy is broader than lagotto's poller: it allows %s", other)
		}
	}
}

// TestSchedulerInvokePolicyDocument_RequiresAccount: an empty account would
// render arn:aws:lambda:*::function:… — a policy that matches nothing.
func TestSchedulerInvokePolicyDocument_RequiresAccount(t *testing.T) {
	if _, err := SchedulerInvokePolicyDocument(""); err == nil {
		t.Error("want an error for an empty account ID")
	}
}

func TestEvaluateInvokePolicy(t *testing.T) {
	wildcard, err := SchedulerInvokePolicyDocument(invokeTestAccount)
	if err != nil {
		t.Fatal(err)
	}
	east := pollerARN("us-east-1", invokeTestAccount)
	west := pollerARN("us-west-2", invokeTestAccount)

	stmt := func(body string) string { return `{"Version":"2012-10-17","Statement":[` + body + `]}` }

	cases := []struct {
		name      string
		doc       string
		fn        string
		wantHere  bool
		wantAllRg bool
	}{
		{"the #218 live policy, from the region it was written in", liveSingleRegionInvokePolicy, west, true, false},
		{"the #218 live policy, from the region it broke", liveSingleRegionInvokePolicy, east, false, false},
		{"the policy this lagotto writes", wildcard, east, true, true},
		{"URL-encoded, as GetRolePolicy returns it", url.PathEscape(wildcard), west, true, true},
		{"action wildcard and resource *", stmt(`{"Effect":"Allow","Action":"lambda:*","Resource":"*"}`), east, true, true},
		{"action case is insensitive", stmt(`{"Effect":"Allow","Action":"Lambda:invokefunction","Resource":"` + east + `"}`), east, true, false},
		{"explicit deny beats the allow", stmt(`{"Effect":"Allow","Action":"lambda:InvokeFunction","Resource":"arn:aws:lambda:*:123456789012:function:lagotto-capacity-poller"},` +
			`{"Effect":"Deny","Action":"lambda:InvokeFunction","Resource":"` + east + `"}`), east, false, false},
		{"a conditioned allow is not counted", stmt(`{"Effect":"Allow","Action":"lambda:InvokeFunction","Resource":"*","Condition":{"StringEquals":{"aws:SourceAccount":"123456789012"}}}`), east, false, false},
		{"a different action", stmt(`{"Effect":"Allow","Action":"lambda:GetFunction","Resource":"*"}`), east, false, false},
		{"two explicit regions, from a third", stmt(`{"Effect":"Allow","Action":"lambda:InvokeFunction","Resource":["` + east + `","` + west + `"]}`), pollerARN("eu-west-1", invokeTestAccount), false, false},
		{"two explicit regions, from one of them", stmt(`{"Effect":"Allow","Action":"lambda:InvokeFunction","Resource":["` + east + `","` + west + `"]}`), east, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cov, err := EvaluateInvokePolicy(tc.doc, tc.fn)
			if err != nil {
				t.Fatalf("EvaluateInvokePolicy: %v", err)
			}
			if cov.ThisRegion != tc.wantHere || cov.AllRegions != tc.wantAllRg {
				t.Errorf("coverage of %s = %+v, want ThisRegion=%v AllRegions=%v", tc.fn, cov, tc.wantHere, tc.wantAllRg)
			}
		})
	}

	if _, err := EvaluateInvokePolicy("not json", east); err == nil {
		t.Error("an unparseable document must be an error, not 'not allowed'")
	}
}

func TestReadSchedulerInvokePolicy(t *testing.T) {
	ctx := context.Background()

	// Round-trips what deploy wrote.
	f := &fakeIAM{}
	if err := EnsureRoles(ctx, f, "us-east-1", invokeTestAccount); err != nil {
		t.Fatal(err)
	}
	doc, err := ReadSchedulerInvokePolicy(ctx, f)
	if err != nil {
		t.Fatalf("ReadSchedulerInvokePolicy: %v", err)
	}
	if !strings.Contains(doc, SchedulerInvokeFunctionARN(invokeTestAccount)) {
		t.Errorf("read back %s, want the document deploy wrote", doc)
	}

	if _, err := ReadSchedulerInvokePolicy(ctx, &fakeIAM{}); !errors.Is(err, ErrInvokeRoleNotFound) {
		t.Errorf("no role: err = %v, want ErrInvokeRoleNotFound", err)
	}
	f = &fakeIAM{existing: map[string]bool{SchedulerInvokeRoleName: true}}
	if _, err := ReadSchedulerInvokePolicy(ctx, f); !errors.Is(err, ErrInvokePolicyNotFound) {
		t.Errorf("role without policy: err = %v, want ErrInvokePolicyNotFound", err)
	}
	denied := errors.New("AccessDenied: not authorized to perform iam:GetRolePolicy")
	f = &fakeIAM{existing: map[string]bool{SchedulerInvokeRoleName: true}, getPolicyErr: denied}
	_, err = ReadSchedulerInvokePolicy(ctx, f)
	if !errors.Is(err, denied) || errors.Is(err, ErrInvokePolicyNotFound) || errors.Is(err, ErrInvokeRoleNotFound) {
		t.Errorf("AccessDenied: err = %v, want the read error itself, not an absence sentinel", err)
	}
}
