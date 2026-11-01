package deploy

import (
	"reflect"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/runtimeiam"
)

// TestInvokePolicyCoversEveryPollerTarget (#218): the Scheduler invoke role is
// shared by every region's poller schedule and by the per-launch schedules, all
// of which target PollerFunctionARN(region, account). The policy runtimeiam
// writes must allow invoking that exact ARN in any region — this is what catches
// the two copies of the function name drifting apart.
func TestInvokePolicyCoversEveryPollerTarget(t *testing.T) {
	const account = "123456789012"
	doc, err := runtimeiam.SchedulerInvokePolicyDocument(account)
	if err != nil {
		t.Fatal(err)
	}
	for _, region := range []string{"us-east-1", "us-west-2", "ap-southeast-2"} {
		cov, err := runtimeiam.EvaluateInvokePolicy(doc, PollerFunctionARN(region, account))
		if err != nil {
			t.Fatal(err)
		}
		if !cov.ThisRegion || !cov.AllRegions {
			t.Errorf("the invoke policy does not cover %s (%+v): %s", PollerFunctionARN(region, account), cov, doc)
		}
	}
}

// TestTeardownCannotTouchIAM (#218): the two poller roles and the invoke policy
// are IAM-global and shared by every region's poller, so a `deploy --teardown`
// in one region must never delete or rewrite them — that would break the
// pollers still running in other regions. Teardown is per-region and decides
// nothing about IAM: it simply has no IAM client. This pins that structurally;
// adding an IAM client to the Deployer must come with a cross-region
// reference-count, not slip in silently.
func TestTeardownCannotTouchIAM(t *testing.T) {
	typ := reflect.TypeOf(Deployer{})
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if usesIAM(f.Type) {
			t.Errorf("Deployer.%s (%s) can reach IAM; teardown in one region must not be able to remove the shared poller roles/policy (#218)", f.Name, f.Type)
		}
	}
}

// usesIAM reports whether t is, points to, or is an interface with a method
// mentioning, a type from the IAM SDK package.
func usesIAM(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if strings.HasSuffix(t.PkgPath(), "/service/iam") {
		return true
	}
	if t.Kind() == reflect.Interface {
		for i := 0; i < t.NumMethod(); i++ {
			m := t.Method(i).Type
			for j := 0; j < m.NumIn(); j++ {
				if typeFromIAM(m.In(j)) {
					return true
				}
			}
		}
	}
	return false
}

func typeFromIAM(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice {
		t = t.Elem()
	}
	return strings.HasSuffix(t.PkgPath(), "/service/iam")
}
