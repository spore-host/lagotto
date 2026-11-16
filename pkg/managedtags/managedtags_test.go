package managedtags

import (
	"testing"
	"time"

	"github.com/spore-host/libs/tags"
)

func TestServiceAndWorkload(t *testing.T) {
	now := time.Date(2026, 11, 16, 1, 2, 3, 0, time.UTC)
	s := Service("us-east-1", "AIDAEXAMPLE", ComponentPoller, now)
	want := map[string]string{
		tags.ManagedBy: "lagotto", tags.Scope: "us-east-1", tags.Created: "2026-11-16T01:02:03Z",
		tags.Schema: "1", tags.Owner: "AIDAEXAMPLE", tags.Role: "service", tags.Component: "capacity-poller",
	}
	if len(s) != len(want) {
		t.Fatalf("Service = %v, want %v", s, want)
	}
	for k, v := range want {
		if s[k] != v {
			t.Errorf("Service[%q] = %q, want %q", k, s[k], v)
		}
	}
	w := Workload("eu-west-1", "", now)
	if tags.IsService(w) || w[tags.ManagedBy] != "lagotto" || w[tags.Scope] != "eu-west-1" {
		t.Errorf("Workload = %v", w)
	}
	if _, ok := w[tags.Owner]; ok {
		t.Errorf("empty owner was written: %v", w)
	}
}

func TestConverge(t *testing.T) {
	desired := Service("us-east-1", "AIDANEW", ComponentAlerts, time.Unix(2000, 0))
	desired["Environment"] = "staging"

	// A resource with none of the tags gets all of them.
	if got := Converge(nil, desired); len(got) != len(desired) {
		t.Errorf("Converge(nil) = %v, want every desired tag", got)
	}

	// created and owner already set are kept; a changed Environment is written.
	existing := Service("us-east-1", "AIDAOLD", ComponentAlerts, time.Unix(1000, 0))
	existing["Environment"] = "production"
	existing["unrelated"] = "x"
	got := Converge(existing, desired)
	if len(got) != 1 || got["Environment"] != "staging" {
		t.Errorf("Converge = %v, want only Environment=staging", got)
	}

	// Steady state: nothing to write.
	if got := Converge(desired, desired); got != nil {
		t.Errorf("Converge(steady) = %v, want nil", got)
	}

	// An empty created on the resource is replaced.
	existing[tags.Created] = ""
	if got := Converge(existing, desired); got[tags.Created] == "" {
		t.Errorf("Converge did not replace an empty %s: %v", tags.Created, got)
	}
}

func TestWithDoesNotModifyArguments(t *testing.T) {
	a := map[string]string{"a": "1"}
	b := map[string]string{"b": "2", "a": "3"}
	got := With(a, b)
	if got["a"] != "3" || got["b"] != "2" || a["a"] != "1" || len(a) != 1 {
		t.Errorf("With = %v, a = %v", got, a)
	}
}

func TestStamp(t *testing.T) {
	at := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	s := Stamp{Owner: "AIDAX", Now: func() time.Time { return at }}
	if got := s.Service("global", ComponentCLIPolicy); got[tags.Created] != "2026-03-04T05:06:07Z" || got[tags.Owner] != "AIDAX" || !tags.IsService(got) {
		t.Errorf("Service = %v", got)
	}
	if got := s.Workload("us-east-1"); tags.IsService(got) || got[tags.Scope] != "us-east-1" {
		t.Errorf("Workload = %v", got)
	}
	if FirstStamp(nil).Owner != "" || FirstStamp([]Stamp{s}).Owner != "AIDAX" {
		t.Error("FirstStamp")
	}
	if _, ok := tags.CreatedAt(Stamp{}.Workload("r")); !ok {
		t.Error("the zero Stamp has no created time")
	}
}
