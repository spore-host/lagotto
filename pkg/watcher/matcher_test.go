package watcher

import (
	"io"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

func TestEvaluate_OnDemand_NoMaxPrice(t *testing.T) {
	w := &Watch{
		WatchID:             "w-test1",
		UserID:              "arn:aws:iam::123456789012:user/test",
		InstanceTypePattern: "g5.xlarge",
		Spot:                false,
	}
	candidate := MatchCandidate{
		InstanceType: truffleaws.InstanceTypeResult{
			InstanceType:  "g5.xlarge",
			Region:        "us-east-1",
			AvailableAZs:  []string{"us-east-1a", "us-east-1b"},
			OnDemandPrice: 1.006,
		},
	}

	m := Evaluate(w, candidate)
	if m == nil {
		t.Fatal("expected match, got nil")
	}
	if m.InstanceType != "g5.xlarge" {
		t.Errorf("instance type = %q, want g5.xlarge", m.InstanceType)
	}
	if m.Region != "us-east-1" {
		t.Errorf("region = %q, want us-east-1", m.Region)
	}
	if m.AvailabilityZone != "us-east-1a" {
		t.Errorf("az = %q, want us-east-1a", m.AvailabilityZone)
	}
	if m.Price != 1.006 {
		t.Errorf("price = %f, want 1.006", m.Price)
	}
	if m.IsSpot {
		t.Error("expected IsSpot = false")
	}
}

func TestEvaluate_OnDemand_MaxPriceExceeded(t *testing.T) {
	w := &Watch{
		WatchID:  "w-test2",
		Spot:     false,
		MaxPrice: 0.50,
	}
	candidate := MatchCandidate{
		InstanceType: truffleaws.InstanceTypeResult{
			InstanceType:  "g5.xlarge",
			Region:        "us-east-1",
			OnDemandPrice: 1.006,
		},
	}

	m := Evaluate(w, candidate)
	if m != nil {
		t.Errorf("expected no match when price exceeds max, got %+v", m)
	}
}

func TestEvaluate_OnDemand_MaxPriceMet(t *testing.T) {
	w := &Watch{
		WatchID:  "w-test3",
		Spot:     false,
		MaxPrice: 2.00,
	}
	candidate := MatchCandidate{
		InstanceType: truffleaws.InstanceTypeResult{
			InstanceType:  "g5.xlarge",
			Region:        "us-west-2",
			OnDemandPrice: 1.006,
		},
	}

	m := Evaluate(w, candidate)
	if m == nil {
		t.Fatal("expected match, got nil")
	}
	if m.Region != "us-west-2" {
		t.Errorf("region = %q, want us-west-2", m.Region)
	}
}

func TestEvaluate_Spot_Match(t *testing.T) {
	w := &Watch{
		WatchID:  "w-spot1",
		UserID:   "arn:aws:iam::123456789012:user/test",
		Spot:     true,
		MaxPrice: 0.50,
	}
	spotPrice := &truffleaws.SpotPriceResult{
		InstanceType:     "g5.xlarge",
		Region:           "us-east-1",
		AvailabilityZone: "us-east-1c",
		SpotPrice:        0.30,
	}
	candidate := MatchCandidate{
		SpotPrice: spotPrice,
	}

	m := Evaluate(w, candidate)
	if m == nil {
		t.Fatal("expected match, got nil")
	}
	if !m.IsSpot {
		t.Error("expected IsSpot = true")
	}
	if m.Price != 0.30 {
		t.Errorf("price = %f, want 0.30", m.Price)
	}
	if m.AvailabilityZone != "us-east-1c" {
		t.Errorf("az = %q, want us-east-1c", m.AvailabilityZone)
	}
}

func TestEvaluate_Spot_PriceExceeded(t *testing.T) {
	w := &Watch{
		WatchID:  "w-spot2",
		Spot:     true,
		MaxPrice: 0.20,
	}
	spotPrice := &truffleaws.SpotPriceResult{
		InstanceType: "g5.xlarge",
		SpotPrice:    0.30,
	}
	candidate := MatchCandidate{
		SpotPrice: spotPrice,
	}

	m := Evaluate(w, candidate)
	if m != nil {
		t.Errorf("expected no match when spot price exceeds max, got %+v", m)
	}
}

func TestEvaluate_Spot_NoSpotData(t *testing.T) {
	w := &Watch{
		WatchID: "w-spot3",
		Spot:    true,
	}
	candidate := MatchCandidate{
		InstanceType: truffleaws.InstanceTypeResult{
			InstanceType: "g5.xlarge",
			Region:       "us-east-1",
		},
		// No SpotPrice
	}

	m := Evaluate(w, candidate)
	if m != nil {
		t.Errorf("expected no match when spot watch has no spot data, got %+v", m)
	}
}

func TestEvaluate_Spot_NoMaxPrice(t *testing.T) {
	w := &Watch{
		WatchID:  "w-spot-any",
		UserID:   "arn:aws:iam::123456789012:user/test",
		Spot:     true,
		MaxPrice: 0, // any price
	}
	spotPrice := &truffleaws.SpotPriceResult{
		InstanceType:     "p5.48xlarge",
		Region:           "us-east-1",
		AvailabilityZone: "us-east-1b",
		SpotPrice:        98.50,
	}
	candidate := MatchCandidate{SpotPrice: spotPrice}

	m := Evaluate(w, candidate)
	if m == nil {
		t.Fatal("expected match with MaxPrice=0 (any), got nil")
	}
	if m.Price != 98.50 {
		t.Errorf("price = %f, want 98.50", m.Price)
	}
}

func TestEvaluate_OnDemand_NoAZs(t *testing.T) {
	w := &Watch{
		WatchID: "w-noaz",
		Spot:    false,
	}
	candidate := MatchCandidate{
		InstanceType: truffleaws.InstanceTypeResult{
			InstanceType:  "g5.xlarge",
			Region:        "us-east-1",
			AvailableAZs:  nil, // empty
			OnDemandPrice: 1.006,
		},
	}

	m := Evaluate(w, candidate)
	if m == nil {
		t.Fatal("expected match even with no AZs, got nil")
	}
	if m.AvailabilityZone != "" {
		t.Errorf("az = %q, want empty", m.AvailabilityZone)
	}
}

func TestEvaluate_OnDemand_CarriesAllCandidateAZs(t *testing.T) {
	w := &Watch{WatchID: "w-azs", Spot: false}
	candidate := MatchCandidate{
		InstanceType: truffleaws.InstanceTypeResult{
			InstanceType:  "g5.12xlarge",
			Region:        "us-west-2",
			AvailableAZs:  []string{"us-west-2a", "us-west-2b", "us-west-2c"},
			OnDemandPrice: 5.67,
		},
	}
	m := Evaluate(w, candidate)
	if m == nil {
		t.Fatal("expected match")
	}
	// All offered AZs are carried for retry; the primary is the first.
	if m.AvailabilityZone != "us-west-2a" {
		t.Errorf("primary AZ = %q, want us-west-2a", m.AvailabilityZone)
	}
	if len(m.CandidateAZs) != 3 {
		t.Errorf("CandidateAZs = %v, want all 3 offered", m.CandidateAZs)
	}
}

func TestEvaluate_OnDemand_AZsPinnedAndOrdered(t *testing.T) {
	// --azs pins us-west-2c,us-west-2b — only those, in that order, from the offered set.
	w := &Watch{WatchID: "w-pin", Spot: false, AvailabilityZones: []string{"us-west-2c", "us-west-2b"}}
	candidate := MatchCandidate{
		InstanceType: truffleaws.InstanceTypeResult{
			InstanceType:  "g5.12xlarge",
			Region:        "us-west-2",
			AvailableAZs:  []string{"us-west-2a", "us-west-2b", "us-west-2c"},
			OnDemandPrice: 5.67,
		},
	}
	m := Evaluate(w, candidate)
	if m == nil {
		t.Fatal("expected match")
	}
	want := []string{"us-west-2c", "us-west-2b"}
	if len(m.CandidateAZs) != 2 || m.CandidateAZs[0] != "us-west-2c" || m.CandidateAZs[1] != "us-west-2b" {
		t.Errorf("CandidateAZs = %v, want %v (pinned + ordered, us-west-2a excluded)", m.CandidateAZs, want)
	}
	if m.AvailabilityZone != "us-west-2c" {
		t.Errorf("primary AZ = %q, want us-west-2c (first preference)", m.AvailabilityZone)
	}
}

func TestEvaluate_OnDemand_PinnedAZNotOffered_NoMatch(t *testing.T) {
	// User pinned an AZ that doesn't offer the type this poll → no match.
	w := &Watch{WatchID: "w-pin-miss", Spot: false, AvailabilityZones: []string{"us-west-2d"}}
	candidate := MatchCandidate{
		InstanceType: truffleaws.InstanceTypeResult{
			InstanceType:  "g5.12xlarge",
			Region:        "us-west-2",
			AvailableAZs:  []string{"us-west-2a", "us-west-2b"},
			OnDemandPrice: 5.67,
		},
	}
	if m := Evaluate(w, candidate); m != nil {
		t.Errorf("expected no match when pinned AZ isn't offered, got %+v", m)
	}
}

func TestEvaluate_Spot_AZPinExcludes(t *testing.T) {
	// Spot price is in us-east-1c, but the watch pins us-east-1a → no match.
	w := &Watch{WatchID: "w-spot-pin", Spot: true, MaxPrice: 0, AvailabilityZones: []string{"us-east-1a"}}
	candidate := MatchCandidate{SpotPrice: &truffleaws.SpotPriceResult{
		InstanceType: "g5.xlarge", Region: "us-east-1", AvailabilityZone: "us-east-1c", SpotPrice: 0.3,
	}}
	if m := Evaluate(w, candidate); m != nil {
		t.Errorf("expected no match when spot AZ excluded by pin, got %+v", m)
	}
}

func TestOrderAZs(t *testing.T) {
	// No preference → offered unchanged.
	got := orderAZs([]string{"a", "b", "c"}, nil)
	if len(got) != 3 || got[0] != "a" {
		t.Errorf("no-pref should return offered as-is, got %v", got)
	}
	// Preference filters + reorders to pref order, dropping unoffered prefs.
	got = orderAZs([]string{"a", "b", "c"}, []string{"c", "z", "a"})
	if len(got) != 2 || got[0] != "c" || got[1] != "a" {
		t.Errorf("orderAZs filter+order = %v, want [c a]", got)
	}
}

func TestAZAllowed(t *testing.T) {
	if !azAllowed("us-west-2a", nil) {
		t.Error("empty pref should allow any AZ")
	}
	if !azAllowed("", []string{"us-west-2a"}) {
		t.Error("empty AZ should be allowed (provider reported none)")
	}
	if azAllowed("us-west-2b", []string{"us-west-2a"}) {
		t.Error("AZ not in pref should be disallowed")
	}
	if !azAllowed("us-west-2a", []string{"us-west-2a", "us-west-2b"}) {
		t.Error("AZ in pref should be allowed")
	}
}

func TestWildcardToRegex(t *testing.T) {
	tests := []struct {
		pattern string
		want    string
	}{
		{"p5.*", `^p5\..*$`},
		{"g5.xlarge", `^g5\.xlarge$`},
		{"t3.micro", `^t3\.micro$`},
		{"^p5\\..*$", `^p5\..*$`}, // already regex, pass through
	}
	for _, tt := range tests {
		got := wildcardToRegex(tt.pattern)
		if got != tt.want {
			t.Errorf("wildcardToRegex(%q) = %q, want %q", tt.pattern, got, tt.want)
		}
	}
}

func TestPatternToRegex_SinglePatternUnchanged(t *testing.T) {
	// A single (comma-free) pattern must convert exactly as wildcardToRegex did,
	// so existing wildcard/exact behavior is preserved (#135).
	for _, p := range []string{"p5.*", "g5.xlarge", "t3.micro", `^p5\..*$`} {
		if got, want := patternToRegex(p), wildcardToRegex(p); got != want {
			t.Errorf("patternToRegex(%q) = %q, want %q (single pattern must match wildcardToRegex)", p, got, want)
		}
	}
}

func TestPatternToRegex_CommaListMatchesAnyRung(t *testing.T) {
	// The comma-list from the #135 repro should compile and match ANY listed
	// rung, but not an unlisted one.
	pattern := "g6.8xlarge,g6.4xlarge,g6.2xlarge,g6.xlarge"
	re, err := regexp.Compile(patternToRegex(pattern))
	if err != nil {
		t.Fatalf("compile %q: %v", patternToRegex(pattern), err)
	}
	for _, typ := range []string{"g6.8xlarge", "g6.4xlarge", "g6.2xlarge", "g6.xlarge"} {
		if !re.MatchString(typ) {
			t.Errorf("comma-list %q should match rung %q", pattern, typ)
		}
	}
	for _, typ := range []string{"g6.16xlarge", "g6.48xlarge", "g5.xlarge", "g6.8xlarge,g6.4xlarge"} {
		if re.MatchString(typ) {
			t.Errorf("comma-list %q should NOT match %q", pattern, typ)
		}
	}
}

func TestPatternToRegex_CommaListWithWildcardsAndSpaces(t *testing.T) {
	// Sub-patterns may be wildcards, and surrounding spaces are trimmed.
	pattern := " p5.* , g5.xlarge "
	re, err := regexp.Compile(patternToRegex(pattern))
	if err != nil {
		t.Fatalf("compile %q: %v", patternToRegex(pattern), err)
	}
	for _, typ := range []string{"p5.48xlarge", "p5.4xlarge", "g5.xlarge"} {
		if !re.MatchString(typ) {
			t.Errorf("pattern %q should match %q", pattern, typ)
		}
	}
	for _, typ := range []string{"g5.2xlarge", "p4d.24xlarge"} {
		if re.MatchString(typ) {
			t.Errorf("pattern %q should NOT match %q", pattern, typ)
		}
	}
}

func TestWarnUnmatchablePattern(t *testing.T) {
	capture := func(w *Watch) string {
		t.Helper()
		old := os.Stderr
		r, wr, err := os.Pipe()
		if err != nil {
			t.Fatalf("os.Pipe: %v", err)
		}
		os.Stderr = wr
		warnUnmatchablePattern(w)
		_ = wr.Close()
		os.Stderr = old
		out, _ := io.ReadAll(r)
		return string(out)
	}

	// First poll (LastPolledAt zero) + unmatchable pattern -> warns, and the
	// message carries the watch ID and a typo hint (#135).
	w := &Watch{WatchID: "w-x", InstanceTypePattern: "g6.8xlarge,g6.4xlarge", Regions: []string{"us-east-1"}}
	if got := capture(w); !strings.Contains(got, "matches no known instance type") ||
		!strings.Contains(got, "w-x") || !strings.Contains(got, "check for typos") {
		t.Errorf("first-poll warning missing/incomplete: %q", got)
	}

	// Already polled -> silent, so a persistently-empty pattern doesn't spam a
	// warning every cycle.
	w2 := &Watch{WatchID: "w-y", InstanceTypePattern: "typo", LastPolledAt: time.Now()}
	if got := capture(w2); got != "" {
		t.Errorf("expected no warning after first poll, got %q", got)
	}

	// Nil watch is a no-op.
	if got := capture(nil); got != "" {
		t.Errorf("nil watch should produce no warning, got %q", got)
	}
}

func TestSplitInstanceTypePatterns(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"g5.xlarge", []string{"g5.xlarge"}},
		{"g6.4xlarge,g6.2xlarge", []string{"g6.4xlarge", "g6.2xlarge"}},
		{" p5.* , g5.xlarge ", []string{"p5.*", "g5.xlarge"}},
		{"a,,b,", []string{"a", "b"}}, // empty entries dropped
		{"", nil},
		{"   ", nil},
		{" , ", nil},
	}
	for _, tt := range tests {
		got := splitInstanceTypePatterns(tt.in)
		if len(got) != len(tt.want) {
			t.Errorf("splitInstanceTypePatterns(%q) = %v, want %v", tt.in, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("splitInstanceTypePatterns(%q)[%d] = %q, want %q", tt.in, i, got[i], tt.want[i])
			}
		}
	}
}

// TestEvaluate_CarriesVCPUs verifies both Evaluate branches stamp the matched
// type's authoritative vCPU count onto the MatchResult (#153) — quotas are
// denominated in vCPUs per family, so the poller needs it to explain a refusal.
func TestEvaluate_CarriesVCPUs(t *testing.T) {
	typeResult := truffleaws.InstanceTypeResult{
		InstanceType:  "g6e.xlarge",
		Region:        "us-east-1",
		AvailableAZs:  []string{"us-east-1a"},
		OnDemandPrice: 2.24,
		VCPUs:         4,
	}

	// On-demand branch.
	m := Evaluate(&Watch{WatchID: "w-v1", InstanceTypePattern: "g6e.xlarge"},
		MatchCandidate{InstanceType: typeResult})
	if m == nil {
		t.Fatal("on-demand: no match")
	}
	if m.VCPUs != 4 {
		t.Errorf("on-demand VCPUs = %d, want 4", m.VCPUs)
	}

	// Spot branch: searchBestMatch back-fills the InstanceTypeResult onto the
	// candidate, so the vCPU count is available even though the type is named by
	// the spot price.
	m = Evaluate(&Watch{WatchID: "w-v2", InstanceTypePattern: "g6e.xlarge", Spot: true},
		MatchCandidate{
			InstanceType: typeResult,
			SpotPrice: &truffleaws.SpotPriceResult{
				InstanceType: "g6e.xlarge", Region: "us-east-1",
				AvailabilityZone: "us-east-1a", SpotPrice: 0.8,
			},
		})
	if m == nil {
		t.Fatal("spot: no match")
	}
	if m.VCPUs != 4 {
		t.Errorf("spot VCPUs = %d, want 4", m.VCPUs)
	}

	// No vCPU figure available (a hand-built candidate) → left at zero, never a
	// guessed value.
	m = Evaluate(&Watch{WatchID: "w-v3", InstanceTypePattern: "g6e.xlarge"},
		MatchCandidate{InstanceType: truffleaws.InstanceTypeResult{
			InstanceType: "g6e.xlarge", Region: "us-east-1", AvailableAZs: []string{"us-east-1a"},
		}})
	if m == nil {
		t.Fatal("no-vcpu: no match")
	}
	if m.VCPUs != 0 {
		t.Errorf("no-vcpu VCPUs = %d, want 0", m.VCPUs)
	}
}
