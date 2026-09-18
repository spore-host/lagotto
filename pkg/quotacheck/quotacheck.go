// Package quotacheck turns an account's EC2 Service Quotas snapshot into the one
// answer a fleet watch needs: "how many workers of this shape can this account
// actually run, and what do I tell the user if the answer is fewer than they
// asked for?" (lagotto#153).
//
// It is a leaf like pkg/failure — no poller state, no DynamoDB, no store — but
// deliberately NOT part of pkg/failure, whose package doc promises no AWS service
// SDKs: the quota numbers come from truffle/pkg/quotas, which pulls in
// servicequotas + ec2.
//
// The load-bearing property is that every number here is OPTIONAL GARNISH. The
// actual quota ceiling lagotto reports is detected from the RunInstances error
// (pkg/failure.IsQuotaExceeded — pure error inspection, no new IAM), and every
// entry point below is nil-safe and fails open, so a poller running a pre-0.59
// runtime policy (no servicequotas:GetServiceQuota) still reports "capped at
// M/N" — it just reports it without the exact limit/usage figures. That is on
// purpose: lagotto#148 and #150 were both "a fix that silently no-ops if you
// forget to re-run `lagotto setup`", and this must not become a third.
package quotacheck

import (
	"context"
	"fmt"
	"strings"

	"github.com/spore-host/truffle/pkg/quotas"
)

// Lookup is the slice of truffle's quota client this package needs — ONE method,
// so a test can inject a fake without a live Service Quotas API.
// *quotas.Client satisfies it.
//
// CanLaunch is deliberately excluded from the seam: it collapses "quota is zero"
// and "the quota lookup failed" into the same false (see Headroom's two-value map
// read below), and its arithmetic — (limit - usage) / vCPUs-per-worker — is
// trivially reproduced here in a form that keeps the distinction.
type Lookup interface {
	GetQuotas(ctx context.Context, region string) (*quotas.QuotaInfo, error)
}

// sizeVCPUs maps an instance type's size suffix to its vCPU count. It duplicates
// truffle's unexported getVCPUCount (pkg/quotas/quotas.go) verbatim; truffle#167
// asks for that helper to be exported so this table can be deleted. Unlike
// truffle's version, an unmappable size here returns ok=false rather than a
// silent 2-vCPU estimate — a feasibility warning built on a guess is worse than
// no warning.
var sizeVCPUs = map[string]int32{
	"nano":      1,
	"micro":     1,
	"small":     1,
	"medium":    1,
	"large":     2,
	"xlarge":    4,
	"2xlarge":   8,
	"3xlarge":   12,
	"4xlarge":   16,
	"6xlarge":   24,
	"8xlarge":   32,
	"9xlarge":   36,
	"10xlarge":  40,
	"12xlarge":  48,
	"16xlarge":  64,
	"18xlarge":  72,
	"24xlarge":  96,
	"32xlarge":  128,
	"48xlarge":  192,
	"56xlarge":  224,
	"112xlarge": 448,
}

// VCPUsForType returns the vCPU count for a LITERAL instance type, reporting
// ok=false for anything it can't map exactly — a wildcard ("g6e.*"), a
// family-only pattern ("g6e"), an empty string, or a size AWS has added that the
// table above doesn't know. Callers use ok=false to mean "I can't convert a vCPU
// quota into a worker count", not "zero vCPUs".
func VCPUsForType(instanceType string) (int32, bool) {
	if strings.ContainsAny(instanceType, "*?[]^$|") {
		return 0, false
	}
	parts := strings.Split(instanceType, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, false
	}
	size := parts[1]
	if v, ok := sizeVCPUs[size]; ok {
		return v, true
	}
	// General pattern: NxLarge has N*4 vCPUs (e.g. "20xlarge" → 80), matching
	// truffle's fallback.
	if rest := strings.TrimSuffix(size, "xlarge"); rest != size && rest != "" {
		var n int32
		if _, err := fmt.Sscanf(rest, "%d", &n); err == nil && n > 0 {
			return n * 4, true
		}
	}
	return 0, false
}

// Headroom reads one family's vCPU limit and current usage out of a quota
// snapshot, using a TWO-VALUE map read.
//
// That distinction is the crux of this package. truffle's GetQuotas `continue`s
// past any per-quota error (no permission, throttled, quota not offered in the
// region), so a family key is ABSENT when the lookup FAILED and PRESENT-with-0
// when the account genuinely has zero quota. A one-value read would report both
// as "your quota is 0", which would turn a missing servicequotas read permission
// into a scary and wrong "this fleet cannot launch a single worker". known=false
// therefore means "no answer", and callers must degrade to a quiet, numberless
// report rather than warn.
func Headroom(info *quotas.QuotaInfo, family quotas.QuotaFamily, spot bool) (limit, used int32, known bool) {
	if info == nil {
		return 0, 0, false
	}
	if spot {
		limit, known = info.Spot[family]
		return limit, info.SpotUsage[family], known
	}
	limit, known = info.OnDemand[family]
	return limit, info.Usage[family], known
}

// Request describes the fleet whose feasibility we're asking about.
type Request struct {
	Region string
	// Rungs are the watch pattern's sub-patterns (#135 comma-list), e.g.
	// ["g6e.4xlarge", "g6e.xlarge"]. Split with watcher.SplitInstanceTypePatterns
	// so create-time and the poller agree on the semantics.
	Rungs   []string
	Spot    bool
	Desired int // --maintain N
}

// Report is the answer. Known=false means the quota lookup produced nothing
// usable, in which case every numeric field is meaningless and the caller must
// stay quiet about numbers (see Headroom).
type Report struct {
	// Known is false when the family's quota couldn't be read at all.
	Known bool `json:"known"`
	// Family is the Service Quotas family the pattern maps to (derived from the
	// first rung; wildcards work — GetQuotaFamily keys off the letter prefix).
	Family quotas.QuotaFamily `json:"family"`
	// Limit / Used are vCPUs: the family's quota and the account's current usage
	// in Region for the requested lifecycle (spot or on-demand).
	Limit int32 `json:"limit"`
	Used  int32 `json:"used"`
	// SmallestRung is the smallest LITERAL rung in the pattern — the one that
	// yields the largest feasible worker count, i.e. the most optimistic reading.
	SmallestRung   string `json:"smallest_rung,omitempty"`
	VCPUsPerWorker int32  `json:"vcpus_per_worker,omitempty"`
	// MaxWorkers is how many SmallestRung workers fit in the remaining headroom,
	// or -1 when the pattern has no literal rung to count against (a pure
	// wildcard) — "unknown", not "zero".
	MaxWorkers int `json:"max_workers"`
	// Feasible is false only when we can positively say the fleet can't reach
	// Desired. An unknown worker size with free headroom is NOT reported
	// infeasible — we don't warn on a guess.
	Feasible bool `json:"feasible"`
	// ZeroQuota is true only for a READ quota of exactly 0 — the "you cannot
	// launch a single worker" case, distinct from !Known.
	ZeroQuota bool `json:"zero_quota"`
	// MixedRungs is true when the pattern lists more than one literal rung, so
	// which rung matches (and therefore how many fit) isn't fixed at create time
	// — the caller should say "at best N × <SmallestRung>".
	MixedRungs bool `json:"mixed_rungs"`
	// Remediation is the quota-increase hint, empty when Known is false.
	Remediation string `json:"remediation,omitempty"`
}

// Feasibility answers "can this fleet reach its --maintain goal under the
// account's quota?". It fails open in every direction: a nil Lookup, a lookup
// error, or an absent family key all yield a Report with Known=false (and, for a
// lookup error, that error, so a verbose caller can say why) rather than a
// pessimistic verdict.
func Feasibility(ctx context.Context, l Lookup, r Request) (Report, error) {
	rep := Report{MaxWorkers: -1}
	if len(r.Rungs) == 0 {
		return rep, fmt.Errorf("quotacheck: no instance-type rungs to check")
	}
	// GetQuotaFamily keys off the leading letter run, so it works on wildcards
	// and family-only patterns too ("g6e.*" → "g" → FamilyG).
	rep.Family = quotas.GetQuotaFamily(r.Rungs[0])

	// Pick the SMALLEST literal rung: it gives the largest feasible count, so a
	// warning based on it is the most conservative claim we can make ("even at
	// your smallest listed size, this doesn't fit").
	literals := 0
	for _, rung := range r.Rungs {
		v, ok := VCPUsForType(rung)
		if !ok {
			continue
		}
		literals++
		if rep.VCPUsPerWorker == 0 || v < rep.VCPUsPerWorker {
			rep.VCPUsPerWorker, rep.SmallestRung = v, rung
		}
	}
	rep.MixedRungs = literals > 1

	if l == nil {
		return rep, nil
	}
	info, err := l.GetQuotas(ctx, r.Region)
	if err != nil {
		return rep, err
	}
	limit, used, known := Headroom(info, rep.Family, r.Spot)
	if !known {
		return rep, nil
	}
	rep.Known, rep.Limit, rep.Used = true, limit, used
	rep.ZeroQuota = limit == 0

	avail := limit - used
	if avail < 0 {
		avail = 0
	}
	if rep.VCPUsPerWorker > 0 {
		rep.MaxWorkers = int(avail / rep.VCPUsPerWorker)
		rep.Feasible = rep.MaxWorkers >= r.Desired
	} else {
		// No literal rung: we know the vCPU headroom but not the worker size, so
		// we can only be definite when there's no headroom at all.
		rep.MaxWorkers = -1
		rep.Feasible = avail > 0
	}

	desiredVCPUs := used + int32(r.Desired)*rep.VCPUsPerWorker
	if desiredVCPUs <= limit {
		// Ask for at least a little more than the current ceiling, so the hint is
		// never "raise your quota to the value it already has".
		desiredVCPUs = limit + 1
	}
	rep.Remediation = Remediation(r.Region, rep.Family, desiredVCPUs, r.Spot)
	return rep, nil
}

// quotaKey identifies one (family, lifecycle) Service Quotas entry.
type quotaKey struct {
	family quotas.QuotaFamily
	spot   bool
}

// remediableQuotaCodes is the allow-list of (family, lifecycle) pairs for which
// truffle's QuotaIncreaseCommand emits the CORRECT quota code — and it exists to
// guard a real upstream bug (truffle#167): that function's switch covers only
// these pairs and silently FALLS THROUGH to the Standard On-Demand code
// (L-1216C47A) for anything else. So for, say, an X-family on-demand fleet or a
// Trn spot fleet it would print a command that raises the wrong quota (and, for
// spot, the wrong lifecycle entirely) while looking authoritative.
//
// Rather than print a confidently wrong command, Remediation points at the
// console for unmapped pairs. Keying the allow-list off truffle's own exported
// QuotaCode* constants keeps the two in step, and TestRemediation_Truffle167Guard
// asserts the mapped pairs really do emit their own code — so when truffle#167
// lands, extending this map is the only change needed.
var remediableQuotaCodes = map[quotaKey]string{
	{quotas.FamilyStandard, false}: quotas.QuotaCodeStandard,
	{quotas.FamilyG, false}:        quotas.QuotaCodeG,
	{quotas.FamilyP, false}:        quotas.QuotaCodeP,
	{quotas.FamilyInf, false}:      quotas.QuotaCodeInf,
	{quotas.FamilyTrn, false}:      quotas.QuotaCodeTrn,
	{quotas.FamilyStandard, true}:  quotas.QuotaCodeSpotStandard,
	{quotas.FamilyG, true}:         quotas.QuotaCodeSpotG,
	{quotas.FamilyP, true}:         quotas.QuotaCodeSpotP,
}

// lifecycleLabel renders the quota lifecycle the way AWS names it.
func lifecycleLabel(spot bool) string {
	if spot {
		return "Spot"
	}
	return "On-Demand"
}

// Remediation returns the quota-increase hint for a family: truffle's ready-made
// `aws service-quotas request-service-quota-increase` block when the pair is in
// remediableQuotaCodes, and a console pointer otherwise (see that var's comment —
// truffle#167).
func Remediation(region string, family quotas.QuotaFamily, desiredVCPUs int32, spot bool) string {
	if _, ok := remediableQuotaCodes[quotaKey{family, spot}]; ok {
		return quotas.QuotaIncreaseCommand(region, family, desiredVCPUs, spot)
	}
	return fmt.Sprintf(
		"Raise the EC2 %q %s vCPU quota in %s to at least %d in the Service Quotas console: "+
			"https://%s.console.aws.amazon.com/servicequotas/home/services/ec2/quotas "+
			"(no exact CLI command is printed for this family/lifecycle — truffle#167)",
		family, lifecycleLabel(spot), region, desiredVCPUs, region)
}

// maxCapReasonBytes bounds the persisted cap explanation. It rides on a DynamoDB
// attribute and is echoed in notifications and `lagotto status`, so it stays a
// short paragraph rather than an unbounded command block.
const maxCapReasonBytes = 512

// CapReason composes the one-line human explanation persisted on the watch
// (QuotaCapReason) and echoed in the notification and `lagotto status`.
//
// rep may be nil — that's the no-lookup / lookup-failed path, which is the DEFAULT
// on a poller running a pre-0.59 runtime policy. In that case the sentence names
// the family and the remediation but no invented numbers.
func CapReason(region, instanceType string, spot bool, desired, running int, rep *Report) string {
	family := quotas.GetQuotaFamily(instanceType)
	if rep != nil && rep.Family != "" {
		family = rep.Family
	}
	var b strings.Builder
	fmt.Fprintf(&b, "fleet capped at %d/%d by the EC2 %s-family %s vCPU quota in %s",
		running, desired, family, lifecycleLabel(spot), region)
	if instanceType != "" {
		fmt.Fprintf(&b, " (RunInstances for %s was refused)", instanceType)
	}
	if rep != nil && rep.Known {
		fmt.Fprintf(&b, "; quota is %d vCPU with %d in use", rep.Limit, rep.Used)
	}
	b.WriteString(". ")

	// Target: enough headroom for the full goal, best-effort from whichever vCPU
	// figure we have.
	perWorker := int32(0)
	if rep != nil {
		perWorker = rep.VCPUsPerWorker
	}
	if perWorker == 0 {
		if v, ok := VCPUsForType(instanceType); ok {
			perWorker = v
		}
	}
	desiredVCPUs := int32(desired) * perWorker
	if rep != nil && rep.Known && desiredVCPUs <= rep.Limit {
		desiredVCPUs = rep.Limit + 1
	}
	if code, ok := remediableQuotaCodes[quotaKey{family, spot}]; ok && desiredVCPUs > 0 {
		fmt.Fprintf(&b, "Raise it: aws service-quotas request-service-quota-increase --service-code ec2 --quota-code %s --desired-value %d --region %s",
			code, desiredVCPUs, region)
	} else {
		fmt.Fprintf(&b, "Raise the EC2 %q %s vCPU quota in %s in the Service Quotas console.",
			family, lifecycleLabel(spot), region)
	}
	return truncate(b.String(), maxCapReasonBytes)
}

// truncate clips s to at most n bytes without splitting a multi-byte rune.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !isRuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// isRuneStart reports whether b can begin a UTF-8 encoded rune.
func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }
