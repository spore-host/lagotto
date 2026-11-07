package quotacheck

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// The opt-in create-time quota gate (lagotto#157): `watch --maintain N
// --require-quota` refuses a fleet watch only when the account's quota PROVABLY
// can't hold N workers. Everything below is built so that "I couldn't tell" can
// never be read as "it won't fit": unknown is not zero (truffle#271).

// GateVerdict is the gate's answer.
type GateVerdict string

const (
	// GateFeasible: the regions whose quota could be read hold at least N
	// workers between them.
	GateFeasible GateVerdict = "feasible"
	// GateInfeasible: EVERY listed region's quota was read, every one gives a
	// definite worker count, and together they hold fewer than N. The only
	// verdict that refuses.
	GateInfeasible GateVerdict = "infeasible"
	// GateUnknown: the readable regions don't reach N, but at least one region
	// couldn't be counted (lookup failed, family not readable, a wildcard-only
	// pattern with free vCPUs, or the watch names no regions). Never refuses.
	GateUnknown GateVerdict = "unknown"
)

// GateRegion is one region's contribution to the gate.
type GateRegion struct {
	Region string `json:"region"`
	// Known is true when this region gives a definite worker count.
	Known bool `json:"known"`
	// MaxWorkers is the definite worker count when Known, else -1.
	MaxWorkers int   `json:"max_workers"`
	Limit      int32 `json:"limit,omitempty"`
	Used       int32 `json:"used,omitempty"`
	// Unknown says why the region couldn't be counted, when !Known.
	Unknown string `json:"unknown_reason,omitempty"`
}

// Gate is the result of the --require-quota check, also emitted in -o json
// (as "quota_gate").
type Gate struct {
	Verdict GateVerdict `json:"verdict"`
	Desired int         `json:"desired"`
	// MaxWorkers is the summed definite worker count of the Known regions — a
	// floor on what the fleet can reach, not a ceiling, when Verdict is unknown.
	MaxWorkers int          `json:"max_workers"`
	Regions    []GateRegion `json:"regions"`
	// Reason is the one-line human explanation of the verdict.
	Reason string `json:"reason"`
	// Remediation is the quota-increase hint, set only when Verdict is
	// infeasible.
	Remediation string `json:"remediation,omitempty"`
}

// gateRegion turns one region's Feasibility answer (or its error) into a
// definite worker count, or an explained "unknown".
func gateRegion(region string, rep Report, err error) GateRegion {
	g := GateRegion{Region: region, MaxWorkers: -1}
	switch {
	case err != nil:
		g.Unknown = fmt.Sprintf("could not read EC2 service quotas: %v", err)
	case !rep.Known:
		g.Unknown = fmt.Sprintf("no EC2 %s-family quota figure (needs servicequotas:GetServiceQuota, or the region doesn't expose it)", rep.Family)
	case rep.MaxWorkers >= 0:
		g.Known, g.MaxWorkers, g.Limit, g.Used = true, rep.MaxWorkers, rep.Limit, rep.Used
	case rep.Limit-rep.Used <= 0:
		// A wildcard-only pattern has no worker size, but with no free vCPU at
		// all no size fits: that is a count (zero), not a guess.
		g.Known, g.MaxWorkers, g.Limit, g.Used = true, 0, rep.Limit, rep.Used
	default:
		g.Limit, g.Used = rep.Limit, rep.Used
		g.Unknown = fmt.Sprintf("%d vCPU free, but the pattern has no literal size to count workers against", rep.Limit-rep.Used)
	}
	return g
}

// DecideGate is the pure decision. A fleet counts its workers across all its
// regions (the poller tops up wherever capacity appears), so the known regions'
// counts are SUMMED: the fleet is infeasible only when every region is known
// and the sum is short of desired.
func DecideGate(desired int, regions []GateRegion) Gate {
	g := Gate{Desired: desired, Regions: regions}
	if len(regions) == 0 {
		g.Verdict, g.MaxWorkers = GateUnknown, -1
		g.Reason = "the watch names no --regions, so there is no region whose quota could be checked"
		return g
	}
	var unknown []string
	for _, r := range regions {
		if r.Known {
			g.MaxWorkers += r.MaxWorkers
		} else {
			unknown = append(unknown, r.Region)
		}
	}
	switch {
	case g.MaxWorkers >= desired:
		g.Verdict = GateFeasible
		g.Reason = fmt.Sprintf("quota holds %d worker(s) across the readable region(s), enough for --maintain %d", g.MaxWorkers, desired)
	case len(unknown) > 0:
		g.Verdict = GateUnknown
		g.Reason = fmt.Sprintf("quota holds %d worker(s) in the readable region(s), short of --maintain %d, but the quota in %s could not be counted, so it is not provably short",
			g.MaxWorkers, desired, strings.Join(unknown, ", "))
	default:
		g.Verdict = GateInfeasible
		g.Reason = fmt.Sprintf("the EC2 vCPU quota holds at most %d worker(s) across %d region(s), short of --maintain %d",
			g.MaxWorkers, len(regions), desired)
	}
	return g
}

// RequireFeasible runs the --require-quota check for a fleet of `desired`
// workers of the given rungs across regions. Each region gets its own timeout,
// so one slow region reads as unknown rather than stalling the rest. It never
// returns an error: every failure is folded into an unknown region.
func RequireFeasible(ctx context.Context, l Lookup, regions, rungs []string, spot bool, desired int, perRegion time.Duration) Gate {
	if l == nil {
		g := DecideGate(desired, nil)
		g.Reason = "no quota reader is available"
		return g
	}
	out := make([]GateRegion, 0, len(regions))
	remediation := ""
	for _, region := range regions {
		rctx, cancel := context.WithTimeout(ctx, perRegion)
		rep, err := Feasibility(rctx, l, Request{Region: region, Rungs: rungs, Spot: spot, Desired: desired})
		cancel()
		if remediation == "" && err == nil && rep.Known {
			remediation = rep.Remediation
		}
		out = append(out, gateRegion(region, rep, err))
	}
	g := DecideGate(desired, out)
	if g.Verdict == GateInfeasible {
		g.Remediation = remediation
	}
	return g
}
