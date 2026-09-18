package watcher

import (
	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

// MatchCandidate represents a potential capacity match to evaluate against a watch.
type MatchCandidate struct {
	InstanceType truffleaws.InstanceTypeResult
	SpotPrice    *truffleaws.SpotPriceResult // nil if not a Spot check
}

// Evaluate checks whether a candidate satisfies the watch criteria.
// Returns a MatchResult if the candidate matches, nil otherwise.
func Evaluate(w *Watch, c MatchCandidate) *MatchResult {
	// If the watch requires Spot and we have Spot pricing, check price
	if w.Spot && c.SpotPrice != nil {
		if w.MaxPrice > 0 && c.SpotPrice.SpotPrice > w.MaxPrice {
			return nil
		}
		az := c.SpotPrice.AvailabilityZone
		// A spot price is for one AZ; honor an --azs pin if it excludes that AZ.
		if !azAllowed(az, w.AvailabilityZones) {
			return nil
		}
		var candidates []string
		if az != "" {
			candidates = []string{az}
		}
		m := &MatchResult{
			WatchID:          w.WatchID,
			UserID:           w.UserID,
			Region:           c.SpotPrice.Region,
			AvailabilityZone: az,
			CandidateAZs:     candidates,
			InstanceType:     c.SpotPrice.InstanceType,
			Price:            c.SpotPrice.SpotPrice,
			IsSpot:           true,
			ActionTaken:      "pending",
		}
		// Carry the authoritative vCPU count so a quota refusal can be explained in
		// the units quotas are actually denominated in (#153). The spot branch names
		// the type from the spot price, but searchBestMatch back-fills the matching
		// InstanceTypeResult onto the candidate, so this is usually populated; guard
		// so a hand-built candidate doesn't stamp a zero.
		if c.InstanceType.VCPUs > 0 {
			m.VCPUs = c.InstanceType.VCPUs
		}
		return m
	}

	// On-demand availability check: instance type exists in the region
	if !w.Spot {
		price := c.InstanceType.OnDemandPrice
		if w.MaxPrice > 0 && price > w.MaxPrice {
			return nil
		}
		// Restrict/order the offered AZs by the watch's --azs preference (empty =
		// all offered AZs, in truffle's order). All eligible AZs are carried so the
		// spawner can retry the next on InsufficientInstanceCapacity (#34).
		candidates := orderAZs(c.InstanceType.AvailableAZs, w.AvailabilityZones)
		if len(w.AvailabilityZones) > 0 && len(candidates) == 0 {
			// The watch pinned AZs, none of which offer this type right now.
			return nil
		}
		az := ""
		if len(candidates) > 0 {
			az = candidates[0]
		}
		m := &MatchResult{
			WatchID:          w.WatchID,
			UserID:           w.UserID,
			Region:           c.InstanceType.Region,
			AvailabilityZone: az,
			CandidateAZs:     candidates,
			InstanceType:     c.InstanceType.InstanceType,
			Price:            price,
			IsSpot:           false,
			ActionTaken:      "pending",
		}
		if c.InstanceType.VCPUs > 0 {
			m.VCPUs = c.InstanceType.VCPUs
		}
		return m
	}

	// Spot watch but no pricing data available — no match
	return nil
}

// azAllowed reports whether az passes the watch's AZ preference (empty pref = all
// allowed). An empty az (provider didn't report one) is always allowed.
func azAllowed(az string, pref []string) bool {
	if az == "" || len(pref) == 0 {
		return true
	}
	for _, p := range pref {
		if p == az {
			return true
		}
	}
	return false
}

// orderAZs returns the offered AZs filtered + ordered by the watch's preference.
// With no preference, the offered AZs are returned unchanged (all eligible). With
// a preference, only offered AZs that appear in pref are kept, in pref's order —
// so a user can both pin (narrow) and prioritize zones.
func orderAZs(offered, pref []string) []string {
	if len(pref) == 0 {
		return offered
	}
	offeredSet := make(map[string]bool, len(offered))
	for _, a := range offered {
		offeredSet[a] = true
	}
	var out []string
	for _, p := range pref {
		if offeredSet[p] {
			out = append(out, p)
		}
	}
	return out
}
