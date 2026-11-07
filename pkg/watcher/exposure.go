package watcher

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	libpricing "github.com/spore-host/libs/pricing"
	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

// Cost exposure (#239 phase 5): what a watch can cost if it lands, shown at
// create (and in the until-landed confirmation), in reminders, in `list` and in
// `status`.
//
// Where the rate comes from, in order:
//
//  1. The watch's --max-price cap, when set. It is the user's own number, not a
//     quote, and is labelled that way.
//  2. Otherwise the on-demand rate, from an [OnDemandPricer]:
//     - the CLI uses [StaticPricer], the static spore.host price list
//     (libs/pricing), because a live lookup needs pricing:GetProducts and the
//     CLI asks for no such permission;
//     - the poller uses truffle's live AWS Price List client, which its runtime
//     policy already allows (pricing:GetProducts backs spawn's --cost-limit),
//     and which falls back to the same static list, labelled, when the live
//     API fails.
//
// The spot price is not used: no caller has it cheaply at the moment it renders
// exposure. On-demand is the ceiling for spot (spot is billed at or below it),
// so "up to" stays true for a spot watch.
//
// Owner rule (truffle#184): every static rate shown says it is static, why, and
// when the list was collected. The wording is truffle's [truffleaws.PriceOrigin]
// Label, so it can't drift from what truffle and spawn print.

// OnDemandPricer prices one instance type in one region, with the rate's
// provenance. *truffleaws.Client satisfies it.
type OnDemandPricer interface {
	OnDemandPriceWithOrigin(ctx context.Context, instanceType, region string) (float64, truffleaws.PriceOrigin, error)
}

// CLIStaticPriceReason is why the CLI's exposure lines use the static price
// list: it never calls the live Price List.
const CLIStaticPriceReason = "the lagotto CLI does not query the live AWS Price List (that would need pricing:GetProducts, which lagotto does not ask you for)"

// StaticPricer prices from the static spore.host price list only (libs/pricing,
// collected at libpricing.PricesAsOf), labelling every rate static with Reason.
type StaticPricer struct{ Reason string }

// OnDemandPriceWithOrigin returns the static rate, or an error when the list
// has no entry for the exact (type, region) pair. It never guesses.
func (s StaticPricer) OnDemandPriceWithOrigin(_ context.Context, instanceType, region string) (float64, truffleaws.PriceOrigin, error) {
	p, err := libpricing.GetEC2HourlyRate(region, instanceType)
	if err != nil {
		return 0, truffleaws.PriceOrigin{}, err
	}
	return p, truffleaws.StaticPriceOrigin(s.Reason), nil
}

// ExposureKind says what kind of cost a watch can incur.
type ExposureKind string

const (
	// ExposureNone is a notify watch: it launches and reserves nothing.
	ExposureNone ExposureKind = "none"
	// ExposureSpawn is a spawn watch: instances billed while they run.
	ExposureSpawn ExposureKind = "spawn"
	// ExposureHold is a hold watch: a reservation billed for HoldDuration.
	ExposureHold ExposureKind = "hold"
	// ExposureUnknown is a watch lagotto could not price (SageMaker, or no rate
	// for any of its types and regions).
	ExposureUnknown ExposureKind = "unknown"
)

// Price bases.
const (
	BasisMaxPrice = "max_price" // the --max-price cap
	BasisOnDemand = "on_demand" // the on-demand rate
)

// Exposure is a watch's cost if it lands. It is derived, never stored.
type Exposure struct {
	Kind ExposureKind `json:"kind"`
	// Basis is BasisMaxPrice or BasisOnDemand; empty for none/unknown.
	Basis string `json:"basis,omitempty"`
	// Count is how many instances (a fleet's --maintain N) or reservations.
	Count int `json:"count,omitempty"`
	// PerInstanceHourlyUSD is the highest per-instance rate considered, and
	// PerInstanceHourlyMinUSD the lowest; they differ for a pattern or several
	// regions.
	PerInstanceHourlyUSD    float64 `json:"per_instance_hourly_usd,omitempty"`
	PerInstanceHourlyMinUSD float64 `json:"per_instance_hourly_min_usd,omitempty"`
	// HourlyUSD is PerInstanceHourlyUSD × Count; DailyUSD is 24 × that (spawn).
	HourlyUSD float64 `json:"hourly_usd,omitempty"`
	DailyUSD  float64 `json:"daily_usd,omitempty"`
	// HoldMinutes / HoldUSD: a hold's reservation length and what it costs at
	// HourlyUSD, billed even if unused.
	HoldMinutes int     `json:"hold_minutes,omitempty"`
	HoldUSD     float64 `json:"hold_usd,omitempty"`
	// Range is set when the maximum is taken across more than one type/region
	// combination (PricedPairs of them).
	Range       bool `json:"range,omitempty"`
	PricedPairs int  `json:"priced_pairs,omitempty"`
	// TypesFromStaticList: a wildcard's matching types were taken from the
	// static price list, so matching types it lacks are not counted.
	TypesFromStaticList bool `json:"types_from_static_list,omitempty"`
	// Unpriced lists "type in region" combinations with no rate. When it is
	// non-empty alongside a price, the true maximum may be higher.
	Unpriced []string `json:"unpriced,omitempty"`
	// PriceSource is live, static, mixed (some of each), or max_price.
	PriceSource string `json:"price_source,omitempty"`
	// StaticNotes are the truffle labels ("static, collected <date>: <why>") of
	// every static rate used.
	StaticNotes []string `json:"static_notes,omitempty"`
	// Summary is the one-line statement ("if this lands: up to ..."), and
	// Details the lines that qualify it (basis, source, gaps).
	Summary string   `json:"summary"`
	Details []string `json:"details,omitempty"`
}

// maxUnpricedShown bounds how many unpriced combinations the text names.
const maxUnpricedShown = 3

// ComputeExposure works out w's cost exposure, pricing on-demand rates through
// pricer (the poller passes truffle's live client). It makes no AWS call
// itself.
//
// The CLI must not use it: the IAM inventory (internal/iamcoverage) resolves
// the interface call below to every OnDemandPricer, truffle's live client
// included, and would list pricing:GetProducts against list/status/watch. The
// CLI uses [StaticExposure], which makes no interface call.
func ComputeExposure(ctx context.Context, w *Watch, pricer OnDemandPricer) *Exposure {
	in := newExposureInput(w)
	in.quotes = make([]priceQuote, len(in.pairs))
	for i, pr := range in.pairs {
		in.quotes[i].rate, in.quotes[i].origin, in.quotes[i].err = pricer.OnDemandPriceWithOrigin(ctx, pr.instanceType, pr.region)
	}
	return in.build()
}

// StaticExposure is [ComputeExposure] priced from the static price list only,
// every rate labelled static with reason. It makes no AWS call, so it needs no
// permission.
func StaticExposure(w *Watch, reason string) *Exposure {
	in := newExposureInput(w)
	in.staticOnly, in.staticReason = true, reason
	sp := StaticPricer{Reason: reason}
	in.quotes = make([]priceQuote, len(in.pairs))
	for i, pr := range in.pairs {
		in.quotes[i].rate, in.quotes[i].origin, in.quotes[i].err = sp.OnDemandPriceWithOrigin(context.Background(), pr.instanceType, pr.region)
	}
	return in.build()
}

// priceQuote is one pair's rate, or why there is none.
type priceQuote struct {
	rate   float64
	origin truffleaws.PriceOrigin
	err    error
}

// exposureInput is everything an exposure depends on: the watch's fields, the
// (type, region) pairs to price and, once priced, their quotes (same index).
type exposureInput struct {
	service      Service
	action       ActionMode
	pattern      string
	regions      []string
	spot         bool
	maxPrice     float64
	desired      int
	pairs        []pricePair
	fromStatic   bool
	unenumerable []string
	quotes       []priceQuote
	staticOnly   bool
	staticReason string
}

func newExposureInput(w *Watch) *exposureInput {
	in := &exposureInput{service: w.Service, action: w.Action, pattern: w.InstanceTypePattern, regions: w.Regions,
		spot: w.Spot, maxPrice: w.MaxPrice, desired: w.DesiredCount}
	if in.needsPrices() {
		in.pairs, in.fromStatic, in.unenumerable = exposurePairs(in.pattern, in.regions)
	}
	return in
}

// needsPrices reports whether the exposure uses on-demand rates: a spawn or
// hold EC2 watch with no --max-price.
func (in *exposureInput) needsPrices() bool {
	return (in.action == ActionSpawn || in.action == ActionHold) &&
		normalizeService(in.service) == ServiceEC2 && in.maxPrice <= 0
}

func (in *exposureInput) build() *Exposure {
	if in.action != ActionSpawn && in.action != ActionHold {
		return &Exposure{Kind: ExposureNone, Summary: "notify only: launches and reserves nothing"}
	}
	if normalizeService(in.service) == ServiceSageMaker {
		return &Exposure{Kind: ExposureUnknown,
			Summary: "if this lands: cost unknown: a SageMaker job is billed by SageMaker, and lagotto does not price it"}
	}
	kind := ExposureSpawn
	count := 1
	if in.action == ActionHold {
		kind = ExposureHold
	} else if in.desired > 0 {
		count = in.desired
	}
	e := &Exposure{Kind: kind, Count: count}

	if in.maxPrice > 0 {
		e.Basis = BasisMaxPrice
		e.PriceSource = BasisMaxPrice
		e.PerInstanceHourlyUSD = in.maxPrice
		e.PerInstanceHourlyMinUSD = in.maxPrice
		e.finish()
		detail := fmt.Sprintf("price basis: your --max-price cap of %s/hr per instance, not a price quote; the actual rate can be lower", usd(in.maxPrice))
		if kind == ExposureHold {
			detail = fmt.Sprintf("price basis: your --max-price cap of %s/hr, not a price quote; the reservation bills the on-demand rate, which can be lower", usd(in.maxPrice))
		}
		e.Details = append(e.Details, detail)
		return e
	}

	e.Basis = BasisOnDemand
	e.TypesFromStaticList = in.fromStatic
	e.Unpriced = append(e.Unpriced, in.unenumerable...)
	var origins []truffleaws.PriceOrigin
	live, static := false, false
	for i, pr := range in.pairs {
		q := in.quotes[i]
		if q.err != nil || q.rate <= 0 {
			e.Unpriced = append(e.Unpriced, pr.instanceType+" in "+pr.region)
			continue
		}
		if e.PricedPairs == 0 || q.rate > e.PerInstanceHourlyUSD {
			e.PerInstanceHourlyUSD = q.rate
		}
		if e.PricedPairs == 0 || q.rate < e.PerInstanceHourlyMinUSD {
			e.PerInstanceHourlyMinUSD = q.rate
		}
		e.PricedPairs++
		switch {
		case q.origin.IsStatic():
			static = true
			origins = append(origins, q.origin)
		case q.origin.PriceSource == truffleaws.PriceSourceLive:
			live = true
		}
	}
	sort.Strings(e.Unpriced)
	e.StaticNotes = truffleaws.StaticPriceNotes(origins)
	switch {
	case live && static:
		e.PriceSource = "mixed"
	case static:
		e.PriceSource = string(truffleaws.PriceSourceStatic)
	case live:
		e.PriceSource = string(truffleaws.PriceSourceLive)
	}

	if e.PricedPairs == 0 {
		e.Kind = ExposureUnknown
		e.Basis = ""
		what := "a capacity reservation"
		if kind == ExposureSpawn {
			what = "the instance"
		}
		e.Summary = fmt.Sprintf("if this lands: cost unknown: no on-demand price for %s in %s, so lagotto cannot say what %s would cost",
			in.pattern, strings.Join(in.regions, ", "), what)
		if kind == ExposureHold {
			e.Summary += fmt.Sprintf(" (it is reserved for %s and billed even if unused)", holdSpan())
		}
		e.Details = append(e.Details, in.unpricedDetail())
		return e
	}

	e.Range = e.PricedPairs > 1
	e.finish()
	e.Details = append(e.Details, e.basisDetail(in.pattern))
	if in.spot && kind == ExposureSpawn {
		e.Details = append(e.Details, "spot: billed at the spot price, which is at or below the on-demand rate")
	}
	if live {
		e.Details = append(e.Details, "price source: live AWS Price List")
	}
	for _, n := range e.StaticNotes {
		e.Details = append(e.Details, "price source: "+n)
	}
	if len(e.Unpriced) > 0 {
		e.Details = append(e.Details, fmt.Sprintf("not priced: %s; the real maximum may be higher", namedList(e.Unpriced)))
	}
	return e
}

// unpricedDetail explains why nothing could be priced.
func (in *exposureInput) unpricedDetail() string {
	if in.staticOnly {
		return fmt.Sprintf("price source: the static price list (collected %s) has no entry for %s there, and %s. "+
			"The list has no GPU or accelerator family newer than p4d", libpricing.PricesAsOf, in.pattern, in.staticReason)
	}
	if in.fromStatic && len(in.pairs) == 0 {
		return fmt.Sprintf("price source: no type matching %s is in the static price list (collected %s), which lagotto uses to expand a wildcard",
			in.pattern, libpricing.PricesAsOf)
	}
	return "price source: neither the live AWS Price List nor its static fallback had a rate"
}

// finish fills the totals and the summary line from the per-instance rate.
func (e *Exposure) finish() {
	e.HourlyUSD = e.PerInstanceHourlyUSD * float64(e.Count)
	if e.Kind == ExposureHold {
		e.HoldMinutes = int(HoldDuration / time.Minute)
		e.HoldUSD = e.HourlyUSD * HoldDuration.Hours()
		e.Summary = fmt.Sprintf("if this lands: %s reserved at up to %s/hr (≈%s), billed even if unused",
			holdSpan(), usd(e.HourlyUSD), usd(e.HoldUSD))
		return
	}
	e.DailyUSD = e.HourlyUSD * 24
	e.Summary = fmt.Sprintf("if this lands: up to %s/hr (≈%s/day)", usd(e.HourlyUSD), usd(e.DailyUSD))
	if e.Count > 1 {
		e.Summary += fmt.Sprintf(" for %d instances at up to %s/hr each", e.Count, usd(e.PerInstanceHourlyUSD))
	}
}

// basisDetail says which on-demand rate the figure is.
func (e *Exposure) basisDetail(pattern string) string {
	s := "price basis: on-demand rate"
	if e.Range {
		s = fmt.Sprintf("price basis: the highest on-demand rate across %d type/region combinations (%s–%s/hr)",
			e.PricedPairs, usd(e.PerInstanceHourlyMinUSD), usd(e.PerInstanceHourlyUSD))
	}
	if e.TypesFromStaticList {
		s += fmt.Sprintf("; the types matching %s are the ones in the static price list (collected %s), so matching types it lacks are not counted",
			pattern, libpricing.PricesAsOf)
	}
	return s
}

// Column is the compact `list` EXPOSURE cell: "≤$98.32/hr", "30m@$98.32/hr",
// "≤$5.00/hr cap", "none" or "unknown". A trailing "*" marks a static rate
// (explained under the table) and "?" a maximum with unpriced combinations.
func (e *Exposure) Column() string {
	switch e.Kind {
	case ExposureNone:
		return "none"
	case ExposureUnknown:
		return "unknown"
	}
	var s string
	if e.Kind == ExposureHold {
		s = fmt.Sprintf("%dm@%s/hr", e.HoldMinutes, usd(e.HourlyUSD))
	} else {
		s = "≤" + usd(e.HourlyUSD) + "/hr"
	}
	if e.Basis == BasisMaxPrice {
		return s + " cap"
	}
	if len(e.StaticNotes) > 0 {
		s += "*"
	}
	if len(e.Unpriced) > 0 {
		s += "?"
	}
	return s
}

// Lines returns the summary followed by its details.
func (e *Exposure) Lines() []string {
	return append([]string{e.Summary}, e.Details...)
}

// pricePair is one (instance type, region) to price.
type pricePair struct{ instanceType, region string }

// exposurePairs expands a watch pattern into the (type, region) pairs to
// price. An exact type is priced in each region. A wildcard can only be
// expanded against a list of types, and the one lagotto has without an AWS
// call is the static price list, so fromStatic reports that it was used; a
// region the list does not cover cannot be expanded and is returned in
// unenumerable.
func exposurePairs(pattern string, regions []string) (pairs []pricePair, fromStatic bool, unenumerable []string) {
	seen := map[pricePair]bool{}
	add := func(p pricePair) {
		if !seen[p] {
			seen[p] = true
			pairs = append(pairs, p)
		}
	}
	for _, sub := range SplitInstanceTypePatterns(pattern) {
		if isExactInstanceType(sub) {
			for _, r := range regions {
				add(pricePair{sub, r})
			}
			continue
		}
		fromStatic = true
		re, err := regexp.Compile(wildcardToRegex(sub))
		if err != nil {
			unenumerable = append(unenumerable, sub+" (not a valid pattern)")
			continue
		}
		for _, r := range regions {
			table, ok := libpricing.EC2Pricing[r]
			if !ok {
				unenumerable = append(unenumerable, sub+" in "+r)
				continue
			}
			types := make([]string, 0, len(table))
			for t := range table {
				if re.MatchString(t) {
					types = append(types, t)
				}
			}
			sort.Strings(types)
			for _, t := range types {
				add(pricePair{t, r})
			}
		}
	}
	return pairs, fromStatic, unenumerable
}

// isExactInstanceType reports whether a sub-pattern names one type: no
// wildcard and nothing wildcardToRegex would treat as a regex.
func isExactInstanceType(sub string) bool {
	return !strings.ContainsAny(sub, "*?^$()[]{}+\\|")
}

// namedList renders up to maxUnpricedShown items and a count of the rest.
func namedList(items []string) string {
	if len(items) <= maxUnpricedShown {
		return strings.Join(items, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(items[:maxUnpricedShown], ", "), len(items)-maxUnpricedShown)
}

// holdSpan renders HoldDuration, e.g. "30 min".
func holdSpan() string {
	return fmt.Sprintf("%d min", int(HoldDuration/time.Minute))
}

// usd formats a dollar amount: four decimals under $1, two otherwise.
func usd(v float64) string {
	if v < 1 {
		return fmt.Sprintf("$%.4f", v)
	}
	return fmt.Sprintf("$%.2f", v)
}
