package watcher

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	libpricing "github.com/spore-host/libs/pricing"
	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

// #239 phase 5: cost exposure.

// fakePricer prices from a map keyed "type/region", with one origin for all.
// A missing key is an error, as truffle's pricer reports one.
type fakePricer struct {
	mu     sync.Mutex
	rates  map[string]float64
	origin truffleaws.PriceOrigin
	calls  int
}

func (f *fakePricer) OnDemandPriceWithOrigin(_ context.Context, instanceType, region string) (float64, truffleaws.PriceOrigin, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	r, ok := f.rates[instanceType+"/"+region]
	if !ok {
		return 0, truffleaws.PriceOrigin{}, fmt.Errorf("no on-demand price for %s in %s", instanceType, region)
	}
	return r, f.origin, nil
}

func exposureWatch(action ActionMode, pattern string, regions ...string) *Watch {
	return &Watch{WatchID: "w-exp", Action: action, InstanceTypePattern: pattern, Regions: regions}
}

func staticRate(t *testing.T, region, instanceType string) float64 {
	t.Helper()
	r, err := libpricing.GetEC2HourlyRate(region, instanceType)
	if err != nil {
		t.Fatalf("the static list has no %s in %s, so this test proves nothing: %v", instanceType, region, err)
	}
	return r
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func allText(e *Exposure) string { return strings.Join(e.Lines(), "\n") }

// TestStaticExposureIsLabelled: a rate from the static list says it is static,
// why, and when the list was collected, in the text, the column marker and
// the JSON fields (owner rule, truffle#184).
func TestStaticExposureIsLabelled(t *testing.T) {
	rate := staticRate(t, "us-east-1", "c7g.large")
	e := StaticExposure(exposureWatch(ActionSpawn, "c7g.large", "us-east-1"), CLIStaticPriceReason)
	if e.Kind != ExposureSpawn || e.Basis != BasisOnDemand || e.PriceSource != "static" {
		t.Fatalf("exposure = %+v", e)
	}
	if !near(e.HourlyUSD, rate) || !near(e.DailyUSD, rate*24) {
		t.Errorf("hourly/daily = %v/%v, want %v/%v", e.HourlyUSD, e.DailyUSD, rate, rate*24)
	}
	text := allText(e)
	for _, want := range []string{
		"if this lands: up to " + usd(rate) + "/hr (≈" + usd(rate*24) + "/day)",
		"static", "collected " + libpricing.PricesAsOf, CLIStaticPriceReason,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("text lacks %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "live") && !strings.Contains(text, "does not query the live") {
		t.Errorf("a static rate is presented as live:\n%s", text)
	}
	if len(e.StaticNotes) != 1 || !strings.Contains(e.StaticNotes[0], libpricing.PricesAsOf) {
		t.Errorf("StaticNotes = %q", e.StaticNotes)
	}
	if c := e.Column(); c != "≤"+usd(rate)+"/hr*" {
		t.Errorf("Column = %q, want the static marker", c)
	}
}

// TestLiveExposureIsNotLabelledStatic: the poller's live rates say live, and
// carry no static note or marker; a live fallback to the static list (truffle's
// origin) keeps truffle's own why.
func TestLiveExposureIsNotLabelledStatic(t *testing.T) {
	live := &fakePricer{rates: map[string]float64{"p5.48xlarge/us-east-1": 98.32}, origin: truffleaws.LivePriceOrigin()}
	e := ComputeExposure(context.Background(), exposureWatch(ActionSpawn, "p5.48xlarge", "us-east-1"), live)
	if e.PriceSource != "live" || len(e.StaticNotes) != 0 || e.Column() != "≤$98.32/hr" {
		t.Errorf("live: source %q notes %q column %q", e.PriceSource, e.StaticNotes, e.Column())
	}
	if !strings.Contains(allText(e), "price source: live AWS Price List") {
		t.Errorf("live text:\n%s", allText(e))
	}

	fellBack := &fakePricer{rates: live.rates, origin: truffleaws.StaticPriceOrigin("live AWS Price List unavailable: ThrottlingException")}
	e = ComputeExposure(context.Background(), exposureWatch(ActionSpawn, "p5.48xlarge", "us-east-1"), fellBack)
	text := allText(e)
	for _, want := range []string{"static", "collected " + libpricing.PricesAsOf, "ThrottlingException"} {
		if !strings.Contains(text, want) {
			t.Errorf("fallback text lacks %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "price source: live") {
		t.Errorf("a fallback rate is presented as live:\n%s", text)
	}
}

// TestExposureMaxPriceCap: the cap is used when set, labelled as the user's
// cap and not a quote, and no price is looked up.
func TestExposureMaxPriceCap(t *testing.T) {
	w := exposureWatch(ActionSpawn, "p5.48xlarge", "us-east-1")
	w.MaxPrice = 40
	p := &fakePricer{}
	e := ComputeExposure(context.Background(), w, p)
	if p.calls != 0 {
		t.Errorf("looked up %d prices with a cap set", p.calls)
	}
	if e.Basis != BasisMaxPrice || e.HourlyUSD != 40 || e.Column() != "≤$40.00/hr cap" {
		t.Errorf("cap: %+v column %q", e, e.Column())
	}
	if !strings.Contains(allText(e), "your --max-price cap of $40.00/hr per instance, not a price quote") {
		t.Errorf("cap text:\n%s", allText(e))
	}
}

// TestExposurePatternRange: several types or regions give the maximum, with
// the range and the number of combinations it was taken across.
func TestExposurePatternRange(t *testing.T) {
	p := &fakePricer{origin: truffleaws.LivePriceOrigin(), rates: map[string]float64{
		"g6.4xlarge/us-east-1": 1.32, "g6.4xlarge/us-west-2": 1.40,
		"g6.2xlarge/us-east-1": 0.98, "g6.2xlarge/us-west-2": 1.05,
	}}
	e := ComputeExposure(context.Background(), exposureWatch(ActionSpawn, "g6.4xlarge, g6.2xlarge", "us-east-1", "us-west-2"), p)
	if !e.Range || e.PricedPairs != 4 || !near(e.PerInstanceHourlyUSD, 1.40) || !near(e.PerInstanceHourlyMinUSD, 0.98) {
		t.Fatalf("range = %+v", e)
	}
	if !strings.Contains(allText(e), "the highest on-demand rate across 4 type/region combinations ($0.9800–$1.40/hr)") {
		t.Errorf("range text:\n%s", allText(e))
	}
	if e.TypesFromStaticList {
		t.Error("exact types were marked as taken from the static list")
	}
}

// TestExposureWildcardUsesStaticTypes: a wildcard is expanded against the
// static list, the maximum is over the types it has, and the text says that
// matching types the list lacks are not counted.
func TestExposureWildcardUsesStaticTypes(t *testing.T) {
	want, n := 0.0, 0
	for typ, r := range libpricing.EC2Pricing["us-east-1"] {
		if strings.HasPrefix(typ, "c7g.") {
			n++
			want = math.Max(want, r)
		}
	}
	if n < 2 {
		t.Fatalf("the static list has %d c7g types in us-east-1; this test needs at least 2", n)
	}
	e := StaticExposure(exposureWatch(ActionSpawn, "c7g.*", "us-east-1"), CLIStaticPriceReason)
	if !e.TypesFromStaticList || !e.Range || e.PricedPairs != n || !near(e.PerInstanceHourlyUSD, want) {
		t.Fatalf("wildcard = %+v, want max %v over %d types", e, want, n)
	}
	if !strings.Contains(allText(e), "matching types it lacks are not counted") {
		t.Errorf("wildcard text:\n%s", allText(e))
	}

	// No match in the list (it has no g7e): unknown, saying why.
	e = StaticExposure(exposureWatch(ActionSpawn, "g7e.*", "us-east-1"), CLIStaticPriceReason)
	if e.Kind != ExposureUnknown || e.Column() != "unknown" {
		t.Errorf("g7e.* = %+v", e)
	}
	if !strings.Contains(allText(e), "collected "+libpricing.PricesAsOf) {
		t.Errorf("unknown text lacks the static list's date:\n%s", allText(e))
	}

	// A region the static list does not cover cannot be expanded.
	e = StaticExposure(exposureWatch(ActionSpawn, "c7g.*", "us-east-1", "af-south-1"), CLIStaticPriceReason)
	if len(e.Unpriced) != 1 || e.Unpriced[0] != "c7g.* in af-south-1" || !strings.HasSuffix(e.Column(), "*?") {
		t.Errorf("uncovered region: unpriced %q column %q", e.Unpriced, e.Column())
	}
}

// TestExposureFleetTimesN: a --maintain N fleet multiplies by N.
func TestExposureFleetTimesN(t *testing.T) {
	w := exposureWatch(ActionSpawn, "p5.48xlarge", "us-east-1")
	w.DesiredCount = 4
	p := &fakePricer{rates: map[string]float64{"p5.48xlarge/us-east-1": 98.32}, origin: truffleaws.LivePriceOrigin()}
	e := ComputeExposure(context.Background(), w, p)
	if e.Count != 4 || !near(e.HourlyUSD, 4*98.32) || !near(e.DailyUSD, 4*98.32*24) {
		t.Fatalf("fleet = %+v", e)
	}
	if !strings.Contains(e.Summary, "up to $393.28/hr (≈$9438.72/day) for 4 instances at up to $98.32/hr each") {
		t.Errorf("fleet summary = %q", e.Summary)
	}
	if e.Column() != "≤$393.28/hr" {
		t.Errorf("fleet column = %q", e.Column())
	}
	// A capped fleet multiplies the cap.
	w.MaxPrice = 10
	if e := ComputeExposure(context.Background(), w, p); !near(e.HourlyUSD, 40) {
		t.Errorf("capped fleet hourly = %v, want 40", e.HourlyUSD)
	}
}

// TestExposureHold: a hold reserves one instance for HoldDuration, billed
// even if unused.
func TestExposureHold(t *testing.T) {
	if HoldDuration != 30*time.Minute {
		t.Fatalf("HoldDuration = %v; the holder's EndDate and these lines assume 30 minutes", HoldDuration)
	}
	w := exposureWatch(ActionHold, "p5.48xlarge", "us-east-1")
	w.DesiredCount = 3 // ignored: a hold reserves one
	p := &fakePricer{rates: map[string]float64{"p5.48xlarge/us-east-1": 98.32}, origin: truffleaws.LivePriceOrigin()}
	e := ComputeExposure(context.Background(), w, p)
	if e.Kind != ExposureHold || e.Count != 1 || e.HoldMinutes != 30 || !near(e.HoldUSD, 49.16) || e.DailyUSD != 0 {
		t.Fatalf("hold = %+v", e)
	}
	if e.Summary != "if this lands: 30 min reserved at up to $98.32/hr (≈$49.16), billed even if unused" {
		t.Errorf("hold summary = %q", e.Summary)
	}
	if e.Column() != "30m@$98.32/hr" {
		t.Errorf("hold column = %q", e.Column())
	}
	// Unpriced, the hold still says it is billed even if unused.
	e = StaticExposure(w, CLIStaticPriceReason)
	if e.Kind != ExposureUnknown || !strings.Contains(e.Summary, "30 min and billed even if unused") {
		t.Errorf("unpriced hold = %q", e.Summary)
	}
}

// TestExposureNotifyAndSageMaker: notify costs nothing; SageMaker is unknown.
func TestExposureNotifyAndSageMaker(t *testing.T) {
	p := &fakePricer{}
	if e := ComputeExposure(context.Background(), exposureWatch(ActionNotify, "p5.48xlarge", "us-east-1"), p); e.Kind != ExposureNone || e.Column() != "none" {
		t.Errorf("notify = %+v", e)
	}
	w := exposureWatch(ActionSpawn, "ml.p5.48xlarge", "us-east-1")
	w.Service = ServiceSageMaker
	if e := ComputeExposure(context.Background(), w, p); e.Kind != ExposureUnknown || !strings.Contains(e.Summary, "SageMaker") {
		t.Errorf("sagemaker = %+v", e)
	}
	if p.calls != 0 {
		t.Errorf("priced %d pairs for notify/sagemaker", p.calls)
	}
}

// TestExposurePartial: a maximum that misses some combinations says so.
func TestExposurePartial(t *testing.T) {
	p := &fakePricer{rates: map[string]float64{"p5.48xlarge/us-east-1": 98.32}, origin: truffleaws.LivePriceOrigin()}
	e := ComputeExposure(context.Background(), exposureWatch(ActionSpawn, "p5.48xlarge", "us-east-1", "ap-south-2", "eu-north-1", "sa-east-1", "me-south-1"), p)
	if e.Kind != ExposureSpawn || len(e.Unpriced) != 4 || e.Column() != "≤$98.32/hr?" {
		t.Fatalf("partial = %+v column %q", e, e.Column())
	}
	if !strings.Contains(allText(e), "and 1 more; the real maximum may be higher") {
		t.Errorf("partial text:\n%s", allText(e))
	}
}

// TestRemindIfDue_PricesWithThePollersPricer: the sweep prices a reminder
// through the poller's live client when it has one, and the static list,
// labelled, when it does not.
func TestRemindIfDue_PricesWithThePollersPricer(t *testing.T) {
	store := capTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	for _, id := range []string{"w-live", "w-static"} {
		if err := store.PutWatch(ctx, reminderWatch(id, base, 7*day, 2)); err != nil {
			t.Fatalf("PutWatch: %v", err)
		}
	}
	clock := base.Add(7*day + time.Minute)
	var got []*Exposure
	capture := func(_ context.Context, w *Watch, _ ReminderStage, _ time.Time) error {
		got = append(got, w.Exposure)
		return nil
	}

	p := NewPoller(nil, store, false)
	p.truffle = &pricingSearcher{fakePricer: fakePricer{rates: map[string]float64{"p5.48xlarge/us-east-1": 98.32}, origin: truffleaws.LivePriceOrigin()}}
	p.notifyReminder = capture
	w, _ := store.GetWatch(ctx, "w-live")
	p.remindIfDue(ctx, w, clock, &PollSummary{})

	p = NewPoller(nil, store, false) // nil truffle client: no live pricer
	p.notifyReminder = capture
	w, _ = store.GetWatch(ctx, "w-static")
	p.remindIfDue(ctx, w, clock, &PollSummary{})

	if len(got) != 2 || got[0] == nil || got[1] == nil {
		t.Fatalf("exposures = %v", got)
	}
	if got[0].PriceSource != "live" || !near(got[0].HourlyUSD, 98.32) {
		t.Errorf("live poller exposure = %+v", got[0])
	}
	// p5 is not in the static list, so the static poller can only say unknown,
	// and why.
	if got[1].Kind != ExposureUnknown || !strings.Contains(allText(got[1]), "collected "+libpricing.PricesAsOf) {
		t.Errorf("static poller exposure = %+v", got[1])
	}
	// Derived only: nothing about it is stored.
	stored, _ := store.GetWatch(ctx, "w-live")
	if stored.Exposure != nil {
		t.Errorf("exposure was persisted: %+v", stored.Exposure)
	}
}

// pricingSearcher is a capacitySearcher that also prices, as *truffleaws.Client does.
type pricingSearcher struct {
	fakeSearcher
	fakePricer
}

// TestReminderCarriesExposure: the body has the Exposure lines and the
// payload an "exposure" object.
func TestReminderCarriesExposure(t *testing.T) {
	base := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	w := reminderWatch("w-rx", base, 7*day, 2)
	w.Action = ActionHold
	w.InstanceTypePattern = "c7g.large"
	rate := staticRate(t, "us-east-1", "c7g.large")
	r := w.ReminderSchedule()[0]
	now := base.Add(7*day + time.Minute)
	body := reminderBody(w, r, now)
	for _, want := range []string{
		"Exposure:  if this lands: 30 min reserved at up to " + usd(rate) + "/hr",
		"billed even if unused", "collected " + libpricing.PricesAsOf, ReminderStaticPriceReason,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("reminder body lacks %q:\n%s", want, body)
		}
	}
	e, ok := reminderPayload(w, r, now)["exposure"].(*Exposure)
	if !ok || e.Kind != ExposureHold || !near(e.HoldUSD, rate/2) {
		t.Errorf("payload exposure = %#v", e)
	}
}

// TestStaticPricerNeverGuesses: a type the static list lacks is an error, not
// a fabricated rate.
func TestStaticPricerNeverGuesses(t *testing.T) {
	_, origin, err := StaticPricer{Reason: "test"}.OnDemandPriceWithOrigin(context.Background(), "p5.48xlarge", "us-east-1")
	if err == nil || origin.PriceSource != "" {
		t.Errorf("p5.48xlarge static = %v, %+v; want an error and no origin", err, origin)
	}
}
