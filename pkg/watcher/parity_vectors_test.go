package watcher

// Parity vectors for lagotto-ts (#289): see internal/parity. Each Test below
// writes (with -update-parity) or checks one testdata/parity/watcher-*.json.
// Inputs are fixed, times included, so the output is a pure function of the
// code. Durations are in seconds, times RFC 3339 UTC.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	truffleaws "github.com/spore-host/truffle/pkg/aws"

	"github.com/spore-host/lagotto/internal/parity"
)

// parityNow is the fixed "now" every time-dependent vector is computed at.
var parityNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func seconds(d time.Duration) float64 { return d.Seconds() }

// TestParityVectorsPatterns: instance-type patterns, including the RE2-vs-JS
// cases of lagotto-ts#35. Matching is Go's: regexp.Compile(patternToRegex(p))
// then MatchString on each candidate (the poller's searchBestMatch).
func TestParityVectorsPatterns(t *testing.T) {
	split := parity.Vectors{Go: "watcher.splitInstanceTypePatterns"}
	for _, in := range []string{"p5.48xlarge", "g6.4xlarge,g6.2xlarge", " a , ,b ", "", " , ", "p5.*,"} {
		out := splitInstanceTypePatterns(in)
		if out == nil {
			out = []string{}
		}
		split.Cases = append(split.Cases, parity.Case{Name: strconv.Quote(in), Input: in, Output: out})
	}

	inputs := []string{
		"p5.*", "g5.xlarge", "c7g.*large", "^g5\\..*$", "a+b", "p?.48xlarge", "ml.g5.*",
		"", " ", "g6.4xlarge,g6.2xlarge", "p5.*, ^g5\\.x", "trn1.*|p5.*",
	}
	wild := parity.Vectors{Go: "watcher.wildcardToRegex", Note: "a sub-pattern containing any of ^ $ ( ) [ ] { } + \\ is a raw regex, used as-is; otherwise . is escaped, * becomes .*, and the result is anchored. ? and | are NOT regex markers"}
	toRe := parity.Vectors{Go: "watcher.patternToRegex"}
	for _, in := range inputs {
		wild.Cases = append(wild.Cases, parity.Case{Name: strconv.Quote(in), Input: in, Output: wildcardToRegex(in)})
		toRe.Cases = append(toRe.Cases, parity.Case{Name: strconv.Quote(in), Input: in, Output: patternToRegex(in)})
	}

	candidates := []string{
		"p5.48xlarge", "p5e.48xlarge", "P5.48XLARGE", ".48xlarge", "p.48xlarge", "g5.xlarge", "g5.2xlarge",
		"xg5.xlarge", "g6.4xlarge", "g6.2xlarge", "trn1.32xlarge", "ml.g5.2xlarge", "a", "aa", "c7g.large", "c7g.4xlarge",
	}
	match := parity.Vectors{
		Go:   "watcher.patternToRegex + regexp.Compile + MatchString",
		Note: "the error is Go regexp's text for a pattern RE2 refuses; the watch then never matches. Cases marked re2 in their name are where JS RegExp differs (lagotto-ts#35)",
	}
	for _, c := range []struct{ name, pattern string }{
		{"glob", "p5.*"},
		{"exact", "g5.xlarge"},
		{"comma list", "g6.4xlarge,g6.2xlarge"},
		{"glob ? is a regex quantifier", "p?.48xlarge"},
		{"raw regex is unanchored", "\\d"},
		{"raw regex alternation", "^(trn1|p5)\\."},
		{"re2 inline case-insensitive flag", "(?i)^P5\\..*$"},
		{"re2 refuses lookahead", "^g5\\.(?=x)"},
		{"re2 refuses backreference", "^(a)\\1$"},
		{"re2 POSIX class", "^[[:alpha:]]+$"},
		{"re2 unicode class", "^\\pL+$"},
		{"re2 \\z end of text", "^aa\\z"},
		{"counted repetition", "^a{2}$"},
		{"invalid regex", "^p5\\.(.*$"},
	} {
		re := patternToRegex(c.pattern)
		out := map[string]any{"regex": re}
		cr, err := regexp.Compile(re)
		if err == nil {
			matched := []string{}
			for _, cand := range candidates {
				if cr.MatchString(cand) {
					matched = append(matched, cand)
				}
			}
			out["matches"] = matched
		}
		match.Cases = append(match.Cases, parity.Case{
			Name:   c.name,
			Input:  map[string]any{"pattern": c.pattern, "candidates": candidates},
			Output: out,
			Error:  parity.ErrText(err),
		})
	}

	svc := parity.Vectors{Go: "watcher.ValidateWatchPattern"}
	for _, c := range []struct {
		service Service
		pattern string
	}{
		{"", "p5.48xlarge"}, {"ec2", "p5.*"}, {"ec2", "ml.g5.2xlarge"}, {"sagemaker", "ml.g5.2xlarge"},
		{"sagemaker", "g5.2xlarge"}, {"sagemaker", "ml.g5.*,ml.p4d.24xlarge"}, {"sagemaker", "ml.g5.*,p4d.24xlarge"},
		{"ec2", ""}, {"ec2", " , "},
	} {
		err := ValidateWatchPattern(c.service, c.pattern)
		svc.Cases = append(svc.Cases, parity.Case{Name: string(c.service) + " " + strconv.Quote(c.pattern),
			Input: map[string]any{"service": c.service, "pattern": c.pattern}, Error: parity.ErrText(err)})
	}

	parity.Check(t, "watcher-patterns.json", parity.File{Area: "instance-type patterns", Vectors: map[string]parity.Vectors{
		"splitInstanceTypePatterns": split, "wildcardToRegex": wild, "patternToRegex": toRe,
		"compileAndMatch": match, "validateWatchPattern": svc,
	}})
}

// TestParityVectorsDurations: lagotto's d/w duration parser and Go's own
// time.ParseDuration, which every --ttl/--remind/--after tries first.
func TestParityVectorsDurations(t *testing.T) {
	inputs := []string{
		"30m", "1h", "90m", "1.5h", "1d", "1.5d", "2d12h", "1w", "2w3d", "36h", "0", "0s", "-1h", "5abcd", "d", "",
		"1x", "10", "1h30m", "300ms", "1.5.5h", "100000w", ".5d", "1e3s",
	}
	lag := parity.Vectors{Go: "watcher.ParseDuration", Note: "output is seconds"}
	goParse := parity.Vectors{Go: "time.ParseDuration", Note: "output is seconds; Go's own parser, which lagotto-ts has as parseGoDuration"}
	for _, in := range inputs {
		var out any
		d, err := ParseDuration(in)
		if err == nil {
			out = seconds(d)
		}
		lag.Cases = append(lag.Cases, parity.Case{Name: strconv.Quote(in), Input: in, Output: out, Error: parity.ErrText(err)})
		out = nil
		d, err = time.ParseDuration(in)
		if err == nil {
			out = seconds(d)
		}
		goParse.Cases = append(goParse.Cases, parity.Case{Name: strconv.Quote(in), Input: in, Output: out, Error: parity.ErrText(err)})
	}
	span := parity.Vectors{Go: "watcher.FormatSpan", Note: "input is seconds"}
	for _, d := range []time.Duration{0, 90 * time.Minute, 36 * time.Hour, 24 * time.Hour, 21 * 24 * time.Hour, 30 * 24 * time.Hour, 45 * time.Second} {
		span.Cases = append(span.Cases, parity.Case{Name: d.String(), Input: seconds(d), Output: FormatSpan(d)})
	}
	parity.Check(t, "watcher-durations.json", parity.File{Area: "durations", Vectors: map[string]parity.Vectors{
		"parseDuration": lag, "goParseDuration": goParse, "formatSpan": span,
	}})
}

// TestParityVectorsLifecycle: a new watch's timestamps, until-landed rules,
// the acknowledgement deadline, and the notify deprecation.
func TestParityVectorsLifecycle(t *testing.T) {
	newW := parity.Vectors{
		Go:   "watcher.NewWatch",
		Note: "input durations in seconds; now is fixed. watch_id is random and omitted. ttl_timestamp is RetentionTTL(expires_at), never expires_at itself (#266)",
	}
	for _, c := range []struct {
		name string
		life Lifetime
	}{
		{"ttl 24h", Lifetime{TTL: 24 * time.Hour}},
		{"ttl 30m", Lifetime{TTL: 30 * time.Minute}},
		{"ttl zero", Lifetime{}},
		{"ttl negative", Lifetime{TTL: -time.Hour}},
		{"until-landed default", Lifetime{UntilLanded: true, RemindEvery: DefaultRemindEvery, RemindPauseAfter: DefaultRemindPauseAfter}},
		{"until-landed 1d pause 0", Lifetime{UntilLanded: true, RemindEvery: 24 * time.Hour}},
		{"until-landed with a ttl", Lifetime{UntilLanded: true, TTL: time.Hour, RemindEvery: DefaultRemindEvery}},
		{"until-landed remind too short", Lifetime{UntilLanded: true, RemindEvery: time.Minute}},
		{"until-landed remind too long", Lifetime{UntilLanded: true, RemindEvery: 31 * 24 * time.Hour}},
		{"until-landed negative pause", Lifetime{UntilLanded: true, RemindEvery: DefaultRemindEvery, RemindPauseAfter: -1}},
	} {
		in := map[string]any{"now": parityNow, "ttl": seconds(c.life.TTL), "until_landed": c.life.UntilLanded,
			"remind_every": seconds(c.life.RemindEvery), "remind_pause_after": c.life.RemindPauseAfter}
		w, err := NewWatch(parityNow, c.life)
		var out any
		if err == nil {
			out = map[string]any{
				"status": w.Status, "created_at": w.CreatedAt, "updated_at": w.UpdatedAt, "expires_at": w.ExpiresAt,
				"ttl_timestamp": w.TTLTimestamp, "until_landed": w.UntilLanded,
				"remind_every_seconds": w.RemindEverySeconds, "remind_pause_after": w.RemindPauseAfter,
			}
		}
		newW.Cases = append(newW.Cases, parity.Case{Name: c.name, Input: in, Output: out, Error: parity.ErrText(err)})
	}

	ret := parity.Vectors{Go: "watcher.RetentionTTL", Note: "output is a Unix time in seconds"}
	for _, e := range []time.Time{parityNow, parityNow.Add(24 * time.Hour), {}} {
		ret.Cases = append(ret.Cases, parity.Case{Name: e.Format(time.RFC3339), Input: e, Output: RetentionTTL(e)})
	}

	email := []NotifyChannel{{Type: "email", Target: "a@example.com"}}
	ul := parity.Vectors{Go: "watcher.ValidateUntilLanded", Note: "remind_every in seconds"}
	for _, c := range []struct {
		name     string
		action   ActionMode
		desired  int
		channels []NotifyChannel
		remind   time.Duration
		pause    int
	}{
		{"spawn ok", ActionSpawn, 0, email, DefaultRemindEvery, DefaultRemindPauseAfter},
		{"hold ok", ActionHold, 0, email, DefaultRemindEvery, 0},
		{"notify refused", ActionNotify, 0, email, DefaultRemindEvery, 0},
		{"fleet refused", ActionSpawn, 3, email, DefaultRemindEvery, 0},
		{"no channel", ActionSpawn, 0, nil, DefaultRemindEvery, 0},
		{"remind too short", ActionSpawn, 0, email, time.Minute, 0},
		{"negative pause", ActionSpawn, 0, email, DefaultRemindEvery, -1},
	} {
		err := ValidateUntilLanded(c.action, c.desired, c.channels, c.remind, c.pause)
		ul.Cases = append(ul.Cases, parity.Case{Name: c.name, Input: map[string]any{
			"action": c.action, "desired_count": c.desired, "notify_channels": c.channels,
			"remind_every": seconds(c.remind), "remind_pause_after": c.pause,
		}, Error: parity.ErrText(err)})
	}

	remind := parity.Vectors{Go: "watcher.ValidateRemindEvery", Note: "input in seconds"}
	for _, d := range []time.Duration{MinRemindEvery - time.Second, MinRemindEvery, DefaultRemindEvery, MaxRemindEvery, MaxRemindEvery + time.Second, 0} {
		remind.Cases = append(remind.Cases, parity.Case{Name: d.String(), Input: seconds(d), Error: parity.ErrText(ValidateRemindEvery(d))})
	}

	ack := parity.Vectors{Go: "Watch.AckWindow, Watch.AckDeadline, Watch.AckWindowStart, Watch.Paused", Note: "ack_window in seconds; deadline null when there is nothing to acknowledge; now is fixed"}
	for _, c := range []struct {
		name string
		w    Watch
	}{
		{"ttl watch", Watch{Status: StatusActive, ExpiresAt: parityNow.Add(time.Hour)}},
		{"until-landed 7d pause 2", Watch{Status: StatusActive, UntilLanded: true, RemindEverySeconds: 7 * 86400, RemindPauseAfter: 2, ExpiresAt: parityNow.Add(21 * 24 * time.Hour)}},
		{"until-landed 1d pause 0", Watch{Status: StatusActive, UntilLanded: true, RemindEverySeconds: 86400, ExpiresAt: parityNow.Add(24 * time.Hour)}},
		{"until-landed no interval", Watch{Status: StatusActive, UntilLanded: true}},
		{"paused", Watch{Status: StatusExpired, ExpiredReason: ExpiredReasonUnacknowledged, UntilLanded: true, RemindEverySeconds: 86400, ExpiresAt: parityNow}},
		{"expired by ttl", Watch{Status: StatusExpired, ExpiresAt: parityNow}},
	} {
		w := c.w
		var deadline any
		if d, ok := w.AckDeadline(parityNow); ok {
			deadline = d
		}
		var start any
		if s := w.AckWindowStart(); !s.IsZero() {
			start = s
		}
		ack.Cases = append(ack.Cases, parity.Case{Name: c.name, Input: map[string]any{"now": parityNow, "watch": w},
			Output: map[string]any{"ack_window": seconds(w.AckWindow()), "ack_deadline": deadline, "ack_window_start": start, "paused": w.Paused()}})
	}

	dep := parity.Vectors{Go: "watcher.ActionDeprecations"}
	for _, a := range []ActionMode{ActionNotify, ActionSpawn, ActionHold} {
		for _, given := range []bool{true, false} {
			out := ActionDeprecations(a, given)
			if out == nil {
				out = []Deprecation{}
			}
			dep.Cases = append(dep.Cases, parity.Case{Name: fmt.Sprintf("%s given=%v", a, given),
				Input: map[string]any{"action": a, "action_given": given}, Output: out})
		}
	}

	act := parity.Vectors{Go: "watcher.ValidateServiceAction"}
	for _, s := range []Service{"", ServiceEC2, ServiceSageMaker} {
		for _, a := range []ActionMode{ActionNotify, ActionSpawn, ActionHold} {
			act.Cases = append(act.Cases, parity.Case{Name: fmt.Sprintf("%s %s", s, a), Input: map[string]any{"service": s, "action": a},
				Error: parity.ErrText(ValidateServiceAction(s, a))})
		}
	}

	parity.Check(t, "watcher-lifecycle.json", parity.File{Area: "watch lifetime, until-landed, acknowledgement, deprecation", Vectors: map[string]parity.Vectors{
		"newWatch": newW, "retentionTTL": ret, "validateUntilLanded": ul, "validateRemindEvery": remind,
		"ackDeadline": ack, "actionDeprecations": dep, "validateServiceAction": act,
	}})
}

// TestParityVectorsAZs: the AZ preference filter and order.
func TestParityVectorsAZs(t *testing.T) {
	allowed := parity.Vectors{Go: "watcher.azAllowed"}
	order := parity.Vectors{Go: "watcher.orderAZs"}
	offered := []string{"us-east-1a", "us-east-1b", "us-east-1c"}
	for _, c := range []struct {
		name string
		az   string
		pref []string
	}{
		{"no preference", "us-east-1a", nil},
		{"in preference", "us-east-1b", []string{"us-east-1b"}},
		{"not in preference", "us-east-1a", []string{"us-east-1b"}},
		{"empty az", "", []string{"us-east-1b"}},
		{"prioritized", "us-east-1c", []string{"us-east-1c", "us-east-1a", "us-east-1z"}},
	} {
		allowed.Cases = append(allowed.Cases, parity.Case{Name: c.name, Input: map[string]any{"az": c.az, "pref": c.pref}, Output: azAllowed(c.az, c.pref)})
		out := orderAZs(offered, c.pref)
		if out == nil {
			out = []string{}
		}
		order.Cases = append(order.Cases, parity.Case{Name: c.name, Input: map[string]any{"offered": offered, "pref": c.pref}, Output: out})
	}
	parity.Check(t, "watcher-azs.json", parity.File{Area: "availability-zone preference", Vectors: map[string]parity.Vectors{
		"azAllowed": allowed, "orderAZs": order,
	}})
}

// parityPricer prices from a fixed table; a missing entry is unpriced. live
// marks a rate as a live quote, otherwise it is static.
type parityPricer struct {
	rates map[string]float64
	live  bool
}

func (p parityPricer) OnDemandPriceWithOrigin(_ context.Context, instanceType, region string) (float64, truffleaws.PriceOrigin, error) {
	r, ok := p.rates[instanceType+" "+region]
	if !ok {
		return 0, truffleaws.PriceOrigin{}, fmt.Errorf("no on-demand price for %s in %s", instanceType, region)
	}
	if p.live {
		return r, truffleaws.LivePriceOrigin(), nil
	}
	return r, truffleaws.PriceOrigin{PriceSource: truffleaws.PriceSourceStatic, PriceAsOf: "2026-01-01", PriceFallbackReason: "parity vector"}, nil
}

// TestParityVectorsExposure: a watch's cost exposure, priced from a fixed
// table so the vectors do not depend on live or static price data. Only exact
// types: a wildcard expands against the static price list, which a port reads
// from its own copy of that list.
func TestParityVectorsExposure(t *testing.T) {
	rates := map[string]float64{
		"p5.48xlarge us-east-1": 98.32, "p5.48xlarge us-west-2": 98.32, "g6.4xlarge us-east-1": 1.3232, "g6.2xlarge us-east-1": 0.9776,
	}
	v := parity.Vectors{
		Go:   "watcher.ComputeExposure, Exposure.Column, Exposure.Lines",
		Note: "input.rates maps \"<type> <region>\" to an hourly USD rate; a missing pair has no price. input.live: rates are live quotes, otherwise static (as of 2026-01-01, reason \"parity vector\")",
	}
	for _, c := range []struct {
		name string
		w    Watch
		live bool
	}{
		{"notify", Watch{Action: ActionNotify, InstanceTypePattern: "p5.48xlarge", Regions: []string{"us-east-1"}}, true},
		{"spawn one", Watch{Action: ActionSpawn, InstanceTypePattern: "p5.48xlarge", Regions: []string{"us-east-1"}}, true},
		{"spawn two regions", Watch{Action: ActionSpawn, InstanceTypePattern: "p5.48xlarge", Regions: []string{"us-east-1", "us-west-2"}}, true},
		{"spawn fleet of 3", Watch{Action: ActionSpawn, InstanceTypePattern: "g6.4xlarge", Regions: []string{"us-east-1"}, DesiredCount: 3}, true},
		{"spawn range", Watch{Action: ActionSpawn, InstanceTypePattern: "g6.4xlarge,g6.2xlarge", Regions: []string{"us-east-1"}}, true},
		{"spawn static rate", Watch{Action: ActionSpawn, InstanceTypePattern: "g6.2xlarge", Regions: []string{"us-east-1"}}, false},
		{"spawn partly unpriced", Watch{Action: ActionSpawn, InstanceTypePattern: "p5.48xlarge", Regions: []string{"us-east-1", "eu-west-1"}}, true},
		{"spawn unpriced", Watch{Action: ActionSpawn, InstanceTypePattern: "x9.huge", Regions: []string{"us-east-1"}}, true},
		{"spawn max-price cap", Watch{Action: ActionSpawn, InstanceTypePattern: "p5.48xlarge", Regions: []string{"us-east-1"}, MaxPrice: 5}, true},
		{"hold", Watch{Action: ActionHold, InstanceTypePattern: "p5.48xlarge", Regions: []string{"us-east-1"}}, true},
		{"sagemaker", Watch{Service: ServiceSageMaker, Action: ActionSpawn, InstanceTypePattern: "ml.g5.2xlarge", Regions: []string{"us-east-1"}}, true},
	} {
		w := c.w
		e := ComputeExposure(context.Background(), &w, parityPricer{rates: rates, live: c.live})
		v.Cases = append(v.Cases, parity.Case{Name: c.name,
			Input:  map[string]any{"watch": w, "rates": rates, "live": c.live},
			Output: map[string]any{"exposure": e, "column": e.Column(), "lines": e.Lines()}})
	}
	parity.Check(t, "watcher-exposure.json", parity.File{Area: "cost exposure", Vectors: map[string]parity.Vectors{"computeExposure": v}})
}

// TestParityVectorsRecords: the DynamoDB items lagotto writes for a watch, a
// match-history row, a group and a scheduled launch, as DynamoDB JSON (the
// wire form: {"S": ...}, {"N": ...}). These are the shapes lagotto-ts must
// write for the Go poller to service, and read back. Key helpers ride along.
func TestParityVectorsRecords(t *testing.T) {
	const owner = "AROAEXAMPLEID:alice"
	created := parityNow
	match := &MatchResult{
		WatchID: OwnerKey(owner, "w-1a2b3c4d"), UserID: "arn:aws:sts::123456789012:assumed-role/dev/alice",
		Region: "us-east-1", AvailabilityZone: "us-east-1b", InstanceType: "p5.48xlarge", Price: 98.32,
		MatchedAt: created.Add(2 * time.Hour), ActionTaken: "spawned", InstanceID: "i-0123456789abcdef0",
	}
	launchConfig, _ := json.Marshal(map[string]any{"instance_type": "p5.48xlarge", "iam_role": "research-role", "ttl": "8h"})
	watches := []struct {
		name string
		w    Watch
	}{
		{"hold watch, ttl", Watch{
			WatchID: OwnerKey(owner, "w-1a2b3c4d"), UserID: "arn:aws:sts::123456789012:assumed-role/dev/alice", OwnerID: owner,
			Status: StatusActive, InstanceTypePattern: "p5.48xlarge", Regions: []string{"us-east-1", "us-west-2"},
			Action: ActionHold, CreatedAt: created, UpdatedAt: created, ExpiresAt: created.Add(24 * time.Hour),
			TTLTimestamp: RetentionTTL(created.Add(24 * time.Hour)),
		}},
		{"spawn watch, until-landed, matched", Watch{
			WatchID: OwnerKey(owner, "w-5e6f7a8b"), UserID: "arn:aws:sts::123456789012:assumed-role/dev/alice", OwnerID: owner,
			Status: StatusMatched, Service: ServiceEC2, InstanceTypePattern: "p5.*,p4d.24xlarge", Regions: []string{"us-east-1"},
			AvailabilityZones: []string{"us-east-1b", "us-east-1a"}, Spot: false, Action: ActionSpawn,
			NotifyChannels: []NotifyChannel{{Type: "email", Target: "alice@example.com"}}, LaunchConfigJSON: launchConfig,
			UntilLanded: true, RemindEverySeconds: int64(DefaultRemindEvery / time.Second), RemindPauseAfter: DefaultRemindPauseAfter,
			CreatedAt: created, UpdatedAt: created.Add(2 * time.Hour), ExpiresAt: created.Add(21 * 24 * time.Hour),
			TTLTimestamp: RetentionTTL(created.Add(21 * 24 * time.Hour)), MatchCount: 1, LastMatch: match, Group: "h100",
		}},
		{"sagemaker watch", Watch{
			WatchID: OwnerKey(owner, "w-9c0d1e2f"), UserID: "arn:aws:iam::123456789012:user/bob", Status: StatusActive,
			Service: ServiceSageMaker, InstanceTypePattern: "ml.g5.2xlarge", Regions: []string{"us-west-2"}, Action: ActionSpawn,
			SageMakerJobJSON: []byte(`{"RoleArn":"arn:aws:iam::123456789012:role/sm-exec"}`), MaxPrice: 2.5,
			CreatedAt: created, UpdatedAt: created, ExpiresAt: created.Add(6 * time.Hour), TTLTimestamp: RetentionTTL(created.Add(6 * time.Hour)),
		}},
	}
	wv := parity.Vectors{Go: "attributevalue.MarshalMap(Watch) (Store.PutWatch)", Note: "input is the watch as lagotto's JSON (-o json); item is the DynamoDB item. []byte fields are B (base64)"}
	for _, c := range watches {
		w := c.w
		wv.Cases = append(wv.Cases, parity.Case{Name: c.name, Input: w, Output: map[string]any{"item": ddbItem(t, &w)}})
	}
	hv := parity.Vectors{Go: "attributevalue.MarshalMap(MatchResult) (Store.RecordHistory)", Cases: []parity.Case{
		{Name: "spawned", Input: match, Output: map[string]any{"item": ddbItem(t, match)}},
	}}
	g := &WatchGroup{
		Key: GroupKey(ScopedName(OwnerKey(owner, "w-5e6f7a8b"), "h100")), Name: "h100", Owner: owner, State: GroupHunting,
		Members: []string{OwnerKey(owner, "w-5e6f7a8b"), OwnerKey(owner, "w-1a2b3c4d")}, CreatedAt: created, UpdatedAt: created,
		TTLTimestamp: RetentionTTL(created.Add(21 * 24 * time.Hour)),
	}
	gv := parity.Vectors{Go: "attributevalue.MarshalMap(WatchGroup)", Note: "the group item lives in the watches table; group_members is a string set (SS)", Cases: []parity.Case{
		{Name: "hunting group", Input: map[string]any{"key": g.Key, "group": g}, Output: map[string]any{"item": ddbItem(t, g)}},
	}}
	keys := parity.Vectors{Go: "watcher.OwnerKey, SplitKey, ShortID, IsLegacyKey, GroupKey, ScopedName"}
	for _, c := range []struct{ owner, id string }{{owner, "w-1a2b3c4d"}, {"", "w-1a2b3c4d"}, {"AIDAEXAMPLE", "sl-0011aabb"}} {
		k := OwnerKey(c.owner, c.id)
		o, id := SplitKey(k)
		keys.Cases = append(keys.Cases, parity.Case{Name: k, Input: map[string]any{"owner": c.owner, "id": c.id}, Output: map[string]any{
			"key": k, "split_owner": o, "split_id": id, "short_id": ShortID(k), "legacy": IsLegacyKey(k),
			"group_key_h100": GroupKey(ScopedName(k, "h100")),
		}})
	}

	parity.Check(t, "watcher-records.json", parity.File{Area: "DynamoDB record shapes and keys", Vectors: map[string]parity.Vectors{
		"watchItem": wv, "historyItem": hv, "groupItem": gv, "keys": keys,
	}})
}

// ddbItem marshals v the way the store does and renders it as DynamoDB JSON.
func ddbItem(t *testing.T, v any) map[string]any {
	t.Helper()
	item, err := attributevalue.MarshalMap(v)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]any{}
	for k, av := range item {
		out[k] = ddbJSON(av)
	}
	return out
}

func ddbJSON(av types.AttributeValue) any {
	switch a := av.(type) {
	case *types.AttributeValueMemberS:
		return map[string]any{"S": a.Value}
	case *types.AttributeValueMemberN:
		return map[string]any{"N": a.Value}
	case *types.AttributeValueMemberBOOL:
		return map[string]any{"BOOL": a.Value}
	case *types.AttributeValueMemberNULL:
		return map[string]any{"NULL": a.Value}
	case *types.AttributeValueMemberB:
		return map[string]any{"B": base64.StdEncoding.EncodeToString(a.Value)}
	case *types.AttributeValueMemberSS:
		ss := append([]string(nil), a.Value...)
		sort.Strings(ss) // a set has no order
		return map[string]any{"SS": ss}
	case *types.AttributeValueMemberNS:
		ns := append([]string(nil), a.Value...)
		sort.Strings(ns)
		return map[string]any{"NS": ns}
	case *types.AttributeValueMemberL:
		l := make([]any, len(a.Value))
		for i, e := range a.Value {
			l[i] = ddbJSON(e)
		}
		return map[string]any{"L": l}
	case *types.AttributeValueMemberM:
		m := map[string]any{}
		for k, e := range a.Value {
			m[k] = ddbJSON(e)
		}
		return map[string]any{"M": m}
	}
	panic(fmt.Sprintf("unhandled attribute value %T", av))
}
