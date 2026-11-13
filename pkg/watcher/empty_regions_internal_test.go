package watcher

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	truffleaws "github.com/spore-host/truffle/pkg/aws"
)

// regionSearcher records the regions each search ran across and, when lister is
// set, answers GetEnabledRegions — the way *truffleaws.Client does.
type regionSearcher struct {
	searched [][]string
	enabled  []string
	listErr  error
	lists    int
}

func (f *regionSearcher) SearchInstanceTypes(_ context.Context, regions []string, _ *regexp.Regexp, _ truffleaws.FilterOptions) ([]truffleaws.InstanceTypeResult, error) {
	f.searched = append(f.searched, append([]string(nil), regions...))
	return nil, nil
}
func (f *regionSearcher) GetSpotPricing(context.Context, []truffleaws.InstanceTypeResult, truffleaws.SpotOptions) ([]truffleaws.SpotPriceResult, error) {
	return nil, nil
}

type listingSearcher struct{ *regionSearcher }

func (f listingSearcher) GetEnabledRegions(context.Context) ([]string, error) {
	f.lists++
	return f.enabled, f.listErr
}

func TestResolveRegionsExpandsEmptyToEnabled(t *testing.T) {
	rs := &regionSearcher{enabled: []string{"us-east-1", "us-west-2"}}
	now := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	p := &Poller{truffle: listingSearcher{rs}, now: func() time.Time { return now }}

	got, err := p.resolveRegions(context.Background(), nil)
	if err != nil || strings.Join(got, ",") != "us-east-1,us-west-2" {
		t.Fatalf("empty regions = %v, %v; want the enabled set", got, err)
	}
	// Explicit regions pass through untouched and never list.
	got, err = p.resolveRegions(context.Background(), []string{"eu-west-1"})
	if err != nil || strings.Join(got, ",") != "eu-west-1" {
		t.Fatalf("explicit regions = %v, %v", got, err)
	}
	// Cached within the TTL: one DescribeRegions per cycle at most.
	_, _ = p.resolveRegions(context.Background(), nil)
	if rs.lists != 1 {
		t.Fatalf("listed %d times inside the TTL; want 1", rs.lists)
	}
	now = now.Add(enabledRegionsTTL + time.Second)
	_, _ = p.resolveRegions(context.Background(), nil)
	if rs.lists != 2 {
		t.Fatalf("listed %d times after the TTL; want 2", rs.lists)
	}
}

func TestResolveRegionsRefusesToSearchNothing(t *testing.T) {
	cases := map[string]capacitySearcher{
		"no lister":    &regionSearcher{},
		"list fails":   listingSearcher{&regionSearcher{listErr: errors.New("AccessDenied")}},
		"none enabled": listingSearcher{&regionSearcher{}},
	}
	for name, s := range cases {
		p := &Poller{truffle: s}
		if got, err := p.resolveRegions(context.Background(), nil); err == nil {
			t.Errorf("%s: got %v and no error; an empty watch must not search zero regions", name, got)
		}
	}
}

// The #306 regression: a stored watch with no regions is searched across every
// enabled region, not across none.
func TestPollGroupSearchesEnabledRegionsForEmptyWatch(t *testing.T) {
	store := fleetStore(t)
	rs := &regionSearcher{enabled: []string{"us-east-1", "us-west-2", "eu-west-1"}}
	p := &Poller{store: store, truffle: listingSearcher{rs}}
	w := &Watch{WatchID: "w-306", InstanceTypePattern: "t4g.nano", Status: StatusActive, Action: "notify"}
	if err := p.pollGroup(context.Background(), w.Regions, w.InstanceTypePattern, false, []*Watch{w}, &PollSummary{}); err != nil {
		t.Fatalf("pollGroup: %v", err)
	}
	if len(rs.searched) != 1 || strings.Join(rs.searched[0], ",") != "us-east-1,us-west-2,eu-west-1" {
		t.Fatalf("searched %v; want one search across the enabled regions", rs.searched)
	}
}
