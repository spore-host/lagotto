package watcher

import "testing"

// TestWatchFilter_Matches covers the #47 scoping predicate: project / owner /
// watch-id dimensions, AND-combined, with an empty field meaning "don't
// constrain on this dimension" and a nil filter matching everything.
func TestWatchFilter_Matches(t *testing.T) {
	w := &Watch{WatchID: "w-aaa", UserID: "arn:alice", Project: "fieldwork"}

	cases := []struct {
		name string
		f    *WatchFilter
		want bool
	}{
		{"nil matches all", nil, true},
		{"zero-value matches all", &WatchFilter{}, true},
		{"project match", &WatchFilter{Project: "fieldwork"}, true},
		{"project mismatch", &WatchFilter{Project: "other"}, false},
		{"owner match", &WatchFilter{Owner: "arn:alice"}, true},
		{"owner mismatch", &WatchFilter{Owner: "arn:bob"}, false},
		{"watch-id match", &WatchFilter{WatchIDs: []string{"w-zzz", "w-aaa"}}, true},
		{"watch-id miss", &WatchFilter{WatchIDs: []string{"w-zzz"}}, false},
		{"all dims match", &WatchFilter{Project: "fieldwork", Owner: "arn:alice", WatchIDs: []string{"w-aaa"}}, true},
		{"one dim mismatch fails AND", &WatchFilter{Project: "fieldwork", Owner: "arn:bob"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.f.matches(w); got != c.want {
				t.Errorf("matches() = %v, want %v", got, c.want)
			}
		})
	}
}

// TestWatchFilter_Empty confirms Empty() reports whether the filter constrains
// nothing (drives the daemon's "scope all vs scoped" messaging).
func TestWatchFilter_Empty(t *testing.T) {
	cases := []struct {
		name string
		f    *WatchFilter
		want bool
	}{
		{"nil", nil, true},
		{"zero", &WatchFilter{}, true},
		{"project set", &WatchFilter{Project: "x"}, false},
		{"owner set", &WatchFilter{Owner: "x"}, false},
		{"ids set", &WatchFilter{WatchIDs: []string{"w-1"}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.f.Empty(); got != c.want {
				t.Errorf("Empty() = %v, want %v", got, c.want)
			}
		})
	}
}
