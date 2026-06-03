package cmd

import (
	"strings"
	"testing"
)

// TestReadYes locks the confirmation convention (spawn#40): only an explicit
// yes proceeds; everything else — including EOF from a non-interactive/piped
// stdin — reads as "no" so `cancel` without --yes can't proceed unattended.
func TestReadYes(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"y\n", true},
		{"Y\n", true},
		{"yes\n", true},
		{"YES\n", true},
		{"  y  \n", true},
		{"n\n", false},
		{"no\n", false},
		{"\n", false},   // bare enter → default no
		{"", false},     // EOF / closed pipe → no
		{"yep\n", false},
	}
	for _, c := range cases {
		if got := readYes(strings.NewReader(c.in)); got != c.want {
			t.Errorf("readYes(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
