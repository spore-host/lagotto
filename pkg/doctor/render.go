package doctor

// Human-readable rendering of a Report.
//
// Kept separate from Evaluate, and pure (a string in, a string out, no writer and
// no globals), so the output can be asserted in tests exactly as a user sees it.
// The status marker arrives as a function so the caller decides between emoji and
// the ASCII alternative — lagotto's root command has --no-emoji/--accessibility,
// and a diagnostic that ignores them is a diagnostic somebody can't read.

import (
	"fmt"
	"strings"
)

// Symboler maps a status name ("success"/"warning"/"error") to a marker. It is
// exactly the shape of libs/i18n's Symbol, which is what the CLI passes in.
type Symboler func(name string) string

// PlainSymbols is the fallback marker set: no emoji, no i18n, deterministic —
// used when no localizer is available and by the tests.
func PlainSymbols(name string) string {
	switch name {
	case "success":
		return "[✓]"
	case "warning":
		return "[!]"
	case "error":
		return "[✗]"
	}
	return "[?]"
}

// i18nName maps a Status to the symbol name libs/i18n uses.
func i18nName(s Status) string {
	switch s {
	case StatusPass:
		return "success"
	case StatusWarn:
		return "warning"
	case StatusFail:
		return "error"
	}
	return "info"
}

// detailIndent lines detail up under the check summary it belongs to.
const detailIndent = "              "

// Text renders the whole report: a header, one line per check, indented detail,
// and a summary that ends in the exact commands to run when anything failed.
func (r *Report) Text(symbol Symboler) string {
	if symbol == nil {
		symbol = PlainSymbols
	}
	lines := []string{
		fmt.Sprintf("lagotto doctor — hosted poller in account %s / %s (CLI %s)",
			orUnknown(r.AccountID), orUnknown(r.Region), displayVersion(r.CLIVersion)),
		"",
	}

	for _, c := range r.Checks {
		lines = append(lines, fmt.Sprintf("%s %-4s %-15s %s",
			symbol(i18nName(c.Status)), string(c.Status), c.Name, c.Summary))
		for _, d := range c.Detail {
			lines = append(lines, detailIndent+d)
		}
	}

	pass, warn, fail := r.Counts()
	lines = append(lines, "", fmt.Sprintf("%d passed, %d warning(s), %d failure(s).", pass, warn, fail))

	// The fixes are repeated at the end on purpose: the per-check lines can scroll
	// away, and the thing a user actually needs is a short list of commands.
	if fixes := failureFixes(r); len(fixes) > 0 {
		lines = append(lines, "", "To fix:")
		for _, f := range fixes {
			lines = append(lines, "    "+f)
		}
	}
	if fail == 0 && warn == 0 {
		lines = append(lines, "", "The deployed poller matches this lagotto.")
	}
	return strings.Join(lines, "\n") + "\n"
}

// failureFixes collects the remediation commands of the FAILING checks, in check
// order and deduplicated (several checks legitimately share one fix).
func failureFixes(r *Report) []string {
	var fixes []string
	seen := map[string]bool{}
	for _, c := range r.Checks {
		if c.Status != StatusFail {
			continue
		}
		for _, f := range c.Fix {
			if !seen[f] {
				seen[f] = true
				fixes = append(fixes, f)
			}
		}
	}
	return fixes
}

func orUnknown(s string) string {
	if s == "" {
		return "(unknown)"
	}
	return s
}
