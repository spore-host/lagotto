package watcher

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// spawnLaunchFlags are the `spawn launch` flags a hold command may use, as
// spawn defines them (spawn cmd/launch_flags.go). Each takes a value.
var spawnLaunchFlags = map[string]bool{"region": true, "az": true, "instance-type": true, "reservation-id": true, "name": true}

// spawnNameRe is a spore name: a DNS label (it becomes the Name tag, DNS record
// and hostname). A <placeholder> is allowed for the README's template.
var spawnNameRe = regexp.MustCompile(`^([a-z0-9]([a-z0-9-]*[a-z0-9])?|<[a-z-]+>)$`)

// checkSpawnLaunch applies spawn's own launch argument rules to a rendered
// command, token by token, and returns the flag values. The rules, from spawn:
// launchNameArgs (cmd/launch_flags.go) wants exactly one name, as the
// positional or --name, and no more than one positional; runLaunch
// (cmd/launch_single.go) fails with "--name is required" without a name and
// with error.instance_type_required without --instance-type, because spawn does
// not read the type from a reservation.
func checkSpawnLaunch(t *testing.T, cmd string) map[string]string {
	t.Helper()
	toks := strings.Fields(cmd)
	if len(toks) < 2 || toks[0] != "spawn" || toks[1] != "launch" {
		t.Fatalf("not a spawn launch command: %q", cmd)
	}
	flags := map[string]string{}
	var positionals []string
	for i := 2; i < len(toks); i++ {
		tok := toks[i]
		if !strings.HasPrefix(tok, "-") {
			positionals = append(positionals, tok)
			continue
		}
		name := strings.TrimPrefix(tok, "--")
		if !strings.HasPrefix(tok, "--") || !spawnLaunchFlags[name] {
			t.Errorf("%q: flag %q is not one this command may use (%v)", cmd, tok, spawnLaunchFlags)
			continue
		}
		if i+1 >= len(toks) || strings.HasPrefix(toks[i+1], "-") || toks[i+1] == "" {
			t.Errorf("%q: flag %s has no value", cmd, tok)
			continue
		}
		flags[name] = toks[i+1]
		i++
	}
	if len(positionals) > 1 {
		t.Errorf("%q: %d positionals %v; spawn launch takes one name", cmd, len(positionals), positionals)
	}
	name := flags["name"]
	if len(positionals) == 1 {
		name = positionals[0]
	}
	if name == "" {
		t.Errorf("%q: no spore name (positional or --name); spawn fails with \"a spore name is required\"", cmd)
	} else if !spawnNameRe.MatchString(name) {
		t.Errorf("%q: name %q is not a valid spore name", cmd, name)
	}
	for _, req := range []string{"instance-type", "region", "az", "reservation-id"} {
		if flags[req] == "" {
			t.Errorf("%q: missing --%s", cmd, req)
		}
	}
	return flags
}

// TestHoldLaunchCommand_SatisfiesSpawnLaunchArgs is #208: the command lagotto
// tells a user to run for a fired hold must launch as written.
func TestHoldLaunchCommand_SatisfiesSpawnLaunchArgs(t *testing.T) {
	w, m := heldMatch()
	flags := checkSpawnLaunch(t, HoldLaunchCommandFor(w, m))
	want := map[string]string{"region": m.Region, "az": m.AvailabilityZone, "instance-type": m.InstanceType, "reservation-id": m.ReservationID}
	for k, v := range want {
		if flags[k] != v {
			t.Errorf("--%s = %q, want %q", k, flags[k], v)
		}
	}
}

// TestHoldEmail_LaunchCommandWorks gates the text actually sent: the
// `spawn launch` line in a fired hold's email is the shared command and
// passes spawn's argument rules.
func TestHoldEmail_LaunchCommandWorks(t *testing.T) {
	w, m := heldMatch()
	_, body := matchEmailSubjectBody(w, m)
	var lines []string
	for _, l := range strings.Split(body, "\n") {
		if strings.Contains(l, "spawn launch") {
			lines = append(lines, strings.TrimSpace(l))
		}
	}
	if len(lines) != 1 {
		t.Fatalf("email has %d spawn launch lines, want 1:\n%s", len(lines), body)
	}
	if lines[0] != HoldLaunchCommandFor(w, m) {
		t.Errorf("email command %q != shared builder %q", lines[0], HoldLaunchCommandFor(w, m))
	}
	checkSpawnLaunch(t, lines[0])
}

// TestREADMEHoldLaunchCommand: the README's example is the shared builder's
// output with placeholders, and passes spawn's argument rules too.
func TestREADMEHoldLaunchCommand(t *testing.T) {
	data, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatalf("read README: %v", err)
	}
	cmds := regexp.MustCompile("`(spawn launch [^`]*)`").FindAllStringSubmatch(string(data), -1)
	want := HoldLaunchCommand("<watch-id>", "<region>", "<az>", "<instance-type>", "<cr-…>")
	found := false
	for _, c := range cmds {
		if strings.Contains(c[1], "--reservation-id") {
			found = true
			if c[1] != want {
				t.Errorf("README hold command %q, want the shared form %q", c[1], want)
			}
			checkSpawnLaunch(t, c[1])
		}
	}
	if !found {
		t.Errorf("README has no spawn launch --reservation-id example; want %q", want)
	}
}

// TestHoldLaunchCommandFor_NoTypeRendersPlaceholder: never an empty
// --instance-type, which spawn would reject.
func TestHoldLaunchCommandFor_NoTypeRendersPlaceholder(t *testing.T) {
	w, m := heldMatch()
	m.InstanceType = ""
	if got := HoldLaunchCommandFor(w, m); !strings.Contains(got, "--instance-type <instance-type> ") {
		t.Errorf("got %q, want an <instance-type> placeholder", got)
	}
}
