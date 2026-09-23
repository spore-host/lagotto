package cmd

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/doctor"
)

// sampleReport is a report with one of each outcome, so both output formats are
// exercised against something realistic.
func sampleReport() *doctor.Report {
	return &doctor.Report{
		Region:     "us-west-2",
		AccountID:  "123456789012",
		CLIVersion: "0.60.0",
		Checks: []doctor.Check{
			{Name: doctor.CheckRuntimePolicy, Status: doctor.StatusFail,
				Summary: "the deployed lagotto-runtime-policy is BEHIND this lagotto",
				Detail:  []string{"missing actions: pricing:GetProducts"},
				Fix:     []string{"lagotto setup"}},
			{Name: doctor.CheckPollerVersion, Status: doctor.StatusWarn, Summary: "the deployed poller is OLDER than this CLI"},
			{Name: doctor.CheckTables, Status: doctor.StatusPass, Summary: "all 3 CLI-owned DynamoDB tables exist"},
		},
	}
}

// TestDoctorExitError_OnlyFailuresExitNonZero is the CI contract for the command.
func TestDoctorExitError_OnlyFailuresExitNonZero(t *testing.T) {
	if err := doctorExitError(sampleReport()); err == nil {
		t.Error("a report with a FAIL must return an error (non-zero exit)")
	} else if !strings.Contains(err.Error(), "1 check(s) failed") {
		t.Errorf("err = %v, want it to count the failures", err)
	}

	warnOnly := &doctor.Report{Checks: []doctor.Check{
		{Name: "a", Status: doctor.StatusWarn}, {Name: "b", Status: doctor.StatusPass},
	}}
	if err := doctorExitError(warnOnly); err != nil {
		t.Errorf("warnings alone must exit zero, got %v", err)
	}
}

// TestWriteDoctorReport_Table: the human format carries a marker and the status
// word per line, plus the fix section.
func TestWriteDoctorReport_Table(t *testing.T) {
	var buf bytes.Buffer
	if err := writeDoctorReport(&buf, sampleReport(), "table"); err != nil {
		t.Fatalf("writeDoctorReport: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"FAIL", "WARN", "PASS", doctor.CheckRuntimePolicy, "pricing:GetProducts", "To fix:", "lagotto setup"} {
		if !strings.Contains(out, want) {
			t.Errorf("table output missing %q:\n%s", want, out)
		}
	}
}

// TestWriteDoctorReport_JSON: the repo's -o json convention, and the drift has to
// survive into it so CI can assert on the exact finding.
func TestWriteDoctorReport_JSON(t *testing.T) {
	var buf bytes.Buffer
	if err := writeDoctorReport(&buf, sampleReport(), "json"); err != nil {
		t.Fatalf("writeDoctorReport: %v", err)
	}
	var decoded doctor.Report
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, buf.String())
	}
	if len(decoded.Checks) != 3 {
		t.Errorf("decoded %d checks, want 3", len(decoded.Checks))
	}
	if decoded.Checks[0].Status != doctor.StatusFail || decoded.Checks[0].Name != doctor.CheckRuntimePolicy {
		t.Errorf("first check = %+v, want the failing runtime-policy check", decoded.Checks[0])
	}
	// JSON output must not carry the human rendering's markers.
	if strings.Contains(buf.String(), "To fix:") {
		t.Error("JSON output leaked the table rendering")
	}
}

// TestDoctorCommandWiring: registered, argument-free, and silencing usage — a
// failed CHECK is a finding, not a usage error, so cobra must not dump the usage
// block underneath the report.
func TestDoctorCommandWiring(t *testing.T) {
	c, _, err := rootCmd.Find([]string{"doctor"})
	if err != nil || c == nil || c.Name() != "doctor" {
		t.Fatalf("rootCmd.Find(doctor) = %v, %v", c, err)
	}
	if !c.SilenceUsage {
		t.Error("doctor must set SilenceUsage: a failing check would otherwise print the usage block after the report")
	}
	if c.Args == nil {
		t.Error("doctor takes no arguments; set Args so a typo isn't silently ignored")
	}
	if c.Flags().Lookup("stack-name") == nil {
		t.Error("doctor should accept --stack-name, matching 'lagotto deploy'")
	}
}
