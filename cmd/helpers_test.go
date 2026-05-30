package cmd

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTruncate(t *testing.T) {
	tests := []struct {
		in  string
		max int
		out string
	}{
		{"short", 10, "short"},
		{"exactlyten", 10, "exactlyten"},
		{"this is way too long", 10, "this is..."},
	}
	for _, tt := range tests {
		if got := truncate(tt.in, tt.max); got != tt.out {
			t.Errorf("truncate(%q, %d) = %q, want %q", tt.in, tt.max, got, tt.out)
		}
	}
}

func TestSplitFirst(t *testing.T) {
	tests := []struct {
		in   string
		sep  byte
		want []string
	}{
		{"email:user@example.com", ':', []string{"email", "user@example.com"}},
		{"webhook:https://x.com/a:b", ':', []string{"webhook", "https://x.com/a:b"}}, // only first sep
		{"nosep", ':', []string{"nosep"}},
		{":leading", ':', []string{"", "leading"}},
	}
	for _, tt := range tests {
		got := splitFirst(tt.in, tt.sep)
		if len(got) != len(tt.want) {
			t.Errorf("splitFirst(%q) = %v, want %v", tt.in, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("splitFirst(%q)[%d] = %q, want %q", tt.in, i, got[i], tt.want[i])
			}
		}
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"30m", 30 * time.Minute, false},
		{"4h", 4 * time.Hour, false},
		{"7d", 7 * 24 * time.Hour, false},
		{"1", 0, true},   // too short
		{"xh", 0, true},  // bad number
		{"10y", 0, true}, // unknown unit
		{"", 0, true},    // empty
	}
	for _, tt := range tests {
		got, err := parseDuration(tt.in)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseDuration(%q) err = %v, wantErr %v", tt.in, err, tt.wantErr)
			continue
		}
		if !tt.wantErr && got != tt.want {
			t.Errorf("parseDuration(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestDisplayRegions(t *testing.T) {
	if got := displayRegions(nil); got != "(all enabled)" {
		t.Errorf("displayRegions(nil) = %q, want (all enabled)", got)
	}
	if got := displayRegions([]string{"us-east-1", "us-west-2"}); got == "" {
		t.Error("displayRegions returned empty for non-empty input")
	}
}

func TestParseNotifyChannels(t *testing.T) {
	t.Run("valid email and sns", func(t *testing.T) {
		ch, err := parseNotifyChannels([]string{"email:a@b.com", "sns:arn:aws:sns:us-east-1:1:t"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(ch) != 2 {
			t.Fatalf("expected 2 channels, got %d", len(ch))
		}
		if ch[0].Type != "email" || ch[0].Target != "a@b.com" {
			t.Errorf("email channel wrong: %+v", ch[0])
		}
	})

	t.Run("valid https webhook", func(t *testing.T) {
		ch, err := parseNotifyChannels([]string{"webhook:https://hooks.example.com/x"})
		if err != nil {
			t.Fatalf("https webhook should be valid: %v", err)
		}
		if len(ch) != 1 || ch[0].Type != "webhook" {
			t.Errorf("webhook channel wrong: %+v", ch)
		}
	})

	t.Run("missing target", func(t *testing.T) {
		if _, err := parseNotifyChannels([]string{"email"}); err == nil {
			t.Error("expected error for missing target")
		}
	})

	t.Run("unknown type", func(t *testing.T) {
		if _, err := parseNotifyChannels([]string{"pigeon:x"}); err == nil {
			t.Error("expected error for unknown notify type")
		}
	})

	t.Run("unsafe webhook rejected", func(t *testing.T) {
		if _, err := parseNotifyChannels([]string{"webhook:http://evil.local/x"}); err == nil {
			t.Error("expected error for unsafe http webhook URL")
		}
	})
}

func TestLoadSpawnConfig(t *testing.T) {
	dir := t.TempDir()

	t.Run("valid yaml", func(t *testing.T) {
		path := filepath.Join(dir, "ok.yaml")
		if err := os.WriteFile(path, []byte("name: test\ninstance_type: g5.xlarge\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		data, err := loadSpawnConfig(path)
		if err != nil {
			t.Fatalf("loadSpawnConfig valid yaml: %v", err)
		}
		// Re-marshaled as JSON for DynamoDB storage.
		if len(data) == 0 || data[0] != '{' {
			t.Errorf("expected JSON output, got: %s", data)
		}
	})

	t.Run("missing file", func(t *testing.T) {
		if _, err := loadSpawnConfig(filepath.Join(dir, "nope.yaml")); err == nil {
			t.Error("expected error for missing file")
		}
	})

	t.Run("invalid yaml", func(t *testing.T) {
		path := filepath.Join(dir, "bad.yaml")
		if err := os.WriteFile(path, []byte("\tnot: : valid: yaml:"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := loadSpawnConfig(path); err == nil {
			t.Error("expected error for invalid yaml")
		}
	})
}

func TestGetOutputFormat(t *testing.T) {
	orig := outputFormat
	defer func() { outputFormat = orig }()
	outputFormat = "json"
	if got := getOutputFormat(); got != "json" {
		t.Errorf("getOutputFormat() = %q, want json", got)
	}
}
