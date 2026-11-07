package cmd

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// #274: `lagotto status` shows a fleet capped at 0 (the quota refused the
// first worker), and still shows nothing for an unmarked pre-#274 record.
func TestStatus_QuotaCapAtZero(t *testing.T) {
	at := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	var buf bytes.Buffer
	writeQuotaCap(&buf, &watcher.Watch{DesiredCount: 4, QuotaCapped: true, QuotaCappedAt: at, QuotaCapReason: "fleet capped at 0/4"})
	if !strings.Contains(buf.String(), "Quota cap: fleet capped at 0/4 since 2026-11-06T12:00:00Z") {
		t.Errorf("cap at 0 not shown:\n%s", buf.String())
	}

	buf.Reset()
	writeQuotaCap(&buf, &watcher.Watch{DesiredCount: 4, QuotaCappedAt: at})
	if buf.Len() != 0 {
		t.Errorf("unmarked pre-#274 record printed a cap:\n%s", buf.String())
	}
}
