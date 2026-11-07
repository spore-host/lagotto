package doctor

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spore-host/lagotto/pkg/watcher"
)

// The email-subscriptions check (#240).

func emailState(t *testing.T, audit watcher.EmailAudit) State {
	st := healthyState(t)
	st.EmailChecked = true
	st.Email = audit
	return st
}

func TestEvaluate_EmailAllConfirmedPasses(t *testing.T) {
	r := Evaluate(emailState(t, watcher.EmailAudit{Confirmed: []string{"alice@example.com"}}))
	if c := find(t, r, CheckEmailSubs); c.Status != StatusPass {
		t.Errorf("email-subscriptions = %s (%s), want PASS", c.Status, c.Summary)
	}
}

// TestEvaluate_EmailPendingWarns: an address that has not clicked its
// confirmation link receives nothing; doctor names it.
func TestEvaluate_EmailPendingWarns(t *testing.T) {
	r := Evaluate(emailState(t, watcher.EmailAudit{
		Confirmed: []string{"alice@example.com"},
		Pending:   []string{"bob@example.com"},
		Missing:   []string{"zed@example.com"},
		Unused:    []string{"old@example.com"},
		Unfiltered: []watcher.EmailSubscription{{Address: "carol@example.com",
			SubscriptionArn: "arn:aws:sns:us-west-2:123456789012:lagotto-capacity-alerts:abc"}},
	}))
	c := find(t, r, CheckEmailSubs)
	if c.Status != StatusWarn {
		t.Fatalf("email-subscriptions = %s, want WARN", c.Status)
	}
	if r.Failed() {
		t.Error("an email-subscription finding failed doctor; it is a WARN")
	}
	detail := strings.Join(c.Detail, "\n")
	for _, want := range []string{
		"pending confirmation: bob@example.com",
		"not subscribed: zed@example.com",
		"unused: old@example.com",
		"NO FILTER POLICY: carol@example.com",
	} {
		if !strings.Contains(detail, want) {
			t.Errorf("detail lacks %q:\n%s", want, detail)
		}
	}
	fix := strings.Join(c.Fix, "\n")
	for _, want := range []string{"lagotto notify-gc --resubscribe", "lagotto notify-gc", "aws sns unsubscribe --region us-west-2"} {
		if !strings.Contains(fix, want) {
			t.Errorf("fix lacks %q:\n%s", want, fix)
		}
	}
}

func TestEvaluate_EmailOmittedWhenNothingToSay(t *testing.T) {
	absent(t, Evaluate(emailState(t, watcher.EmailAudit{})), CheckEmailSubs)
	absent(t, Evaluate(healthyState(t)), CheckEmailSubs)
}

func TestEvaluate_EmailNoTopicAndReadError(t *testing.T) {
	r := Evaluate(emailState(t, watcher.EmailAudit{NoTopic: true, Missing: []string{"alice@example.com"}}))
	if c := find(t, r, CheckEmailSubs); c.Status != StatusWarn || !strings.Contains(strings.Join(c.Fix, " "), "lagotto deploy") {
		t.Errorf("no topic: %+v, want WARN naming lagotto deploy", c)
	}
	st := emailState(t, watcher.EmailAudit{})
	st.EmailErr = errors.New("AccessDenied")
	if c := find(t, Evaluate(st), CheckEmailSubs); c.Status != StatusWarn {
		t.Errorf("read error: %s, want WARN", c.Status)
	}
}

// TestGather_CallsEmailAudit: Gather reads the audit when one is supplied.
func TestGather_CallsEmailAudit(t *testing.T) {
	called := false
	st := Gather(context.Background(), Options{Region: testRegion, AccountID: testAccount,
		EmailAudit: func(context.Context) (watcher.EmailAudit, error) {
			called = true
			return watcher.EmailAudit{Pending: []string{"bob@example.com"}}, nil
		}})
	if !called || !st.EmailChecked || len(st.Email.Pending) != 1 {
		t.Errorf("Gather: called=%v checked=%v email=%+v", called, st.EmailChecked, st.Email)
	}
}
