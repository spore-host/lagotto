package cmd

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/spore-host/lagotto/pkg/cliiam"
	"github.com/spore-host/lagotto/pkg/idle"
	"github.com/spore-host/lagotto/pkg/inventory"
)

// The ninth review of lagotto#351.

// TestReview9UnfencedIncomplete (G3, G4): outside the aws partition, a run
// whose policy delete was refused (attached since the plan; the deleter
// never detaches) is not "removed".
func TestReview9UnfencedIncomplete(t *testing.T) {
	facts := func() inventory.Facts {
		f := orphanFacts()
		f.Regions = []inventory.Region{{Region: "us-gov-west-1"}}
		return f
	}
	_, rec, _ := stubPlanTeardown(t, facts)
	rec.failPolicy = fmt.Errorf("cliiam: lagotto-cli-user left in place: attached to user/alice: %w", cliiam.ErrAttached)
	r := runAccountWideUnfenced(context.Background(), aws.Config{}, fpAcct, nil, func(string, ...any) {})
	if r.Outcome != idle.AccountWideFailed || accountWideError(r) == nil {
		t.Errorf("%+v", r)
	}
}

// TestReview9PromptHonoursTheSignal (G5): a [y/N] prompt under
// commandContext reads as "no" once the context is done (the first Ctrl-C).
func TestReview9PromptHonoursTheSignal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	never := blockingReader{}
	done := make(chan bool, 1)
	go func() { done <- readYesCtx(ctx, never) }()
	cancel()
	select {
	case yes := <-done:
		if yes {
			t.Error("a cancelled prompt read as yes")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the prompt did not end")
	}
	if !readYesCtx(context.Background(), strings.NewReader("y\n")) || readYesCtx(context.Background(), bytes.NewReader(nil)) {
		t.Error("readYesCtx")
	}
}

type blockingReader struct{}

func (blockingReader) Read([]byte) (int, error) { select {} }
