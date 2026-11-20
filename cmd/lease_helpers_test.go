package cmd

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/spore-host/libs/lifecycle"
	"github.com/spore-host/libs/lifecycle/lifecycletest"
)

// installFakeLeases answers every command's lifecycle client from table, an
// in-memory spore-host-lifecycle table shared by every region, with opts
// applied on top of a no-wait clock (lagotto#326).
func installFakeLeases(t *testing.T, table *lifecycletest.Fake, opts ...func(*lifecycle.Options)) {
	t.Helper()
	prev := newLifecycleClient
	newLifecycleClient = func(_ aws.Config, owner string) (*lifecycle.Client, error) {
		o := lifecycle.Options{
			DynamoDB: func(string) (lifecycle.API, error) { return table, nil },
			Owner:    owner,
			Sleep:    func(context.Context, time.Duration) error { return nil },
		}
		for _, f := range opts {
			f(&o)
		}
		return lifecycle.New(o)
	}
	t.Cleanup(func() { newLifecycleClient = prev })
}
