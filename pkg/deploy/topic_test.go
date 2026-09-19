package deploy

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
)

const testTopicARN = "arn:aws:sns:us-east-1:123456789012:lagotto-capacity-alerts"

// TestEnsureAlertsTopic_CreatePathSetsAttributes: on a fresh account the create
// carries the attributes (so a new topic is never briefly unencrypted) AND the
// convergence step then asserts them, which is what makes the behaviour
// observable to anything that doesn't record create-time attributes.
func TestEnsureAlertsTopic_CreatePathSetsAttributes(t *testing.T) {
	fs := &fakeSNS{arn: testTopicARN} // no attributes yet — a brand-new topic
	d, _ := testDeployer(nil, fs, nil)

	arn, err := d.EnsureAlertsTopic(context.Background(), "production")
	if err != nil {
		t.Fatalf("EnsureAlertsTopic: %v", err)
	}
	if arn != testTopicARN {
		t.Errorf("arn = %q, want %q", arn, testTopicARN)
	}
	if len(fs.createCalls) != 1 {
		t.Fatalf("CreateTopic called %d times, want 1", len(fs.createCalls))
	}
	c := fs.createCalls[0]
	if aws.ToString(c.Name) != AlertsTopicName {
		t.Errorf("Name = %q, want %q", aws.ToString(c.Name), AlertsTopicName)
	}
	if c.Attributes["DisplayName"] != "Lagotto Capacity Alerts" {
		t.Errorf("create DisplayName = %q", c.Attributes["DisplayName"])
	}
	if c.Attributes["KmsMasterKeyId"] != "alias/aws/sns" {
		t.Errorf("create KmsMasterKeyId = %q, want alias/aws/sns", c.Attributes["KmsMasterKeyId"])
	}
	tags := map[string]string{}
	for _, tg := range c.Tags {
		tags[aws.ToString(tg.Key)] = aws.ToString(tg.Value)
	}
	if tags["Environment"] != "production" || tags["Application"] != "lagotto" || tags["Component"] != "alerts" {
		t.Errorf("create tags = %v", tags)
	}
	// Convergence then sets both attributes, since the (new) topic reports neither.
	if len(fs.setCalls) != 2 {
		t.Fatalf("SetTopicAttributes called %d times, want 2 (DisplayName + KmsMasterKeyId)", len(fs.setCalls))
	}
	if len(fs.tagCalls) != 1 {
		t.Errorf("TagResource called %d times, want 1", len(fs.tagCalls))
	}
}

// TestEnsureAlertsTopic_RepairsOnlyDifferingAttributes: a topic left behind by an
// older or hand-rolled path (unencrypted, wrong display name) is repaired — and
// only the keys that actually differ are written.
func TestEnsureAlertsTopic_RepairsOnlyDifferingAttributes(t *testing.T) {
	t.Run("both wrong", func(t *testing.T) {
		fs := &fakeSNS{arn: testTopicARN, attributes: map[string]string{
			"DisplayName": "old name", // wrong
			// KmsMasterKeyId absent entirely — an unencrypted topic
		}}
		d, _ := testDeployer(nil, fs, nil)
		if _, err := d.EnsureAlertsTopic(context.Background(), "staging"); err != nil {
			t.Fatalf("EnsureAlertsTopic: %v", err)
		}
		if len(fs.setCalls) != 2 {
			t.Fatalf("SetTopicAttributes called %d times, want 2", len(fs.setCalls))
		}
		if fs.attributes["KmsMasterKeyId"] != "alias/aws/sns" {
			t.Errorf("KmsMasterKeyId not repaired: %v", fs.attributes)
		}
		if fs.attributes["DisplayName"] != "Lagotto Capacity Alerts" {
			t.Errorf("DisplayName not repaired: %v", fs.attributes)
		}
	})

	t.Run("only the KMS key wrong", func(t *testing.T) {
		fs := &fakeSNS{arn: testTopicARN, attributes: map[string]string{
			"DisplayName":    "Lagotto Capacity Alerts",
			"KmsMasterKeyId": "alias/some-other-key",
		}}
		d, _ := testDeployer(nil, fs, nil)
		if _, err := d.EnsureAlertsTopic(context.Background(), "production"); err != nil {
			t.Fatalf("EnsureAlertsTopic: %v", err)
		}
		if len(fs.setCalls) != 1 {
			t.Fatalf("SetTopicAttributes called %d times, want exactly 1 (only the differing key)", len(fs.setCalls))
		}
		if aws.ToString(fs.setCalls[0].AttributeName) != "KmsMasterKeyId" {
			t.Errorf("wrote %q, want KmsMasterKeyId", aws.ToString(fs.setCalls[0].AttributeName))
		}
	})

	t.Run("only the display name wrong", func(t *testing.T) {
		fs := &fakeSNS{arn: testTopicARN, attributes: map[string]string{
			"DisplayName":    "whatever",
			"KmsMasterKeyId": "alias/aws/sns",
		}}
		d, _ := testDeployer(nil, fs, nil)
		if _, err := d.EnsureAlertsTopic(context.Background(), "production"); err != nil {
			t.Fatalf("EnsureAlertsTopic: %v", err)
		}
		if len(fs.setCalls) != 1 || aws.ToString(fs.setCalls[0].AttributeName) != "DisplayName" {
			t.Errorf("SetTopicAttributes calls = %d, first = %q; want 1 × DisplayName",
				len(fs.setCalls), aws.ToString(fs.setCalls[0].AttributeName))
		}
	})
}

// TestEnsureAlertsTopic_SteadyStateIssuesNoWrites: a converged topic costs one
// read and zero attribute writes. (The tag upsert still runs — it's the only way
// a changed --environment retags an existing topic.)
func TestEnsureAlertsTopic_SteadyStateIssuesNoWrites(t *testing.T) {
	fs := &fakeSNS{arn: testTopicARN, attributes: map[string]string{
		"DisplayName":    "Lagotto Capacity Alerts",
		"KmsMasterKeyId": "alias/aws/sns",
	}}
	d, _ := testDeployer(nil, fs, nil)

	arn, err := d.EnsureAlertsTopic(context.Background(), "production")
	if err != nil {
		t.Fatalf("EnsureAlertsTopic: %v", err)
	}
	if arn != testTopicARN {
		t.Errorf("arn = %q, want the existing topic's ARN %q", arn, testTopicARN)
	}
	if fs.getCalls != 1 {
		t.Errorf("GetTopicAttributes called %d times, want 1", fs.getCalls)
	}
	if len(fs.setCalls) != 0 {
		t.Errorf("SetTopicAttributes called %d times in steady state, want 0", len(fs.setCalls))
	}
}

// TestEnsureAlertsTopic_DefaultsEnvironment: an empty --environment tags as
// production, matching the template parameter's default.
func TestEnsureAlertsTopic_DefaultsEnvironment(t *testing.T) {
	fs := &fakeSNS{arn: testTopicARN}
	d, _ := testDeployer(nil, fs, nil)
	if _, err := d.EnsureAlertsTopic(context.Background(), ""); err != nil {
		t.Fatalf("EnsureAlertsTopic: %v", err)
	}
	for _, tg := range fs.createCalls[0].Tags {
		if aws.ToString(tg.Key) == "Environment" && aws.ToString(tg.Value) != "production" {
			t.Errorf("Environment tag = %q, want production", aws.ToString(tg.Value))
		}
	}
}

func TestEnsureAlertsTopic_CreateErrorPropagates(t *testing.T) {
	fs := &fakeSNS{arn: testTopicARN, createErr: errors.New("AuthorizationError")}
	d, _ := testDeployer(nil, fs, nil)
	if _, err := d.EnsureAlertsTopic(context.Background(), "production"); err == nil {
		t.Error("expected the CreateTopic failure to propagate")
	}
}

// TestDeleteAlertsTopic_AbsentIsNotAnError: Teardown must be re-runnable.
func TestDeleteAlertsTopic_AbsentIsNotAnError(t *testing.T) {
	fs := &fakeSNS{deleteErr: &snstypes.NotFoundException{}}
	d, _ := testDeployer(nil, fs, nil)
	if err := d.deleteAlertsTopic(context.Background(), testRegion, testAccount); err != nil {
		t.Errorf("deleting an absent topic should be a no-op success, got %v", err)
	}

	fs2 := &fakeSNS{}
	d2, _ := testDeployer(nil, fs2, nil)
	if err := d2.deleteAlertsTopic(context.Background(), testRegion, testAccount); err != nil {
		t.Errorf("deleteAlertsTopic: %v", err)
	}
	if fs2.deleteCalls != 1 {
		t.Errorf("DeleteTopic called %d times, want 1", fs2.deleteCalls)
	}

	fs3 := &fakeSNS{deleteErr: errors.New("AuthorizationErrorException")}
	d3, _ := testDeployer(nil, fs3, nil)
	if err := d3.deleteAlertsTopic(context.Background(), testRegion, testAccount); err == nil {
		t.Error("a real delete failure must surface")
	}
}
