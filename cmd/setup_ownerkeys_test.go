package cmd

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/spore-host/lagotto/pkg/testutil"
	"github.com/spore-host/lagotto/pkg/watcher"
)

type fakePrincipals struct{}

func (fakePrincipals) GetUser(_ context.Context, in *iam.GetUserInput, _ ...func(*iam.Options)) (*iam.GetUserOutput, error) {
	if aws.ToString(in.UserName) == "alice" {
		return &iam.GetUserOutput{User: &iamtypes.User{UserId: aws.String("AIDAALICE")}}, nil
	}
	return nil, &iamtypes.NoSuchEntityException{}
}

func (fakePrincipals) GetRole(context.Context, *iam.GetRoleInput, ...func(*iam.Options)) (*iam.GetRoleOutput, error) {
	return nil, errors.New("not used")
}

// TestEnsureOwnerKeys: setup's owner-keys step against the substrate
// emulator: legacy records move and the report names what stayed.
func TestEnsureOwnerKeys(t *testing.T) {
	env := testutil.SubstrateServer(t)
	ctx := context.Background()
	st := watcher.NewStore(env.AWSConfig, "ok-watches", "ok-history")
	st.SetScheduledTable("ok-scheduled")
	if _, err := st.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	db := dynamodb.NewFromConfig(env.AWSConfig)
	for _, it := range []map[string]types.AttributeValue{
		{"watch_id": &types.AttributeValueMemberS{Value: "w-1"}, "user_id": &types.AttributeValueMemberS{Value: "arn:aws:iam::123456789012:user/alice"},
			"status": &types.AttributeValueMemberS{Value: "active"}, "instance_type_pattern": &types.AttributeValueMemberS{Value: "g5.xlarge"}},
		{"watch_id": &types.AttributeValueMemberS{Value: "w-2"}, "user_id": &types.AttributeValueMemberS{Value: "arn:aws:iam::123456789012:user/gone"},
			"status": &types.AttributeValueMemberS{Value: "active"}, "instance_type_pattern": &types.AttributeValueMemberS{Value: "g5.xlarge"}},
	} {
		if _, err := db.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("ok-watches"), Item: it}); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	rep, err := ensureOwnerKeys(ctx, db, fakePrincipals{}, watcher.MigrateTables{Watches: "ok-watches", History: "ok-history", Scheduled: "ok-scheduled"}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Watches != 1 || len(rep.Skipped) != 1 {
		t.Errorf("report = %+v", rep)
	}
	for _, want := range []string{"Moved 1 record(s)", "Left 1 legacy record(s)", "w-2"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
	if w, err := st.GetWatch(ctx, "AIDAALICE/w-1"); err != nil || w == nil {
		t.Errorf("moved watch: %v %v", w, err)
	}
}
