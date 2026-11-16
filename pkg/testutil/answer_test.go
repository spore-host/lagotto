package testutil

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type snsAnswers struct{ in *sns.CreateTopicInput }

func (f *snsAnswers) CreateTopic(_ context.Context, in *sns.CreateTopicInput, _ ...func(*sns.Options)) (*sns.CreateTopicOutput, error) {
	f.in = in
	return &sns.CreateTopicOutput{TopicArn: aws.String("arn:aws:sns:us-east-1:123456789012:t")}, nil
}

func (f *snsAnswers) DeleteTopic(context.Context, *sns.DeleteTopicInput, ...func(*sns.Options)) (*sns.DeleteTopicOutput, error) {
	return nil, errors.New("scripted failure")
}

func TestAnswerFrom(t *testing.T) {
	f := &snsAnswers{}
	c := sns.NewFromConfig(aws.Config{Region: "us-east-1"}, func(o *sns.Options) { o.APIOptions = append(o.APIOptions, AnswerFrom(f)) })
	ctx := context.Background()

	out, err := c.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("t")})
	if err != nil || aws.ToString(out.TopicArn) == "" || aws.ToString(f.in.Name) != "t" {
		t.Fatalf("CreateTopic = %v, %v; fake saw %v", out, err, f.in)
	}
	if _, err := c.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: aws.String("arn")}); err == nil || !strings.Contains(err.Error(), "scripted failure") {
		t.Errorf("DeleteTopic err = %v, want the fake's", err)
	}
	if _, err := c.Publish(ctx, &sns.PublishInput{Message: aws.String("m"), TopicArn: aws.String("arn")}); err == nil || !strings.Contains(err.Error(), "has no Publish") {
		t.Errorf("Publish err = %v, want the missing-method error", err)
	}
}
