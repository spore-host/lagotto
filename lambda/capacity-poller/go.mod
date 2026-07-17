module github.com/spore-host/lagotto/lambda/capacity-poller

go 1.26

require (
	github.com/aws/aws-lambda-go v1.54.0
	github.com/aws/aws-sdk-go-v2 v1.42.0
	github.com/aws/aws-sdk-go-v2/config v1.32.17
	github.com/aws/aws-sdk-go-v2/service/scheduler v1.17.24
	github.com/spore-host/lagotto v0.0.0-00010101000000-000000000000
	github.com/spore-host/truffle v0.38.1
)

require github.com/spore-host/libs v0.42.0 // indirect

require (
	github.com/Microsoft/hcsshim v0.14.1 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.13 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.16 // indirect
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.20.39 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.23 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.22.18 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.29 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.29 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.30 // indirect
	github.com/aws/aws-sdk-go-v2/service/account v1.32.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs v1.73.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.57.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.32.16 // indirect
	github.com/aws/aws-sdk-go-v2/service/ebs v1.34.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/ec2 v1.301.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/fsx v1.65.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/iam v1.53.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/imagebuilder v1.55.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.22 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.11.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.29 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.29 // indirect
	github.com/aws/aws-sdk-go-v2/service/kms v1.51.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/pricing v1.42.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi v1.33.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.104.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sagemaker v1.250.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/sns v1.39.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/sqs v1.42.27 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssm v1.68.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.42.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/xray v1.36.23 // indirect
	github.com/aws/smithy-go v1.27.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/containerd/cgroups/v3 v3.0.5 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spore-host/spawn v0.75.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws v0.68.0 // indirect
	go.opentelemetry.io/otel v1.43.0 // indirect
	go.opentelemetry.io/otel/metric v1.43.0 // indirect
	go.opentelemetry.io/otel/sdk v1.43.0 // indirect
	go.opentelemetry.io/otel/trace v1.43.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260401024825-9d38bb4040a9 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260401024825-9d38bb4040a9 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/spore-host/lagotto => ../..
