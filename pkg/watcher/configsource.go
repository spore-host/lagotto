package watcher

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ConfigReader fetches the bytes at a config reference. A reference is a local
// filesystem path, an "s3://bucket/key" URI, or "-" for stdin. It's the seam
// that lets a watch's spawn config (and the files it references) be read from
// whichever source the creator has, and be unit-tested without real S3.
//
// The point (lagotto#132, #140): a hosted poller has no access to the machine
// that created the watch, so resolving these references must happen once at
// watch-creation — on the creating machine, which has the files and creds —
// and the resolved content stored inline. This reader is what makes s3:// and
// stdin config sources work; MakeSelfContained is what folds the referenced
// files into the stored config.
type ConfigReader func(ctx context.Context, ref string) ([]byte, error)

// IsS3Ref reports whether ref is an s3:// URI.
func IsS3Ref(ref string) bool {
	return strings.HasPrefix(ref, "s3://")
}

// parseS3Ref splits "s3://bucket/key" into its bucket and key.
func parseS3Ref(ref string) (bucket, key string, err error) {
	rest := strings.TrimPrefix(ref, "s3://")
	i := strings.IndexByte(rest, '/')
	if i <= 0 || i == len(rest)-1 {
		return "", "", fmt.Errorf("invalid s3 URI %q: want s3://bucket/key", ref)
	}
	return rest[:i], rest[i+1:], nil
}

// NewConfigReader returns a ConfigReader that serves local paths from the
// filesystem, "-" from stdin, and s3:// URIs via S3. The AWS config is loaded
// lazily on the first s3:// reference (via loadCfg) and the S3 client cached,
// so a purely-local config never loads AWS config or needs credentials. Pass a
// nil stdin to default to os.Stdin.
func NewConfigReader(loadCfg func(ctx context.Context) (aws.Config, error), stdin io.Reader) ConfigReader {
	if stdin == nil {
		stdin = os.Stdin
	}
	var (
		once    sync.Once
		client  *s3.Client
		loadErr error
	)
	return func(ctx context.Context, ref string) ([]byte, error) {
		switch {
		case ref == "-":
			return io.ReadAll(stdin)
		case IsS3Ref(ref):
			bucket, key, err := parseS3Ref(ref)
			if err != nil {
				return nil, err
			}
			once.Do(func() {
				cfg, err := loadCfg(ctx)
				if err != nil {
					loadErr = err
					return
				}
				client = s3.NewFromConfig(cfg)
			})
			if loadErr != nil {
				return nil, fmt.Errorf("load AWS config for %s: %w", ref, loadErr)
			}
			out, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				return nil, fmt.Errorf("get %s: %w", ref, err)
			}
			defer out.Body.Close()
			return io.ReadAll(out.Body)
		default:
			return os.ReadFile(ref)
		}
	}
}
