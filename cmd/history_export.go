package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/spf13/cobra"

	"github.com/spore-host/lagotto/pkg/awscfg"
	"github.com/spore-host/lagotto/pkg/watcher"
)

// `lagotto history export` (lagotto#327, part of leave-no-trace
// spore-host/spore-host#752). The match history is deleted with lagotto's
// idle teardown, by owner decision, so this is how to keep it: every row of
// the match-history table, every owner's, as JSON Lines. Read-only.

var historyExportOut string

var historyExportCmd = &cobra.Command{
	Use:         "export",
	Annotations: level(levelAdvanced),
	Short:       "Write the whole match-history table to JSON Lines (read-only)",
	Long: `Write every row of the match-history table (--history-table, default
lagotto-match-history) in one region (--region, else your configured region),
every owner's, as JSON Lines: one JSON object per row, with every attribute
the row holds, to stdout or, with --out, to a new file (created 0600; an
existing file is never overwritten). It reads and changes nothing else.

Export before lagotto is torn down: the match history is not kept as user
data. Once nothing lagotto manages has been active in a region for the grace
period (24 h), the poller's idle teardown removes the region's tables, the
match history included, whatever they hold (in this release it only reports
what it would remove; lagotto#328 turns removal on). 'lagotto teardown --plan
... --mode reap' deletes them too, and 'lagotto teardown --force' deletes them
when they still hold records.

It makes one DynamoDB call, dynamodb:Scan on the history table, which the
lagotto-cli-user policy does not grant (its item access is owner-scoped and a
Scan reads every owner's rows), so run it as an administrator. To save only
your own history, use 'lagotto history -o json'.

Numbers are written as JSON numbers with DynamoDB's exact digits, binary
values as base64 strings, and string, number and binary sets as arrays.`,
	Args: cobra.NoArgs,
	RunE: runHistoryExport,
}

func init() {
	historyCmd.AddCommand(historyExportCmd)
	historyExportCmd.Flags().StringVar(&historyExportOut, "out", "", "Write to this new file (mode 0600) instead of stdout")
}

// historyScanner is the read `lagotto history export` makes.
type historyScanner interface {
	ScanMatchHistory(ctx context.Context, fn func(map[string]types.AttributeValue) error) error
}

// historyExportStore is the history table's store; a seam for tests.
var historyExportStore = func(ctx context.Context) (historyScanner, string, error) {
	cfg, err := awscfg.Load(ctx, "")
	if err != nil {
		return nil, "", fmt.Errorf("load AWS config: %w", err)
	}
	return watcher.NewStore(cfg, watchesTable, historyTable), cfg.Region, nil
}

func runHistoryExport(cmd *cobra.Command, _ []string) error {
	ctx := context.Background()
	store, region, err := historyExportStore(ctx)
	if err != nil {
		return err
	}
	out := cmd.OutOrStdout()
	dest := "stdout"
	var file *os.File
	if historyExportOut != "" {
		f, err := os.OpenFile(historyExportOut, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // #nosec G304 -- the path the caller chose
		if err != nil {
			return fmt.Errorf("create %s (an existing file is never overwritten): %w", historyExportOut, err)
		}
		defer func() { _ = f.Close() }()
		out, dest, file = f, historyExportOut, f
	}
	n, err := exportHistory(ctx, store, out)
	if err != nil {
		return fmt.Errorf("export match history from %s after %d row(s): %w", historyTable, n, err)
	}
	if file != nil {
		if err := file.Sync(); err != nil {
			return fmt.Errorf("write %s: %w", historyExportOut, err)
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("write %s: %w", historyExportOut, err)
		}
	}
	_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "Exported %d match-history row(s) from %s in %s to %s.\n", n, historyTable, region, dest)
	return nil
}

// exportHistory writes every row store scans to w as JSON Lines and returns
// how many it wrote.
func exportHistory(ctx context.Context, store historyScanner, w io.Writer) (int, error) {
	bw := bufio.NewWriter(w)
	n := 0
	err := store.ScanMatchHistory(ctx, func(item map[string]types.AttributeValue) error {
		b, err := json.Marshal(itemJSON(item))
		if err != nil {
			return err
		}
		if _, err := bw.Write(append(b, '\n')); err != nil {
			return err
		}
		n++
		return nil
	})
	if ferr := bw.Flush(); err == nil {
		err = ferr
	}
	return n, err
}

// itemJSON is a DynamoDB item as plain JSON values, losing nothing: every
// attribute, numbers with their exact digits (json.Number), binary as base64
// ([]byte), sets as arrays (sorted, since a set has no order).
func itemJSON(item map[string]types.AttributeValue) map[string]any {
	out := make(map[string]any, len(item))
	for k, v := range item {
		out[k] = avJSON(v)
	}
	return out
}

func avJSON(v types.AttributeValue) any {
	switch t := v.(type) {
	case *types.AttributeValueMemberS:
		return t.Value
	case *types.AttributeValueMemberN:
		return json.Number(t.Value)
	case *types.AttributeValueMemberBOOL:
		return t.Value
	case *types.AttributeValueMemberNULL:
		return nil
	case *types.AttributeValueMemberB:
		return t.Value
	case *types.AttributeValueMemberM:
		return itemJSON(t.Value)
	case *types.AttributeValueMemberL:
		l := make([]any, len(t.Value))
		for i, e := range t.Value {
			l[i] = avJSON(e)
		}
		return l
	case *types.AttributeValueMemberSS:
		s := append([]string(nil), t.Value...)
		sort.Strings(s)
		return s
	case *types.AttributeValueMemberNS:
		s := append([]string(nil), t.Value...)
		sort.Strings(s)
		ns := make([]json.Number, len(s))
		for i, e := range s {
			ns[i] = json.Number(e)
		}
		return ns
	case *types.AttributeValueMemberBS:
		bs := append([][]byte(nil), t.Value...)
		sort.Slice(bs, func(i, j int) bool { return string(bs[i]) < string(bs[j]) })
		return bs
	}
	return nil
}
