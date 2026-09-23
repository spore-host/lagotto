package runtimeiam

// Reading the DEPLOYED runtime policy back, and comparing it against what THIS
// binary would apply (#156).
//
// `lagotto setup` writes the policy with PutRolePolicy, which REPLACES
// lagotto-runtime-policy wholesale with whatever PolicyDocument compiles into the
// binary that ran it. So an account can sit in either half of a drift:
//
//   - the deployed policy is BEHIND this binary (somebody upgraded lagotto and
//     never re-ran `setup`) — new code silently lacks a permission it needs, and
//     the only evidence is an AccessDenied in the poller's CloudWatch Logs;
//   - the deployed policy is AHEAD of this binary (a newer lagotto applied it) —
//     running THIS binary's `setup` would revert the newer grants.
//
// Both halves have bitten real accounts (#149 iam:* on spawn-instance*, #151
// pricing:GetProducts, #150, #153 servicequotas), and neither produces a signal
// until a watch fails. That is what this file exists to make visible.
//
// # Why not a text diff
//
// Because a text diff of two policy documents is almost all noise. The deployed
// document comes back from IAM URL-encoded, re-serialized by IAM (not
// byte-identical to what was PUT), with its own key order and whitespace, and any
// re-grouping of the same grants across statements changes every line while
// changing nothing about what the poller may do. The comparison here is therefore
// SEMANTIC: both documents are normalized into a set of (effect, action,
// resource, condition) grants — the cross product IAM itself evaluates — and the
// two sets are diffed. Key order, whitespace, statement grouping and action/
// resource ordering are all invisible to it; a genuinely added or removed
// permission is not.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
)

// ErrRoleNotFound / ErrPolicyNotFound distinguish the two ways the deployed
// policy can be absent, because the fix differs: no role at all means the poller
// was never deployed (`lagotto deploy`), while a role without the inline policy
// means `setup` has never been run against it (`lagotto setup`). IAM answers both
// with the same NoSuchEntity, so ReadRuntimePolicy does the extra GetRole needed
// to tell them apart.
var (
	ErrRoleNotFound   = errors.New("runtimeiam: poller execution role not found")
	ErrPolicyNotFound = errors.New("runtimeiam: runtime policy not attached to the poller role")
)

// Grant is one atomic permission: a single action on a single resource, under a
// single (canonicalized) condition block, with its Effect. It is the unit IAM
// actually evaluates, and expanding a statement's Action × Resource lists into
// grants is what makes two differently-GROUPED policies compare equal when they
// permit the same things.
//
// Condition holds the statement's Condition block as canonical JSON (Go marshals
// maps with sorted keys), or "" when there is none. It is part of the identity of
// a grant on purpose: `iam:PassRole` on `*` conditioned to sagemaker.amazonaws.com
// and `iam:PassRole` on `*` unconditioned are wildly different permissions, and a
// diff that called them equal would hide the more dangerous one.
type Grant struct {
	Effect    string `json:"effect"`
	Action    string `json:"action"`
	Resource  string `json:"resource"`
	Condition string `json:"condition,omitempty"`
}

// String renders a grant the way the doctor output reports it.
func (g Grant) String() string {
	s := fmt.Sprintf("%s on %s", g.Action, g.Resource)
	if !strings.EqualFold(g.Effect, "Allow") {
		s = fmt.Sprintf("%s %s", strings.ToUpper(g.Effect), s)
	}
	if g.Condition != "" {
		s += " when " + g.Condition
	}
	return s
}

// PolicyDiff is the semantic difference between a deployed policy and the one
// this binary would apply.
//
// The asymmetry is the whole point, and callers are expected to treat the two
// sides differently: Missing means the deployed poller CANNOT do something this
// binary's code assumes it can (a silent degradation — worth failing over),
// while Extra means the deployed policy was applied by a NEWER lagotto and
// running this binary's `setup` would revert those grants (worth warning about,
// but nothing is broken right now).
type PolicyDiff struct {
	// Missing: expected by this binary, absent from the deployed policy.
	Missing []Grant `json:"missing,omitempty"`
	// Extra: present in the deployed policy, unknown to this binary.
	Extra []Grant `json:"extra,omitempty"`
}

// Empty reports whether the deployed policy is semantically identical to what
// this binary would apply.
func (d *PolicyDiff) Empty() bool {
	return d == nil || (len(d.Missing) == 0 && len(d.Extra) == 0)
}

// MissingActions returns the distinct actions with at least one missing grant,
// sorted. This is the headline a human reads first ("you're missing
// pricing:GetProducts").
func (d *PolicyDiff) MissingActions() []string { return distinctActions(d.Missing) }

// ExtraActions returns the distinct actions with at least one extra grant, sorted.
func (d *PolicyDiff) ExtraActions() []string { return distinctActions(d.Extra) }

// MissingSummary / ExtraSummary render the diff grouped by action so the report
// says "iam:GetRole on <two ARNs>" instead of repeating the action per resource.
func (d *PolicyDiff) MissingSummary() []string { return Summarize(d.Missing) }

// ExtraSummary is MissingSummary's counterpart for the extra grants.
func (d *PolicyDiff) ExtraSummary() []string { return Summarize(d.Extra) }

func distinctActions(grants []Grant) []string {
	seen := map[string]bool{}
	var out []string
	for _, g := range grants {
		if !seen[g.Action] {
			seen[g.Action] = true
			out = append(out, g.Action)
		}
	}
	sort.Strings(out)
	return out
}

// Summarize groups grants by (effect, action, condition) and renders one line per
// group, listing that group's resources. Grouping keeps a diff readable when a
// single action differs on several ARNs — the #149 case, where iam:GetRole and
// friends were missing on both spawn-instance* ARNs at once.
func Summarize(grants []Grant) []string {
	type group struct {
		g         Grant
		resources []string
	}
	order := []string{}
	groups := map[string]*group{}
	for _, g := range grants {
		key := g.Effect + "\x00" + g.Action + "\x00" + g.Condition
		if groups[key] == nil {
			groups[key] = &group{g: g}
			order = append(order, key)
		}
		groups[key].resources = append(groups[key].resources, g.Resource)
	}
	sort.Slice(order, func(i, j int) bool {
		a, b := groups[order[i]].g, groups[order[j]].g
		if a.Action != b.Action {
			return a.Action < b.Action
		}
		return a.Condition < b.Condition
	})
	out := make([]string, 0, len(order))
	for _, key := range order {
		gr := groups[key]
		sort.Strings(gr.resources)
		line := fmt.Sprintf("%s on %s", gr.g.Action, strings.Join(dedupe(gr.resources), ", "))
		if !strings.EqualFold(gr.g.Effect, "Allow") {
			line = strings.ToUpper(gr.g.Effect) + " " + line
		}
		if gr.g.Condition != "" {
			line += " when " + gr.g.Condition
		}
		out = append(out, line)
	}
	return out
}

func dedupe(ss []string) []string {
	out := ss[:0:0]
	for i, s := range ss {
		if i == 0 || s != ss[i-1] {
			out = append(out, s)
		}
	}
	return out
}

// DiffPolicy compares a deployed policy document against an expected one and
// reports the semantic difference. Both arguments are raw policy documents;
// either may be URL-encoded (as IAM returns them) or plain JSON, with any key
// order, whitespace and statement grouping.
func DiffPolicy(deployed, expected string) (*PolicyDiff, error) {
	deployedGrants, err := ParsePolicy(deployed)
	if err != nil {
		return nil, fmt.Errorf("parse deployed policy: %w", err)
	}
	expectedGrants, err := ParsePolicy(expected)
	if err != nil {
		return nil, fmt.Errorf("parse expected policy: %w", err)
	}
	return diffGrants(deployedGrants, expectedGrants), nil
}

// DiffRuntimePolicy is DiffPolicy against this binary's own PolicyDocument for
// the given region/account — the comparison `lagotto doctor` actually makes.
func DiffRuntimePolicy(deployed, region, accountID string) (*PolicyDiff, error) {
	expected, err := PolicyDocument(region, accountID)
	if err != nil {
		return nil, fmt.Errorf("build expected policy: %w", err)
	}
	return DiffPolicy(deployed, expected)
}

func diffGrants(deployed, expected []Grant) *PolicyDiff {
	have := indexGrants(deployed)
	want := indexGrants(expected)
	diff := &PolicyDiff{}
	for _, g := range expected {
		if _, ok := have[grantKey(g)]; !ok {
			diff.Missing = append(diff.Missing, g)
		}
	}
	for _, g := range deployed {
		if _, ok := want[grantKey(g)]; !ok {
			diff.Extra = append(diff.Extra, g)
		}
	}
	sortGrants(diff.Missing)
	sortGrants(diff.Extra)
	return diff
}

func indexGrants(grants []Grant) map[string]Grant {
	m := make(map[string]Grant, len(grants))
	for _, g := range grants {
		m[grantKey(g)] = g
	}
	return m
}

// grantKey is the identity of a grant. Effect is lower-cased because IAM treats
// it case-insensitively; action names likewise ("EC2:RunInstances" is the same
// permission as "ec2:RunInstances"). Resource ARNs and condition VALUES are left
// alone — those are case-sensitive in general.
func grantKey(g Grant) string {
	return strings.ToLower(g.Effect) + "\x00" + strings.ToLower(g.Action) + "\x00" + g.Resource + "\x00" + g.Condition
}

func sortGrants(grants []Grant) {
	sort.Slice(grants, func(i, j int) bool {
		if grants[i].Action != grants[j].Action {
			return grants[i].Action < grants[j].Action
		}
		if grants[i].Resource != grants[j].Resource {
			return grants[i].Resource < grants[j].Resource
		}
		return grants[i].Condition < grants[j].Condition
	})
}

// --- parsing ----------------------------------------------------------------

// stringOrSlice accepts an IAM field that may be a bare string or an array of
// strings — the two forms are equivalent in a policy document, and the deployed
// document may well use the other one from what this binary emits.
type stringOrSlice []string

func (s *stringOrSlice) UnmarshalJSON(data []byte) error {
	var one string
	if err := json.Unmarshal(data, &one); err == nil {
		*s = []string{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(data, &many); err != nil {
		return err
	}
	*s = many
	return nil
}

type rawStatement struct {
	Sid         string          `json:"Sid"`
	Effect      string          `json:"Effect"`
	Action      stringOrSlice   `json:"Action"`
	NotAction   stringOrSlice   `json:"NotAction"`
	Resource    stringOrSlice   `json:"Resource"`
	NotResource stringOrSlice   `json:"NotResource"`
	Principal   json.RawMessage `json:"Principal"`
	Condition   json.RawMessage `json:"Condition"`
}

// ParsePolicy normalizes a policy document into a sorted grant set.
//
// It tolerates every representational difference two equivalent documents can
// have: URL-encoding (IAM returns policies RFC-3986-encoded; a fake or emulator
// returns plain JSON), a single `Statement` object instead of a one-element
// array, `Action`/`Resource` as a string instead of an array, arbitrary key order
// and arbitrary whitespace.
//
// A statement using NotAction / NotResource / Principal is NOT expanded — those
// invert or re-target the match and a cross product would misrepresent them.
// Such a statement becomes ONE opaque grant keyed by its canonical JSON, so a
// change to it is still detected and reported, just not described action by
// action. lagotto's own PolicyDocument uses none of them.
func ParsePolicy(doc string) ([]Grant, error) {
	decoded := decodePolicyDocument(doc)
	if strings.TrimSpace(decoded) == "" {
		return nil, fmt.Errorf("policy document is empty")
	}
	var envelope struct {
		Version   string          `json:"Version"`
		Statement json.RawMessage `json:"Statement"`
	}
	if err := json.Unmarshal([]byte(decoded), &envelope); err != nil {
		return nil, fmt.Errorf("not a JSON policy document: %w", err)
	}
	statements, err := parseStatements(envelope.Statement)
	if err != nil {
		return nil, err
	}
	var grants []Grant
	for _, st := range statements {
		grants = append(grants, expandStatement(st)...)
	}
	sortGrants(grants)
	return grants, nil
}

// parseStatements accepts either an array of statements or a single statement
// object (both are legal IAM).
func parseStatements(raw json.RawMessage) ([]rawStatement, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("policy document has no Statement")
	}
	var many []rawStatement
	if err := json.Unmarshal(raw, &many); err == nil {
		return many, nil
	}
	var one rawStatement
	if err := json.Unmarshal(raw, &one); err != nil {
		return nil, fmt.Errorf("policy Statement is neither an object nor an array: %w", err)
	}
	return []rawStatement{one}, nil
}

func expandStatement(st rawStatement) []Grant {
	effect := st.Effect
	if effect == "" {
		effect = "Allow"
	}
	cond := canonicalJSON(st.Condition)

	// Anything that inverts or re-targets the match is kept opaque — see
	// ParsePolicy's note.
	if len(st.NotAction) > 0 || len(st.NotResource) > 0 || len(st.Principal) > 0 {
		blob, err := json.Marshal(st)
		if err != nil {
			blob = []byte(st.Sid)
		}
		return []Grant{{
			Effect:    effect,
			Action:    "(unexpanded statement)",
			Resource:  string(blob),
			Condition: cond,
		}}
	}

	resources := []string(st.Resource)
	if len(resources) == 0 {
		// A resource-less statement is only legal for some APIs; represent it
		// explicitly rather than dropping the actions on the floor.
		resources = []string{"(no Resource)"}
	}
	grants := make([]Grant, 0, len(st.Action)*len(resources))
	for _, a := range st.Action {
		for _, r := range resources {
			grants = append(grants, Grant{Effect: effect, Action: a, Resource: r, Condition: cond})
		}
	}
	return grants
}

// canonicalJSON re-marshals a JSON blob through interface{} so object keys come
// back sorted and all insignificant whitespace is gone. That is what lets two
// conditions that differ only in key order compare equal.
//
// The generic interface{} is required, not laziness: an IAM Condition block is an
// open-ended map of operator → key → value(s) (and the value may be a string or
// an array), so there is no concrete struct that can round-trip an arbitrary one.
// Nothing is dispatched on the decoded shape — it is immediately re-marshalled to
// a string used only as a comparison key — so the deserialization-gadget concern
// the rule is about does not apply.
func canonicalJSON(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	// nosemgrep: go.lang.security.deserialization.unsafe-deserialization-interface.go-unsafe-deserialization-interface -- see above: an IAM Condition has no fixed schema, and the value is only re-marshalled into a comparison key.
	var v interface{}
	if err := json.Unmarshal(raw, &v); err != nil {
		// Unparseable: compare it verbatim rather than silently ignoring it.
		return string(raw)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return string(raw)
	}
	return string(b)
}

// decodePolicyDocument URL-decodes an IAM policy document when it needs it.
//
// GetRolePolicy returns the document "URL-encoded compliant with RFC 3986" and
// the SDK does not decode it, so the raw string is typically
// `%7B%22Version%22%3A...`. Fakes, emulators and hand-written fixtures return
// plain JSON. Deciding by "does it already parse as JSON" handles both without a
// flag, and PathUnescape (not QueryUnescape) is used deliberately: QueryUnescape
// would turn a literal '+' inside an ARN or condition value into a space.
func decodePolicyDocument(doc string) string {
	trimmed := strings.TrimSpace(doc)
	if trimmed == "" {
		return trimmed
	}
	if json.Valid([]byte(trimmed)) {
		return trimmed
	}
	if unescaped, err := url.PathUnescape(trimmed); err == nil {
		return strings.TrimSpace(unescaped)
	}
	return trimmed
}

// --- reading the deployed policy --------------------------------------------

// ReadRuntimePolicy returns the inline runtime policy document currently attached
// to the poller execution role, decoded to plain JSON.
//
// The two "absent" cases are reported as ErrRoleNotFound / ErrPolicyNotFound
// (use errors.Is) so the caller can name the right next step; every other error
// is returned as-is, because an AccessDenied must never be reported as "the
// policy isn't there" — that sends the user off re-running `setup` to fix a
// permissions problem.
func ReadRuntimePolicy(ctx context.Context, client IAMAPI) (string, error) {
	out, err := client.GetRolePolicy(ctx, &iam.GetRolePolicyInput{
		RoleName:   aws.String(RoleName),
		PolicyName: aws.String(PolicyName),
	})
	if err == nil {
		return decodePolicyDocument(aws.ToString(out.PolicyDocument)), nil
	}
	var notFound *iamtypes.NoSuchEntityException
	if !errors.As(err, &notFound) {
		return "", fmt.Errorf("runtimeiam: get inline policy %s on role %s: %w", PolicyName, RoleName, err)
	}
	// IAM answers a missing ROLE and a missing POLICY with the same NoSuchEntity,
	// so one extra call is what separates "never deployed" from "never set up".
	exists, rErr := roleExists(ctx, client, RoleName)
	if rErr != nil {
		return "", fmt.Errorf("runtimeiam: check role %s: %w", RoleName, rErr)
	}
	if !exists {
		return "", ErrRoleNotFound
	}
	return "", ErrPolicyNotFound
}

// RoleExists reports whether the named role exists, treating NoSuchEntity as
// "absent" rather than an error. Exported for read-only diagnostics (`lagotto
// doctor`); the package's own Ensure* paths use the unexported form.
func RoleExists(ctx context.Context, client IAMAPI, name string) (bool, error) {
	return roleExists(ctx, client, name)
}
