package deploy

// The retain overlay for --migrate-from-cloudformation (#226): the stack's OWN
// deployed template, with DeletionPolicy/UpdateReplacePolicy: Retain added to
// every resource and nothing else changed.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	policyDeletion      = "DeletionPolicy"
	policyUpdateReplace = "UpdateReplacePolicy"
	policyRetain        = "Retain"
)

// retainPolicyKeys are the only keys the overlay may add or change.
var retainPolicyKeys = []string{policyDeletion, policyUpdateReplace}

// maxTemplateBody is CloudFormation's limit for an inline TemplateBody.
const maxTemplateBody = 51200

// withRetainPolicies returns body with DeletionPolicy: Retain and
// UpdateReplacePolicy: Retain on EVERY resource in its Resources section, and
// otherwise unchanged.
//
// body is the stack's own deployed template (GetTemplate, Original stage), so it
// may be YAML or JSON, and may carry a Transform (SAM):
//
//   - YAML is edited as TEXT, at positions taken from a yaml.Node parse: a line is
//     inserted above each resource's first key, or an existing non-Retain policy
//     line is rewritten. Re-emitting the document through a YAML encoder would
//     reformat it and put CloudFormation's short-form tags (!Ref, !GetAtt, !Sub)
//     at risk; inserting lines cannot.
//   - JSON has no tags and no formatting CloudFormation cares about, so it is
//     decoded (numbers kept verbatim), the keys are set, and it is re-encoded.
//
// Either way the result is then CHECKED, not trusted: both documents are parsed,
// the two policy keys are removed from every resource, and the remainder must be
// identical (verifyRetainOverlay). Any formatting this code does not handle (a
// flow-style resource, say) is refused rather than guessed at.
func withRetainPolicies(body string) (string, error) {
	var out string
	var err error
	if isJSONTemplate(body) {
		out, err = jsonRetainOverlay(body)
	} else {
		out, err = yamlRetainOverlay(body)
	}
	if err != nil {
		return "", fmt.Errorf("retain overlay: %w", err)
	}
	if err := verifyRetainOverlay(body, out); err != nil {
		return "", fmt.Errorf("retain overlay: %w", err)
	}
	return out, nil
}

func isJSONTemplate(body string) bool {
	return strings.HasPrefix(strings.TrimLeft(body, " \t\r\n\ufeff"), "{")
}

// jsonRetainOverlay sets both policies on every resource of a JSON template.
func jsonRetainOverlay(body string) (string, error) {
	dec := json.NewDecoder(strings.NewReader(body))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		return "", fmt.Errorf("parse JSON template: %w", err)
	}
	resources, ok := doc["Resources"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("the template has no Resources object")
	}
	for id, v := range resources {
		res, ok := v.(map[string]any)
		if !ok {
			return "", fmt.Errorf("resource %s is not an object", id)
		}
		for _, k := range retainPolicyKeys {
			res[k] = policyRetain
		}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(doc); err != nil {
		return "", fmt.Errorf("encode JSON template: %w", err)
	}
	return buf.String(), nil
}

// yamlRetainOverlay sets both policies on every resource of a YAML template by
// line edits at node positions.
func yamlRetainOverlay(body string) (string, error) {
	resources, err := yamlResources(body)
	if err != nil {
		return "", err
	}
	if resources.Style&yaml.FlowStyle != 0 {
		return "", fmt.Errorf("the template's Resources is written in flow style; edit the template by hand")
	}
	lines := strings.Split(body, "\n")
	// insertBefore[line] are lines to insert above 1-based line; replace[line]
	// replaces it.
	insertBefore := map[int][]string{}
	replace := map[int]string{}

	for i := 0; i+1 < len(resources.Content); i += 2 {
		key, val := resources.Content[i], resources.Content[i+1]
		id := key.Value
		if val.Kind != yaml.MappingNode || len(val.Content) == 0 {
			return "", fmt.Errorf("resource %s is not a mapping", id)
		}
		if val.Style&yaml.FlowStyle != 0 || val.Content[0].Line <= key.Line {
			return "", fmt.Errorf("resource %s is written in flow style; edit the template by hand", id)
		}
		first := val.Content[0]
		indent := strings.Repeat(" ", first.Column-1)
		for _, policy := range retainPolicyKeys {
			k, v := mappingValue(val, policy)
			switch {
			case k == nil:
				insertBefore[first.Line] = append(insertBefore[first.Line], indent+policy+": "+policyRetain)
			case v.Kind == yaml.ScalarNode && v.Value == policyRetain:
				// already retained
			case v.Kind == yaml.ScalarNode && v.Line == k.Line:
				replace[k.Line] = strings.Repeat(" ", k.Column-1) + policy + ": " + policyRetain
			default:
				return "", fmt.Errorf("resource %s has a %s this code cannot rewrite safely; edit the template by hand", id, policy)
			}
		}
	}

	out := make([]string, 0, len(lines)+2*len(resources.Content))
	for i, line := range lines {
		n := i + 1
		out = append(out, insertBefore[n]...)
		if r, ok := replace[n]; ok {
			line = r
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n"), nil
}

// yamlResources returns the Resources mapping node of a template (YAML or JSON:
// JSON is YAML).
func yamlResources(body string) (*yaml.Node, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(body), &doc); err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("the template is not a mapping")
	}
	_, resources := mappingValue(doc.Content[0], "Resources")
	if resources == nil || resources.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("the template has no Resources mapping")
	}
	return resources, nil
}

// mappingValue returns the key and value nodes for key in a mapping node.
func mappingValue(m *yaml.Node, key string) (*yaml.Node, *yaml.Node) {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i], m.Content[i+1]
		}
	}
	return nil, nil
}

// verifyRetainOverlay proves the overlay changed nothing but the policies: with
// DeletionPolicy and UpdateReplacePolicy removed from every resource, the two
// templates parse to the same tree (tags included), and every resource of the
// overlay carries both as Retain.
func verifyRetainOverlay(orig, overlay string) error {
	a, err := canonicalTemplate(orig)
	if err != nil {
		return fmt.Errorf("re-parse the original template: %w", err)
	}
	b, err := canonicalTemplate(overlay)
	if err != nil {
		return fmt.Errorf("re-parse the overlaid template: %w", err)
	}
	missing, err := resourcesMissingRetain(overlay, nil)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return fmt.Errorf("the overlay left %s without Retain", strings.Join(missing, ", "))
	}
	stripPolicies(a)
	stripPolicies(b)
	if !reflect.DeepEqual(a, b) {
		return fmt.Errorf("the overlaid template differs from the stack's own template by more than the retain policies; refusing")
	}
	return nil
}

// canonicalTemplate parses a template into plain maps/slices whose scalars carry
// their YAML tag, so that `!Ref X` and `X` stay different.
func canonicalTemplate(body string) (map[string]any, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(body), &doc); err != nil {
		return nil, err
	}
	if len(doc.Content) == 0 {
		return nil, fmt.Errorf("empty template")
	}
	m, ok := canonNode(doc.Content[0]).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("the template is not a mapping")
	}
	return m, nil
}

func canonNode(n *yaml.Node) any {
	switch n.Kind {
	case yaml.DocumentNode:
		if len(n.Content) == 0 {
			return nil
		}
		return canonNode(n.Content[0])
	case yaml.AliasNode:
		return canonNode(n.Alias)
	case yaml.MappingNode:
		m := map[string]any{}
		for i := 0; i+1 < len(n.Content); i += 2 {
			m[n.Content[i].Value] = canonNode(n.Content[i+1])
		}
		if t := n.ShortTag(); t != "!!map" {
			return map[string]any{"tag": t, "value": m}
		}
		return m
	case yaml.SequenceNode:
		s := make([]any, 0, len(n.Content))
		for _, c := range n.Content {
			s = append(s, canonNode(c))
		}
		if t := n.ShortTag(); t != "!!seq" {
			return map[string]any{"tag": t, "value": s}
		}
		return s
	default:
		return n.ShortTag() + ":" + n.Value
	}
}

// stripPolicies removes the two policy keys from every resource of a canonical
// template.
func stripPolicies(t map[string]any) {
	res, ok := t["Resources"].(map[string]any)
	if !ok {
		return
	}
	for _, v := range res {
		if r, ok := v.(map[string]any); ok {
			for _, k := range retainPolicyKeys {
				delete(r, k)
			}
		}
	}
}

// resourcesMissingRetain returns the resources of body that do NOT carry
// DeletionPolicy: Retain. With ids == nil every resource in the template is
// checked; otherwise exactly ids are, and an id the template does not declare
// counts as missing. Works on YAML and JSON bodies (GetTemplate returns the
// Processed stage as JSON). An unparseable body is an error: failing towards
// "don't delete" is the point.
func resourcesMissingRetain(body string, ids []string) ([]string, error) {
	resources, err := yamlResources(body)
	if err != nil {
		return nil, err
	}
	retained := map[string]bool{}
	declared := []string{}
	for i := 0; i+1 < len(resources.Content); i += 2 {
		id := resources.Content[i].Value
		declared = append(declared, id)
		if resources.Content[i+1].Kind != yaml.MappingNode {
			continue
		}
		if _, v := mappingValue(resources.Content[i+1], policyDeletion); v != nil && v.Kind == yaml.ScalarNode && v.Value == policyRetain {
			retained[id] = true
		}
	}
	if ids == nil {
		ids = declared
	}
	var missing []string
	for _, id := range ids {
		if !retained[id] {
			missing = append(missing, id)
		}
	}
	sort.Strings(missing)
	return missing, nil
}
