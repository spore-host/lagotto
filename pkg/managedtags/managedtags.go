// Package managedtags is how lagotto tags what it creates (lagotto#324), so
// leave-no-trace cleanup (spore-host/spore-host#752) can find every resource
// lagotto made and nothing it did not.
//
// # The keys
//
// Every resource lagotto creates carries the spore.host tags from
// github.com/spore-host/libs/tags: spore-host:managed-by=lagotto (the uniform
// key cleanup selects on), spore-host:scope, spore-host:created,
// spore-host:schema and, where the caller's principal id is cheaply at hand,
// spore-host:owner. Scope is the region for a regional resource and "global"
// for an IAM one ([ScopeGlobal]).
//
// The long-lived machinery a deployment runs on (the poller Lambda, its log
// group, roles, topic, schedule group, tables, artifact bucket, the CLI-user
// policy and lagotto's instance role) also carries spore-host:role=service and
// a spore-host:component naming its part ([Service]). What the poller makes
// for a user's watch (an instance, a capacity reservation, a SageMaker job)
// carries no role ([Workload]).
//
// # lagotto:managed
//
// lagotto:managed is the older, per-resource ownership marker, and code still
// matches on its exact values, so they are kept where they are and never
// added anywhere new:
//
//   - "cli": created by the lagotto CLI (the three DynamoDB tables, the
//     artifact bucket, the poller log group, the lagotto-cli-user policy).
//     `teardown` and `deploy --teardown` delete only what carries it.
//   - "true": created by the poller at runtime (capacity reservations,
//     SageMaker jobs). `lagotto cancel` releases only a reservation that
//     carries it.
//   - "instance-profile": an instance profile (and lagotto's own instance
//     role) that `setup` created. The instance-profile deleters require it.
//
// spore-host:managed-by=lagotto is set on all of those and on everything
// that has no lagotto:managed tag at all (the SNS topic, the Lambda, the
// poller's IAM roles, the schedule group).
package managedtags

import (
	"time"

	"github.com/spore-host/libs/tags"
)

// ScopeGlobal is the scope of an IAM resource, which belongs to no region.
const ScopeGlobal = "global"

// Components of the deployment, the spore-host:component value on each
// service resource.
const (
	ComponentPoller       = "capacity-poller"
	ComponentAlerts       = "alerts"
	ComponentStore        = "store"
	ComponentArtifacts    = "artifacts"
	ComponentCLIPolicy    = "cli-policy"
	ComponentInstanceRole = "instance-role"
)

// Workload is the spore.host tag set for something the poller creates for a
// watch: spore-host:managed-by=lagotto and the rest of tags.For, with no role.
// An empty owner is omitted.
func Workload(scope, owner string, now time.Time) map[string]string {
	return tags.For(tags.ToolLagotto, scope, now, owner)
}

// Service is the spore.host tag set for a piece of lagotto's own long-lived
// machinery: [Workload]'s tags plus spore-host:role=service and
// spore-host:component=component.
func Service(scope, owner, component string, now time.Time) map[string]string {
	m := Workload(scope, owner, now)
	m[tags.Role] = tags.RoleService
	if component != "" {
		m[tags.Component] = component
	}
	return m
}

// With returns a new map holding base and then extra (extra wins on a key
// both have). Neither argument is modified.
func With(base map[string]string, extra map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(extra))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

// sticky are the keys whose first value is kept: when the resource was
// created and who created it do not change when somebody else re-runs setup
// or deploy later.
var sticky = map[string]bool{tags.Created: true, tags.Owner: true}

// Converge returns the tags to write so a resource that has existing ends up
// carrying desired: every desired key whose value differs, except that a
// spore-host:created or spore-host:owner already on the resource is kept. A
// resource that already has everything yields nil, so the caller can skip the
// write. existing may be nil.
//
// It is how setup and deploy back-fill the spore.host tags onto a resource an
// older lagotto created: the resource gets them all, and its spore-host:created
// says when lagotto first tagged it.
func Converge(existing, desired map[string]string) map[string]string {
	var out map[string]string
	for k, v := range desired {
		cur, ok := existing[k]
		if ok && (cur == v || (sticky[k] && cur != "")) {
			continue
		}
		if out == nil {
			out = map[string]string{}
		}
		out[k] = v
	}
	return out
}

// Stamp is who creates a resource and when, for its spore-host:owner and
// spore-host:created tags. The zero Stamp omits the owner and uses the
// current time.
type Stamp struct {
	// Owner is the creating principal's aws:userid. Empty omits the tag.
	Owner string
	// Now is the clock; nil is time.Now.
	Now func() time.Time
}

// FirstStamp returns the first of stamps, or the zero Stamp. It is how a
// function takes an optional trailing Stamp argument.
func FirstStamp(stamps []Stamp) Stamp {
	if len(stamps) > 0 {
		return stamps[0]
	}
	return Stamp{}
}

func (s Stamp) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Service is [Service] with the stamp's owner and time.
func (s Stamp) Service(scope, component string) map[string]string {
	return Service(scope, s.Owner, component, s.now())
}

// Workload is [Workload] with the stamp's owner and time.
func (s Stamp) Workload(scope string) map[string]string {
	return Workload(scope, s.Owner, s.now())
}
