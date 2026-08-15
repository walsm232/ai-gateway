// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package controller

import (
	"context"
	"errors"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
	gwapiv1b1 "sigs.k8s.io/gateway-api/apis/v1beta1"
)

const (
	// aiGatewayRouteKind is the kind for AIGatewayRoute.
	aiGatewayRouteKind = "AIGatewayRoute"
	// mcpRouteKind is the kind for MCPRoute.
	mcpRouteKind = "MCPRoute"
	// gatewayAPIGroup is the group for the Gateway API, e.g. HTTPRoute.
	gatewayAPIGroup = "gateway.networking.k8s.io"
	// httpRouteKind is the kind for HTTPRoute.
	httpRouteKind = "HTTPRoute"
	// coreGroup is the group for the core Kubernetes API, e.g. Service and Secret.
	coreGroup = ""
	// secretKind is the kind for the core Secret.
	secretKind = "Secret"
	// serviceKind is the kind for the core Service.
	serviceKind = "Service"
)

// errReferenceNotPermitted is wrapped into the error returned when no ReferenceGrant authorizes a
// cross-namespace reference. Callers match on it to tell an actual denial apart from a transient
// failure to evaluate the grants: only a denial may deprogram resources that were previously
// authorized.
var errReferenceNotPermitted = errors.New("not permitted")

// referenceSource identifies the kind of resource that makes a cross-namespace reference, i.e. what a
// ReferenceGrant's "from" entry must name for the reference to be permitted.
type referenceSource struct {
	group gwapiv1b1.Group
	kind  gwapiv1b1.Kind
}

var (
	aiGatewayRouteSource = referenceSource{group: aiServiceBackendGroup, kind: aiGatewayRouteKind}
	mcpRouteSource       = referenceSource{group: aiServiceBackendGroup, kind: mcpRouteKind}
	httpRouteSource      = referenceSource{group: gatewayAPIGroup, kind: httpRouteKind}
)

// ReferenceGrantValidator validates cross-namespace references using ReferenceGrant resources.
type referenceGrantValidator struct {
	client client.Client
}

// NewReferenceGrantValidator creates a new ReferenceGrantValidator.
func newReferenceGrantValidator(c client.Client) *referenceGrantValidator {
	return &referenceGrantValidator{client: c}
}

// validateAIServiceBackendReference validates that an AIGatewayRoute can reference an AIServiceBackend
// in a different namespace by checking for a valid ReferenceGrant.
//
// Parameters:
//   - ctx: context for the operation
//   - routeNamespace: namespace of the AIGatewayRoute
//   - backendNamespace: namespace of the AIServiceBackend
//   - backendName: name of the AIServiceBackend
//
// Returns:
//   - error: nil if the reference is valid (same namespace or valid ReferenceGrant exists), error otherwise
func (v *referenceGrantValidator) validateAIServiceBackendReference(
	ctx context.Context,
	routeNamespace string,
	backendNamespace string,
	backendName string,
) error {
	return v.validateReference(ctx, []referenceSource{aiGatewayRouteSource}, routeNamespace, backendNamespace, backendName, aiServiceBackendGroup, aiServiceBackendKind)
}

// validateMCPBackendReference validates that an MCPRoute can reference a backend (a Service or an Envoy
// Gateway Backend, identified by backendGroup/backendKind) in a different namespace.
//
// A cross-namespace MCP backend needs two grants: one for the MCPRoute reference, and one for the
// HTTPRoute the controller generates per backendRef, which Envoy Gateway validates on its own. Both are
// checked here so that a missing grant surfaces on the MCPRoute status instead of leaving the route
// Accepted while Envoy Gateway rejects the generated HTTPRoute.
func (v *referenceGrantValidator) validateMCPBackendReference(
	ctx context.Context,
	routeNamespace string,
	backendNamespace string,
	backendName string,
	backendGroup gwapiv1b1.Group,
	backendKind gwapiv1b1.Kind,
) error {
	return v.validateReference(ctx, []referenceSource{mcpRouteSource, httpRouteSource},
		routeNamespace, backendNamespace, backendName, backendGroup, backendKind)
}

// validateMCPSecretReference validates that an MCPRoute can reference a credential Secret in a
// different namespace. Only the MCPRoute grant applies: the Secret is read by this controller, not
// referenced by any generated resource.
func (v *referenceGrantValidator) validateMCPSecretReference(
	ctx context.Context,
	routeNamespace string,
	secretNamespace string,
	secretName string,
) error {
	return v.validateReference(ctx, []referenceSource{mcpRouteSource}, routeNamespace, secretNamespace, secretName, coreGroup, secretKind)
}

// validateInferencePoolReference validates that an AIGatewayRoute can reference an InferencePool
// in a different namespace by checking for a valid ReferenceGrant.
//
// Parameters:
//   - ctx: context for the operation
//   - routeNamespace: namespace of the AIGatewayRoute
//   - poolNamespace: namespace of the InferencePool
//   - poolName: name of the InferencePool (optional, for logging)
//
// Returns:
//   - error: nil if the reference is valid (same namespace or valid ReferenceGrant exists), error otherwise
func (v *referenceGrantValidator) validateInferencePoolReference(
	ctx context.Context,
	routeNamespace string,
	poolNamespace string,
	poolName string,
) error {
	return v.validateReference(ctx, []referenceSource{aiGatewayRouteSource}, routeNamespace, poolNamespace, poolName, inferencePoolGroup, inferencePoolKind)
}

// validateReference validates that every resource in sources can reference a target resource
// (identified by targetGroup/targetKind/targetName) in a different namespace by checking for a valid
// ReferenceGrant. A single lookup serves every source, since the grants that could authorize them are
// the same set.
func (v *referenceGrantValidator) validateReference(
	ctx context.Context,
	sources []referenceSource,
	routeNamespace string,
	targetNamespace string,
	targetName string,
	targetGroup gwapiv1b1.Group,
	targetKind gwapiv1b1.Kind,
) error {
	// Same namespace references don't need ReferenceGrant.
	if routeNamespace == targetNamespace {
		return nil
	}

	indexKey := getReferenceGrantIndexKey(targetNamespace, string(targetKind))
	var referenceGrants gwapiv1b1.ReferenceGrantList
	if err := v.client.List(ctx, &referenceGrants,
		client.MatchingFields{k8sClientIndexReferenceGrantToTargetKind: indexKey},
	); err != nil {
		return fmt.Errorf("failed to list ReferenceGrants in namespace %s for kind %s: %w",
			targetNamespace, targetKind, err)
	}

	for _, source := range sources {
		permitted := false
		for i := range referenceGrants.Items {
			if v.isReferenceGrantValid(&referenceGrants.Items[i], source, routeNamespace, targetName, targetGroup, targetKind) {
				permitted = true
				break
			}
		}
		if !permitted {
			return fmt.Errorf(
				"cross-namespace reference from %s in namespace %s to %s %s in namespace %s is %w: "+
					"no valid ReferenceGrant found in namespace %s. "+
					"A ReferenceGrant must allow %s from namespace %s to reference %s %s in namespace %s",
				source.kind, routeNamespace, targetKind, targetName, targetNamespace, errReferenceNotPermitted,
				targetNamespace,
				source.kind, routeNamespace, targetKind, targetName, targetNamespace,
			)
		}
	}

	return nil
}

// isReferenceGrantValid checks if a ReferenceGrant allows the resource identified by source to
// reference the target resource identified by targetGroup/targetKind/targetName.
func (v *referenceGrantValidator) isReferenceGrantValid(
	grant *gwapiv1b1.ReferenceGrant,
	source referenceSource,
	fromNamespace string,
	targetName string,
	targetGroup gwapiv1b1.Group,
	targetKind gwapiv1b1.Kind,
) bool {
	// Check if the grant allows references from the route's namespace.
	fromAllowed := false
	for _, from := range grant.Spec.From {
		if v.matchesFrom(&from, source, fromNamespace) {
			fromAllowed = true
			break
		}
	}

	if !fromAllowed {
		return false
	}

	// Check if the grant allows references to the target resource.
	for _, to := range grant.Spec.To {
		if v.matchesTo(&to, targetName, targetGroup, targetKind) {
			return true
		}
	}

	return false
}

// matchesFrom checks if a ReferenceGrantFrom matches a reference originating from the given source.
func (v *referenceGrantValidator) matchesFrom(from *gwapiv1b1.ReferenceGrantFrom, source referenceSource, fromNamespace string) bool {
	// Check group
	if from.Group != source.group {
		return false
	}

	// Check kind
	if from.Kind != source.kind {
		return false
	}

	// Check namespace
	if from.Namespace != gwapiv1b1.Namespace(fromNamespace) {
		return false
	}

	return true
}

// matchesTo checks if a ReferenceGrantTo matches the target resource identified by
// targetGroup/targetKind/targetName.
func (v *referenceGrantValidator) matchesTo(to *gwapiv1b1.ReferenceGrantTo, targetName string, targetGroup gwapiv1b1.Group, targetKind gwapiv1b1.Kind) bool {
	// Check group
	if to.Group != targetGroup {
		return false
	}

	// Check kind
	if to.Kind != targetKind {
		return false
	}

	// Check name. An unset name grants access to every resource of the kind, otherwise the grant is
	// limited to the named one.
	if to.Name != nil && string(*to.Name) != targetName {
		return false
	}

	return true
}
