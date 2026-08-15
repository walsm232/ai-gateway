// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package controller

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aigv1b1 "github.com/envoyproxy/ai-gateway/api/v1beta1"
)

// ReferenceGrantController implements [reconcile.TypedReconciler] for ReferenceGrant.
//
// This controller watches ReferenceGrant resources and triggers reconciliation of
// affected AIGatewayRoutes and MCPRoutes when grants are created, updated, or deleted.
//
// Exported for testing purposes.
type ReferenceGrantController struct {
	client             client.Client
	logger             logr.Logger
	aiGatewayRouteChan chan event.GenericEvent
	mcpRouteChan       chan event.GenericEvent
}

// NewReferenceGrantController creates a new [reconcile.TypedReconciler] for ReferenceGrant.
func NewReferenceGrantController(
	c client.Client,
	logger logr.Logger,
	aiGatewayRouteChan chan event.GenericEvent,
	mcpRouteChan chan event.GenericEvent,
) *ReferenceGrantController {
	return &ReferenceGrantController{
		client:             c,
		logger:             logger,
		aiGatewayRouteChan: aiGatewayRouteChan,
		mcpRouteChan:       mcpRouteChan,
	}
}

// Reconcile implements the [reconcile.TypedReconciler] for ReferenceGrant.
func (c *ReferenceGrantController) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	c.logger.Info("Reconciling ReferenceGrant", "namespace", req.Namespace, "name", req.Name)

	// Routes are looked up by the namespace they reference rather than by the grant's own "from"
	// entries. Those entries are unknown once the grant is deleted, and on an update they no longer
	// name the routes an entry was just removed from — both of which must still be reconciled so a
	// revoked grant is reflected on their status.
	targetNamespace := req.Namespace

	// Get all AIGatewayRoutes that might be affected by this ReferenceGrant
	affectedRoutes, err := c.getAffectedAIGatewayRoutes(ctx, targetNamespace)
	if err != nil {
		c.logger.Error(err, "failed to get affected AIGatewayRoutes",
			"namespace", req.Namespace, "name", req.Name)
		return ctrl.Result{}, err
	}

	// Trigger reconciliation for each affected AIGatewayRoute
	for _, route := range affectedRoutes {
		c.logger.Info("Triggering reconciliation for affected AIGatewayRoute",
			"route_namespace", route.Namespace, "route_name", route.Name,
			"grant_namespace", req.Namespace, "grant_name", req.Name)
		c.aiGatewayRouteChan <- event.GenericEvent{Object: route}
	}

	// Get all MCPRoutes that might be affected by this ReferenceGrant.
	affectedMCPRoutes, err := c.getAffectedMCPRoutes(ctx, targetNamespace)
	if err != nil {
		c.logger.Error(err, "failed to get affected MCPRoutes",
			"namespace", req.Namespace, "name", req.Name)
		return ctrl.Result{}, err
	}

	// Trigger reconciliation for each affected MCPRoute.
	for _, route := range affectedMCPRoutes {
		c.logger.Info("Triggering reconciliation for affected MCPRoute",
			"route_namespace", route.Namespace, "route_name", route.Name,
			"grant_namespace", req.Namespace, "grant_name", req.Name)
		c.mcpRouteChan <- event.GenericEvent{Object: route}
	}

	return reconcile.Result{}, nil
}

// getAffectedAIGatewayRoutes returns all AIGatewayRoutes that reference targetNamespace, and are
// therefore affected by a ReferenceGrant in that namespace being created, updated, or deleted.
func (c *ReferenceGrantController) getAffectedAIGatewayRoutes(
	ctx context.Context,
	targetNamespace string,
) ([]*aigv1b1.AIGatewayRoute, error) {
	var routes aigv1b1.AIGatewayRouteList
	if err := c.client.List(ctx, &routes); err != nil {
		return nil, fmt.Errorf("failed to list AIGatewayRoutes: %w", err)
	}

	var affectedRoutes []*aigv1b1.AIGatewayRoute
	for i := range routes.Items {
		route := &routes.Items[i]
		if c.aiGatewayRouteReferencesNamespace(route, targetNamespace) {
			affectedRoutes = append(affectedRoutes, route)
		}
	}

	return affectedRoutes, nil
}

// getAffectedMCPRoutes returns all MCPRoutes that reference targetNamespace, either for a backend or
// for a credential Secret, and are therefore affected by a ReferenceGrant change in that namespace.
func (c *ReferenceGrantController) getAffectedMCPRoutes(
	ctx context.Context,
	targetNamespace string,
) ([]*aigv1b1.MCPRoute, error) {
	var routes aigv1b1.MCPRouteList
	if err := c.client.List(ctx, &routes); err != nil {
		return nil, fmt.Errorf("failed to list MCPRoutes: %w", err)
	}

	var affectedRoutes []*aigv1b1.MCPRoute
	for i := range routes.Items {
		route := &routes.Items[i]
		if c.mcpRouteReferencesNamespace(route, targetNamespace) {
			affectedRoutes = append(affectedRoutes, route)
		}
	}

	return affectedRoutes, nil
}

// mcpRouteReferencesNamespace checks if an MCPRoute has any backend or credential Secret reference
// to a specific namespace.
func (c *ReferenceGrantController) mcpRouteReferencesNamespace(route *aigv1b1.MCPRoute, namespace string) bool {
	// Same-namespace references never need a grant, so a grant here cannot affect this route.
	if route.Namespace == namespace {
		return false
	}
	for i := range route.Spec.BackendRefs {
		ref := &route.Spec.BackendRefs[i]
		if ref.Namespace != nil && string(*ref.Namespace) == namespace {
			return true
		}
		if ref.SecurityPolicy == nil || ref.SecurityPolicy.APIKey == nil || ref.SecurityPolicy.APIKey.SecretRef == nil {
			continue
		}
		if secretNs := ref.SecurityPolicy.APIKey.SecretRef.Namespace; secretNs != nil && string(*secretNs) == namespace {
			return true
		}
	}
	return false
}

// aiGatewayRouteReferencesNamespace checks if an AIGatewayRoute has any backend references to a specific namespace.
func (c *ReferenceGrantController) aiGatewayRouteReferencesNamespace(route *aigv1b1.AIGatewayRoute, namespace string) bool {
	// Same-namespace references never need a grant, so a grant here cannot affect this route.
	if route.Namespace == namespace {
		return false
	}
	for _, rule := range route.Spec.Rules {
		for _, backendRef := range rule.BackendRefs {
			// Only check AIServiceBackend references
			if backendRef.IsAIServiceBackend() {
				backendNs := backendRef.GetNamespace(route.Namespace)
				if backendNs == namespace {
					return true
				}
			}
		}
	}
	return false
}
