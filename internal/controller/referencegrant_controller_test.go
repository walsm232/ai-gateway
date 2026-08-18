// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package controller

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
	gwapiv1b1 "sigs.k8s.io/gateway-api/apis/v1beta1"

	aigv1b1 "github.com/envoyproxy/ai-gateway/api/v1beta1"
)

func TestReferenceGrantController_Reconcile(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = gwapiv1b1.Install(scheme)
	_ = aigv1b1.AddToScheme(scheme)

	t.Run("ReferenceGrant created - triggers affected AIGatewayRoutes", func(t *testing.T) {
		referenceGrant := &gwapiv1b1.ReferenceGrant{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-grant",
				Namespace: "backend-ns",
			},
			Spec: gwapiv1b1.ReferenceGrantSpec{
				From: []gwapiv1b1.ReferenceGrantFrom{
					{
						Group:     aiServiceBackendGroup,
						Kind:      aiGatewayRouteKind,
						Namespace: "route-ns",
					},
				},
				To: []gwapiv1b1.ReferenceGrantTo{
					{
						Group: aiServiceBackendGroup,
						Kind:  aiServiceBackendKind,
					},
				},
			},
		}

		affectedRoute := &aigv1b1.AIGatewayRoute{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "affected-route",
				Namespace: "route-ns",
			},
			Spec: aigv1b1.AIGatewayRouteSpec{
				Rules: []aigv1b1.AIGatewayRouteRule{
					{
						BackendRefs: []aigv1b1.AIGatewayRouteRuleBackendRef{
							{
								Name:      "backend",
								Namespace: ptr.To(gwapiv1.Namespace("backend-ns")),
							},
						},
					},
				},
			},
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(referenceGrant, affectedRoute).
			Build()

		// Create a buffered channel to avoid blocking
		aiGatewayRouteChan := make(chan event.GenericEvent, 10)
		logger := logr.Discard()

		controller := NewReferenceGrantController(fakeClient, logger, aiGatewayRouteChan, make(chan event.GenericEvent, 10))

		req := reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(referenceGrant),
		}

		result, err := controller.Reconcile(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, reconcile.Result{}, result)

		// Verify that an event was sent to the channel
		require.Len(t, aiGatewayRouteChan, 1)
		event := <-aiGatewayRouteChan
		require.Equal(t, affectedRoute.Name, event.Object.GetName())
		require.Equal(t, affectedRoute.Namespace, event.Object.GetNamespace())
	})

	t.Run("ReferenceGrant deleted - reconciles successfully", func(t *testing.T) {
		// When a ReferenceGrant is deleted, it doesn't exist in the cluster
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			Build()

		aiGatewayRouteChan := make(chan event.GenericEvent, 10)
		logger := logr.Discard()

		controller := NewReferenceGrantController(fakeClient, logger, aiGatewayRouteChan, make(chan event.GenericEvent, 10))

		req := reconcile.Request{
			NamespacedName: client.ObjectKey{
				Namespace: "backend-ns",
				Name:      "deleted-grant",
			},
		}

		result, err := controller.Reconcile(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, reconcile.Result{}, result)

		// No events should be sent when grant is deleted
		require.Empty(t, aiGatewayRouteChan)
	})

	t.Run("ReferenceGrant with no affected routes", func(t *testing.T) {
		referenceGrant := &gwapiv1b1.ReferenceGrant{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-grant",
				Namespace: "backend-ns",
			},
			Spec: gwapiv1b1.ReferenceGrantSpec{
				From: []gwapiv1b1.ReferenceGrantFrom{
					{
						Group:     aiServiceBackendGroup,
						Kind:      aiGatewayRouteKind,
						Namespace: "route-ns",
					},
				},
				To: []gwapiv1b1.ReferenceGrantTo{
					{
						Group: aiServiceBackendGroup,
						Kind:  aiServiceBackendKind,
					},
				},
			},
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(referenceGrant).
			Build()

		aiGatewayRouteChan := make(chan event.GenericEvent, 10)
		logger := logr.Discard()

		controller := NewReferenceGrantController(fakeClient, logger, aiGatewayRouteChan, make(chan event.GenericEvent, 10))

		req := reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(referenceGrant),
		}

		result, err := controller.Reconcile(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, reconcile.Result{}, result)

		// No events should be sent when there are no affected routes
		require.Empty(t, aiGatewayRouteChan)
	})

	t.Run("ReferenceGrant with multiple affected routes", func(t *testing.T) {
		referenceGrant := &gwapiv1b1.ReferenceGrant{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-grant",
				Namespace: "backend-ns",
			},
			Spec: gwapiv1b1.ReferenceGrantSpec{
				From: []gwapiv1b1.ReferenceGrantFrom{
					{
						Group:     aiServiceBackendGroup,
						Kind:      aiGatewayRouteKind,
						Namespace: "route-ns",
					},
				},
				To: []gwapiv1b1.ReferenceGrantTo{
					{
						Group: aiServiceBackendGroup,
						Kind:  aiServiceBackendKind,
					},
				},
			},
		}

		route1 := &aigv1b1.AIGatewayRoute{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "route-1",
				Namespace: "route-ns",
			},
			Spec: aigv1b1.AIGatewayRouteSpec{
				Rules: []aigv1b1.AIGatewayRouteRule{
					{
						BackendRefs: []aigv1b1.AIGatewayRouteRuleBackendRef{
							{
								Name:      "backend-1",
								Namespace: ptr.To(gwapiv1.Namespace("backend-ns")),
							},
						},
					},
				},
			},
		}

		route2 := &aigv1b1.AIGatewayRoute{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "route-2",
				Namespace: "route-ns",
			},
			Spec: aigv1b1.AIGatewayRouteSpec{
				Rules: []aigv1b1.AIGatewayRouteRule{
					{
						BackendRefs: []aigv1b1.AIGatewayRouteRuleBackendRef{
							{
								Name:      "backend-2",
								Namespace: ptr.To(gwapiv1.Namespace("backend-ns")),
							},
						},
					},
				},
			},
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(referenceGrant, route1, route2).
			Build()

		aiGatewayRouteChan := make(chan event.GenericEvent, 10)
		logger := logr.Discard()

		controller := NewReferenceGrantController(fakeClient, logger, aiGatewayRouteChan, make(chan event.GenericEvent, 10))

		req := reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(referenceGrant),
		}

		result, err := controller.Reconcile(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, reconcile.Result{}, result)

		// Both routes should trigger events
		require.Len(t, aiGatewayRouteChan, 2)

		// Collect route names from events
		routeNames := make(map[string]bool)
		event1 := <-aiGatewayRouteChan
		routeNames[event1.Object.GetName()] = true
		event2 := <-aiGatewayRouteChan
		routeNames[event2.Object.GetName()] = true

		require.True(t, routeNames["route-1"])
		require.True(t, routeNames["route-2"])
	})
}

func TestNewReferenceGrantController(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = gwapiv1b1.Install(scheme)
	_ = aigv1b1.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	aiGatewayRouteChan := make(chan event.GenericEvent, 10)
	logger := logr.Discard()

	controller := NewReferenceGrantController(fakeClient, logger, aiGatewayRouteChan, make(chan event.GenericEvent, 10))

	require.NotNil(t, controller)
	require.Equal(t, fakeClient, controller.client)
	require.Equal(t, logger, controller.logger)
	require.Equal(t, aiGatewayRouteChan, controller.aiGatewayRouteChan)
}

// TestReferenceGrantController_Reconcile_GetError tests reconcile when Get returns error
func TestReferenceGrantController_Reconcile_GetError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = gwapiv1b1.Install(scheme)
	_ = aigv1b1.AddToScheme(scheme)

	// Create a fake client that will return an error for Get operations
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	aiGatewayRouteChan := make(chan event.GenericEvent, 10)
	logger := logr.Discard()

	controller := NewReferenceGrantController(fakeClient, logger, aiGatewayRouteChan, make(chan event.GenericEvent, 10))

	// Try to reconcile a non-existent ReferenceGrant - this should be handled gracefully
	req := reconcile.Request{
		NamespacedName: client.ObjectKey{
			Namespace: "test-ns",
			Name:      "non-existent",
		},
	}

	result, err := controller.Reconcile(context.Background(), req)
	require.NoError(t, err, "should ignore not found errors")
	require.Equal(t, reconcile.Result{}, result)
}

// TestReferenceGrantController_Reconcile_GetAffectedRoutesError tests when GetAffectedAIGatewayRoutes returns an error
func TestReferenceGrantController_Reconcile_GetAffectedRoutesError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = gwapiv1b1.Install(scheme)

	referenceGrant := &gwapiv1b1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-grant",
			Namespace: "backend-ns",
		},
		Spec: gwapiv1b1.ReferenceGrantSpec{
			From: []gwapiv1b1.ReferenceGrantFrom{
				{
					Group:     aiServiceBackendGroup,
					Kind:      aiGatewayRouteKind,
					Namespace: "route-ns",
				},
			},
			To: []gwapiv1b1.ReferenceGrantTo{
				{
					Group: aiServiceBackendGroup,
					Kind:  aiServiceBackendKind,
				},
			},
		},
	}

	// Create fake client without AIGatewayRoute in scheme to cause List error
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(referenceGrant).
		Build()

	aiGatewayRouteChan := make(chan event.GenericEvent, 10)
	logger := logr.Discard()

	controller := NewReferenceGrantController(fakeClient, logger, aiGatewayRouteChan, make(chan event.GenericEvent, 10))

	req := reconcile.Request{
		NamespacedName: client.ObjectKeyFromObject(referenceGrant),
	}

	result, err := controller.Reconcile(context.Background(), req)
	require.Error(t, err, "should return error when GetAffectedAIGatewayRoutes fails")
	require.Equal(t, reconcile.Result{}, result)
}

func TestReferenceGrantController_GetAffectedAIGatewayRoutes(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = gwapiv1b1.Install(scheme)
	_ = aigv1b1.AddToScheme(scheme)

	tests := []struct {
		name           string
		referenceGrant gwapiv1b1.ReferenceGrant
		routes         []aigv1b1.AIGatewayRoute
		expectedRoutes []string // route names that should be affected
	}{
		{
			name: "Grant with route referencing backend in grant namespace",
			referenceGrant: gwapiv1b1.ReferenceGrant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-grant",
					Namespace: "backend-ns",
				},
				Spec: gwapiv1b1.ReferenceGrantSpec{
					From: []gwapiv1b1.ReferenceGrantFrom{
						{
							Group:     aiServiceBackendGroup,
							Kind:      aiGatewayRouteKind,
							Namespace: "route-ns",
						},
					},
					To: []gwapiv1b1.ReferenceGrantTo{
						{
							Group: aiServiceBackendGroup,
							Kind:  aiServiceBackendKind,
						},
					},
				},
			},
			routes: []aigv1b1.AIGatewayRoute{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "affected-route",
						Namespace: "route-ns",
					},
					Spec: aigv1b1.AIGatewayRouteSpec{
						Rules: []aigv1b1.AIGatewayRouteRule{
							{
								BackendRefs: []aigv1b1.AIGatewayRouteRuleBackendRef{
									{
										Name:      "backend",
										Namespace: ptr.To(gwapiv1.Namespace("backend-ns")),
									},
								},
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "unaffected-route",
						Namespace: "route-ns",
					},
					Spec: aigv1b1.AIGatewayRouteSpec{
						Rules: []aigv1b1.AIGatewayRouteRule{
							{
								BackendRefs: []aigv1b1.AIGatewayRouteRuleBackendRef{
									{
										Name: "local-backend",
										// No namespace specified, uses local namespace
									},
								},
							},
						},
					},
				},
			},
			expectedRoutes: []string{"affected-route"},
		},
		{
			// InferencePool backends are validated against a ReferenceGrant just like AIServiceBackend
			// ones, so a grant in the pool's namespace must reconcile the route referencing it.
			name: "Grant with route referencing InferencePool in grant namespace",
			referenceGrant: gwapiv1b1.ReferenceGrant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-grant",
					Namespace: "pool-ns",
				},
				Spec: gwapiv1b1.ReferenceGrantSpec{
					From: []gwapiv1b1.ReferenceGrantFrom{
						{
							Group:     aiServiceBackendGroup,
							Kind:      aiGatewayRouteKind,
							Namespace: "route-ns",
						},
					},
					To: []gwapiv1b1.ReferenceGrantTo{
						{
							Group: inferencePoolGroup,
							Kind:  inferencePoolKind,
						},
					},
				},
			},
			routes: []aigv1b1.AIGatewayRoute{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "inference-pool-route",
						Namespace: "route-ns",
					},
					Spec: aigv1b1.AIGatewayRouteSpec{
						Rules: []aigv1b1.AIGatewayRouteRule{
							{
								BackendRefs: []aigv1b1.AIGatewayRouteRuleBackendRef{
									{
										Name:      "pool",
										Namespace: ptr.To(gwapiv1.Namespace("pool-ns")),
										Group:     ptr.To(inferencePoolGroup),
										Kind:      ptr.To(inferencePoolKind),
									},
								},
							},
						},
					},
				},
			},
			expectedRoutes: []string{"inference-pool-route"},
		},
		{
			// The route sits outside the namespace the grant's "from" names, but still references the
			// grant's namespace, so it is reconciled: another grant there may be what authorizes it.
			name: "Route in another namespace referencing the grant namespace",
			referenceGrant: gwapiv1b1.ReferenceGrant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-grant",
					Namespace: "backend-ns",
				},
				Spec: gwapiv1b1.ReferenceGrantSpec{
					From: []gwapiv1b1.ReferenceGrantFrom{
						{
							Group:     aiServiceBackendGroup,
							Kind:      aiGatewayRouteKind,
							Namespace: "route-ns",
						},
					},
					To: []gwapiv1b1.ReferenceGrantTo{
						{
							Group: aiServiceBackendGroup,
							Kind:  aiServiceBackendKind,
						},
					},
				},
			},
			routes: []aigv1b1.AIGatewayRoute{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "route-in-different-ns",
						Namespace: "other-ns",
					},
					Spec: aigv1b1.AIGatewayRouteSpec{
						Rules: []aigv1b1.AIGatewayRouteRule{
							{
								BackendRefs: []aigv1b1.AIGatewayRouteRuleBackendRef{
									{
										Name:      "backend",
										Namespace: ptr.To(gwapiv1.Namespace("backend-ns")),
									},
								},
							},
						},
					},
				},
			},
			expectedRoutes: []string{"route-in-different-ns"},
		},
		{
			// The grant no longer names AIGatewayRoute, which is exactly the revocation case: the
			// route must still be reconciled so it can report that it is no longer authorized.
			name: "Grant naming a different kind",
			referenceGrant: gwapiv1b1.ReferenceGrant{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-grant",
					Namespace: "backend-ns",
				},
				Spec: gwapiv1b1.ReferenceGrantSpec{
					From: []gwapiv1b1.ReferenceGrantFrom{
						{
							Group:     aiServiceBackendGroup,
							Kind:      "WrongKind",
							Namespace: "route-ns",
						},
					},
					To: []gwapiv1b1.ReferenceGrantTo{
						{
							Group: aiServiceBackendGroup,
							Kind:  aiServiceBackendKind,
						},
					},
				},
			},
			routes: []aigv1b1.AIGatewayRoute{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "route",
						Namespace: "route-ns",
					},
					Spec: aigv1b1.AIGatewayRouteSpec{
						Rules: []aigv1b1.AIGatewayRouteRule{
							{
								BackendRefs: []aigv1b1.AIGatewayRouteRuleBackendRef{
									{
										Name:      "backend",
										Namespace: ptr.To(gwapiv1.Namespace("backend-ns")),
									},
								},
							},
						},
					},
				},
			},
			expectedRoutes: []string{"route"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fake client with routes
			objs := make([]client.Object, len(tt.routes))
			for i := range tt.routes {
				objs[i] = &tt.routes[i]
			}
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				Build()

			aiGatewayRouteChan := make(chan event.GenericEvent, 10)
			logger := logr.Discard()
			controller := NewReferenceGrantController(fakeClient, logger, aiGatewayRouteChan, make(chan event.GenericEvent, 10))

			affectedRoutes, err := controller.getAffectedAIGatewayRoutes(
				context.Background(),
				tt.referenceGrant.Namespace,
			)
			require.NoError(t, err)

			actualRouteNames := make([]string, len(affectedRoutes))
			for i, route := range affectedRoutes {
				actualRouteNames[i] = route.Name
			}

			require.ElementsMatch(t, tt.expectedRoutes, actualRouteNames)
		})
	}

	// Test case where List returns an error
	t.Run("List AIGatewayRoutes error", func(t *testing.T) {
		// Create a scheme without AIGatewayRoute to cause List error
		badScheme := runtime.NewScheme()
		_ = gwapiv1b1.Install(badScheme)
		fakeClient := fake.NewClientBuilder().
			WithScheme(badScheme).
			Build()

		aiGatewayRouteChan := make(chan event.GenericEvent, 10)
		logger := logr.Discard()
		controller := NewReferenceGrantController(fakeClient, logger, aiGatewayRouteChan, make(chan event.GenericEvent, 10))

		routes, err := controller.getAffectedAIGatewayRoutes(context.Background(), "backend-ns")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to list AIGatewayRoutes")
		require.Nil(t, routes)
	})
}

// TestReferenceGrantController_Reconcile_GrantRevoked asserts that routes are reconciled when the
// grant authorizing them is deleted, or narrowed so that it no longer names them. Neither case can be
// resolved from the grant's own "from" entries, so the lookup is by referenced namespace.
func TestReferenceGrantController_Reconcile_GrantRevoked(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = gwapiv1b1.Install(scheme)
	_ = aigv1b1.AddToScheme(scheme)

	aiGatewayRoute := &aigv1b1.AIGatewayRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "ai-route", Namespace: "route-ns"},
		Spec: aigv1b1.AIGatewayRouteSpec{Rules: []aigv1b1.AIGatewayRouteRule{{
			BackendRefs: []aigv1b1.AIGatewayRouteRuleBackendRef{
				{Name: "backend", Namespace: ptr.To(gwapiv1.Namespace("backend-ns"))},
			},
		}}},
	}
	mcpRoute := &aigv1b1.MCPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-route", Namespace: "route-ns"},
		Spec: aigv1b1.MCPRouteSpec{BackendRefs: []aigv1b1.MCPRouteBackendRef{{
			BackendObjectReference: gwapiv1.BackendObjectReference{
				Name:      "svc-a",
				Namespace: ptr.To(gwapiv1.Namespace("backend-ns")),
			},
		}}},
	}

	t.Run("grant deleted", func(t *testing.T) {
		// The grant itself is absent, as it would be after a delete.
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(aiGatewayRoute, mcpRoute).Build()
		aiGatewayRouteChan := make(chan event.GenericEvent, 10)
		mcpRouteChan := make(chan event.GenericEvent, 10)
		controller := NewReferenceGrantController(fakeClient, logr.Discard(), aiGatewayRouteChan, mcpRouteChan)

		_, err := controller.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: client.ObjectKey{Namespace: "backend-ns", Name: "deleted-grant"},
		})
		require.NoError(t, err)
		require.Len(t, aiGatewayRouteChan, 1)
		require.Len(t, mcpRouteChan, 1)
	})

	t.Run("grant narrowed to another namespace", func(t *testing.T) {
		// The grant still exists but no longer names route-ns.
		grant := &gwapiv1b1.ReferenceGrant{
			ObjectMeta: metav1.ObjectMeta{Name: "test-grant", Namespace: "backend-ns"},
			Spec: gwapiv1b1.ReferenceGrantSpec{
				From: []gwapiv1b1.ReferenceGrantFrom{
					{Group: aiServiceBackendGroup, Kind: mcpRouteKind, Namespace: "other-ns"},
				},
				To: []gwapiv1b1.ReferenceGrantTo{{Group: coreGroup, Kind: serviceKind}},
			},
		}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(grant, aiGatewayRoute, mcpRoute).Build()
		aiGatewayRouteChan := make(chan event.GenericEvent, 10)
		mcpRouteChan := make(chan event.GenericEvent, 10)
		controller := NewReferenceGrantController(fakeClient, logr.Discard(), aiGatewayRouteChan, mcpRouteChan)

		_, err := controller.Reconcile(context.Background(), reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(grant),
		})
		require.NoError(t, err)
		require.Len(t, aiGatewayRouteChan, 1)
		require.Len(t, mcpRouteChan, 1)
	})
}

func TestReferenceGrantController_Reconcile_MCPRoutes(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = gwapiv1b1.Install(scheme)
	_ = aigv1b1.AddToScheme(scheme)

	mcpRoute := func(name string, ref aigv1b1.MCPRouteBackendRef) *aigv1b1.MCPRoute {
		return &aigv1b1.MCPRoute{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "route-ns"},
			Spec:       aigv1b1.MCPRouteSpec{BackendRefs: []aigv1b1.MCPRouteBackendRef{ref}},
		}
	}

	// Referenced via backendRef.namespace.
	backendRouteA := mcpRoute("backend-route", aigv1b1.MCPRouteBackendRef{
		BackendObjectReference: gwapiv1.BackendObjectReference{
			Name:      "svc-a",
			Namespace: ptr.To(gwapiv1.Namespace("backend-ns")),
		},
	})
	// Referenced via the credential secretRef.namespace only.
	secretRoute := mcpRoute("secret-route", aigv1b1.MCPRouteBackendRef{
		BackendObjectReference: gwapiv1.BackendObjectReference{Name: "svc-b"},
		SecurityPolicy: &aigv1b1.MCPBackendSecurityPolicy{
			APIKey: &aigv1b1.MCPBackendAPIKey{SecretRef: &gwapiv1.SecretObjectReference{
				Name:      "tenant-secret",
				Namespace: ptr.To(gwapiv1.Namespace("backend-ns")),
			}},
		},
	})
	// References a different namespace entirely.
	unrelatedRoute := mcpRoute("unrelated-route", aigv1b1.MCPRouteBackendRef{
		BackendObjectReference: gwapiv1.BackendObjectReference{
			Name:      "svc-c",
			Namespace: ptr.To(gwapiv1.Namespace("other-ns")),
		},
	})

	grant := &gwapiv1b1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "test-grant", Namespace: "backend-ns"},
		Spec: gwapiv1b1.ReferenceGrantSpec{
			From: []gwapiv1b1.ReferenceGrantFrom{
				{Group: aiServiceBackendGroup, Kind: mcpRouteKind, Namespace: "route-ns"},
			},
			To: []gwapiv1b1.ReferenceGrantTo{{Group: coreGroup, Kind: serviceKind}},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(grant, backendRouteA, secretRoute, unrelatedRoute).
		Build()

	aiGatewayRouteChan := make(chan event.GenericEvent, 10)
	mcpRouteChan := make(chan event.GenericEvent, 10)
	controller := NewReferenceGrantController(fakeClient, logr.Discard(), aiGatewayRouteChan, mcpRouteChan)

	result, err := controller.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: client.ObjectKeyFromObject(grant),
	})
	require.NoError(t, err)
	require.Equal(t, reconcile.Result{}, result)

	// Both the backendRef and the secretRef routes are affected; the unrelated one is not.
	require.Empty(t, aiGatewayRouteChan)
	require.Len(t, mcpRouteChan, 2)
	var names []string
	for range 2 {
		names = append(names, (<-mcpRouteChan).Object.GetName())
	}
	require.ElementsMatch(t, []string{"backend-route", "secret-route"}, names)
}

func TestReferenceGrantController_getAffectedMCPRoutes_OtherNamespace(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = gwapiv1b1.Install(scheme)
	_ = aigv1b1.AddToScheme(scheme)

	// A route referencing a namespace other than the grant's must not be reconciled.
	route := &aigv1b1.MCPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "mcp-route", Namespace: "route-ns"},
		Spec: aigv1b1.MCPRouteSpec{BackendRefs: []aigv1b1.MCPRouteBackendRef{{
			BackendObjectReference: gwapiv1.BackendObjectReference{
				Name:      "svc-a",
				Namespace: ptr.To(gwapiv1.Namespace("other-ns")),
			},
		}}},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(route).Build()
	controller := NewReferenceGrantController(fakeClient, logr.Discard(),
		make(chan event.GenericEvent, 10), make(chan event.GenericEvent, 10))

	routes, err := controller.getAffectedMCPRoutes(context.Background(), "backend-ns")
	require.NoError(t, err)
	require.Empty(t, routes)
}
