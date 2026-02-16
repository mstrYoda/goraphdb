package controllers

// ============================================================================
// Labels & Selectors
//
// Consistent labeling strategy following Kubernetes recommended labels:
// https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
//
// Labels are used for:
// 1. Selector matching (StatefulSet → Pods, Service → Pods)
// 2. Operator-managed resource identification (owned-by queries)
// 3. Role-based routing (leader vs follower pod labels)
//
// The operator updates the "goraphdb.io/role" label on each pod based on
// the /api/cluster response, enabling the leader-only Service to route
// traffic exclusively to the current Raft leader.
// ============================================================================

import (
	"fmt"

	goraphdbv1alpha1 "github.com/mstrYoda/goraphdb/operator/api/v1alpha1"
)

const (
	// LabelManagedBy identifies the operator as the manager of resources.
	LabelManagedBy = "app.kubernetes.io/managed-by"

	// LabelName is the application name label.
	LabelName = "app.kubernetes.io/name"

	// LabelInstance distinguishes different GoraphDBCluster instances.
	LabelInstance = "app.kubernetes.io/instance"

	// LabelComponent identifies the functional component.
	LabelComponent = "app.kubernetes.io/component"

	// LabelPartOf identifies the higher-level application.
	LabelPartOf = "app.kubernetes.io/part-of"

	// LabelRole is a custom label set by the operator to indicate the
	// current Raft role of a pod: "leader", "follower", or "standalone".
	// This label is dynamically updated by the failover controller and
	// used as a selector by the leader-only client Service.
	//
	// Updated by: controllers/failover.go
	// Used by:    controllers/services.go (client Service selector)
	// Source:     GET /api/health → response.role (server/server.go)
	LabelRole = "goraphdb.io/role"
)

// commonLabels returns the base label set for all resources owned by a cluster.
func commonLabels(cluster *goraphdbv1alpha1.GoraphDBCluster) map[string]string {
	return map[string]string{
		LabelName:      "goraphdb",
		LabelInstance:  cluster.Name,
		LabelManagedBy: "goraphdb-operator",
		LabelPartOf:    "goraphdb",
	}
}

// podLabels returns labels for GoraphDB pods (StatefulSet pod template).
// Includes all common labels plus the component and initial role.
func podLabels(cluster *goraphdbv1alpha1.GoraphDBCluster) map[string]string {
	labels := commonLabels(cluster)
	labels[LabelComponent] = "database"

	// Initial role: standalone for single-node, TBD for cluster.
	// The failover controller will update this dynamically.
	if cluster.Spec.Replicas != nil && *cluster.Spec.Replicas > 1 {
		labels[LabelRole] = "follower" // safe default until election completes
	} else {
		labels[LabelRole] = "standalone"
	}

	return labels
}

// selectorLabels returns the minimal label set for Service → Pod selection.
// Must be immutable after StatefulSet creation (K8s does not allow changing
// the selector of an existing StatefulSet).
func selectorLabels(cluster *goraphdbv1alpha1.GoraphDBCluster) map[string]string {
	return map[string]string{
		LabelName:     "goraphdb",
		LabelInstance: cluster.Name,
	}
}

// headlessServiceName returns the name of the headless Service for peer discovery.
// Pod DNS entries follow the pattern: {pod-name}.{headless-svc}.{namespace}.svc.cluster.local
//
// Used by the entrypoint script to construct -raft-addr and -grpc-addr flags.
func headlessServiceName(cluster *goraphdbv1alpha1.GoraphDBCluster) string {
	return cluster.Name + "-headless"
}

// clientServiceName returns the name of the leader-only client Service.
// This Service uses the LabelRole="leader" selector to route traffic
// only to the current Raft leader.
func clientServiceName(cluster *goraphdbv1alpha1.GoraphDBCluster) string {
	return cluster.Name + "-client"
}

// readServiceName returns the name of the read-replica Service.
// This Service routes to all ready pods regardless of role.
func readServiceName(cluster *goraphdbv1alpha1.GoraphDBCluster) string {
	return cluster.Name + "-read"
}

// configMapName returns the name of the ConfigMap containing the entrypoint script.
func configMapName(cluster *goraphdbv1alpha1.GoraphDBCluster) string {
	return cluster.Name + "-config"
}

// podName returns the expected pod name for a given ordinal index.
// StatefulSet pods are named: {statefulset-name}-{ordinal}
func podName(cluster *goraphdbv1alpha1.GoraphDBCluster, ordinal int) string {
	return fmt.Sprintf("%s-%d", cluster.Name, ordinal)
}
