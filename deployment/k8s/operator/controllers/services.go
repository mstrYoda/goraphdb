package controllers

// ============================================================================
// Service Topology
//
// The operator creates three Kubernetes Services for each GoraphDB cluster,
// each serving a different purpose in the architecture:
//
// ┌─────────────────────────────────────────────────────────────────────┐
// │                     Service Topology                                │
// │                                                                     │
// │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │
// │  │  {name}-headless  │  │  {name}-client   │  │  {name}-read     │  │
// │  │  (Headless/None)  │  │  (ClusterIP)     │  │  (ClusterIP)     │  │
// │  │                   │  │                   │  │                   │  │
// │  │  Purpose:         │  │  Purpose:         │  │  Purpose:         │  │
// │  │  Peer discovery   │  │  Read+Write to    │  │  Read-only from   │  │
// │  │  for Raft + gRPC  │  │  leader only      │  │  any replica      │  │
// │  │                   │  │                   │  │                   │  │
// │  │  Selector:        │  │  Selector:        │  │  Selector:        │  │
// │  │  name + instance  │  │  name + instance  │  │  name + instance  │  │
// │  │                   │  │  + role=leader    │  │                   │  │
// │  │                   │  │                   │  │                   │  │
// │  │  Ports:           │  │  Ports:           │  │  Ports:           │  │
// │  │  HTTP, Raft, gRPC │  │  HTTP only        │  │  HTTP only        │  │
// │  └──────────────────┘  └──────────────────┘  └──────────────────┘  │
// │         │                       │                       │           │
// │         ▼                       ▼                       ▼           │
// │  ┌──────────┐            ┌──────────┐           All ready pods     │
// │  │ All pods │            │ Leader   │                              │
// │  │ (DNS)    │            │ pod only │                              │
// │  └──────────┘            └──────────┘                              │
// └─────────────────────────────────────────────────────────────────────┘
//
// Service 1: Headless Service ({name}-headless)
//
//   clusterIP: None — creates DNS A records for each pod:
//     goraphdb-ha-0.goraphdb-ha-headless.ns.svc.cluster.local
//     goraphdb-ha-1.goraphdb-ha-headless.ns.svc.cluster.local
//     goraphdb-ha-2.goraphdb-ha-headless.ns.svc.cluster.local
//
//   Used by:
//     - StatefulSet (required: spec.serviceName must reference a headless svc)
//     - Entrypoint script: builds -raft-addr and -grpc-addr peer list from DNS
//     - Raft transport: peers connect to each other via these DNS names
//     - gRPC replication: followers connect to leader's gRPC address via DNS
//
//   Exposes: HTTP (7474), Raft (7000), gRPC (7001)
//
// Service 2: Client Service ({name}-client)
//
//   clusterIP: auto-assigned — provides a single stable endpoint for clients.
//   Selector includes: goraphdb.io/role=leader
//
//   This ensures all client traffic (reads AND writes) goes to the leader.
//   This is the simplest routing strategy. For read scaling, use the
//   read Service instead.
//
//   Design decision: using label-based routing instead of an external
//   load balancer or sidecar proxy. The operator continuously updates
//   the goraphdb.io/role label on pods based on /api/cluster status,
//   so Kubernetes endpoints controller automatically routes to the leader.
//
//   Used by: application clients connecting to GoraphDB
//   Exposes: HTTP (7474) only
//
// Service 3: Read Service ({name}-read)
//
//   clusterIP: auto-assigned — load-balances across all ready replicas.
//   No role selector — any healthy pod can serve reads.
//
//   GoraphDB followers maintain a full copy of the data via WAL replication,
//   so reads are always consistent (within replication lag, typically <10ms).
//
//   Used by: applications that need read scaling or can tolerate slight lag
//   Exposes: HTTP (7474) only
//
// Standalone mode (replicas=1):
//
//   Only the headless Service and a single client Service are created.
//   The read Service is skipped (no point load-balancing across 1 pod).
//   The client Service does NOT use role selector (no Raft in standalone).
//
// Reference:
//   - server/server.go: handleHealth() returns role for routing decisions
//   - server/server.go: handleClusterStatus() for replication state
//   - replication/cluster.go: ClusterManager manages role transitions
// ============================================================================

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	goraphdbv1alpha1 "github.com/mstrYoda/goraphdb/operator/api/v1alpha1"
)

// reconcileServices creates or updates the Services for the GoraphDB cluster.
func (r *GoraphDBClusterReconciler) reconcileServices(ctx context.Context, cluster *goraphdbv1alpha1.GoraphDBCluster) error {
	// Always create the headless Service (required by StatefulSet).
	if err := r.reconcileHeadlessService(ctx, cluster); err != nil {
		return err
	}

	// Always create the client Service.
	if err := r.reconcileClientService(ctx, cluster); err != nil {
		return err
	}

	// Create read Service only in cluster mode (replicas > 1).
	if *cluster.Spec.Replicas > 1 {
		if err := r.reconcileReadService(ctx, cluster); err != nil {
			return err
		}
	}

	return nil
}

// reconcileHeadlessService creates the headless Service for peer discovery.
//
// This Service is required by the StatefulSet (spec.serviceName) and provides
// stable DNS names for each pod. The entrypoint script uses these DNS names
// to construct the -raft-addr, -grpc-addr, and -peers CLI flags.
//
// DNS format: {pod-name}.{service-name}.{namespace}.svc.cluster.local
// Example:    goraphdb-ha-0.goraphdb-ha-headless.default.svc.cluster.local
func (r *GoraphDBClusterReconciler) reconcileHeadlessService(ctx context.Context, cluster *goraphdbv1alpha1.GoraphDBCluster) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      headlessServiceName(cluster),
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(cluster, svc, r.Scheme); err != nil {
			return err
		}

		labels := commonLabels(cluster)
		labels[LabelComponent] = "headless"
		svc.Labels = labels

		svc.Spec = corev1.ServiceSpec{
			// ClusterIP: None makes this a headless Service.
			// Kubernetes creates DNS A records for each matching pod.
			ClusterIP: "None",
			Selector:  selectorLabels(cluster),
			// publishNotReadyAddresses ensures DNS records exist even for
			// pods that haven't passed readiness checks yet. This is crucial
			// for bootstrap: new pods need to resolve peer DNS names before
			// they're healthy (chicken-and-egg: Raft needs peers to elect
			// a leader, but the health check requires a leader to return "ok").
			PublishNotReadyAddresses: true,
			Ports:                   r.headlessServicePorts(cluster),
		}
		return nil
	})
	return err
}

// reconcileClientService creates the leader-only client Service.
//
// In cluster mode, this Service uses goraphdb.io/role=leader selector
// to route all client traffic to the current Raft leader. The operator
// updates pod labels based on /api/cluster response (see failover.go).
//
// In standalone mode, no role selector is used (single pod is always the target).
func (r *GoraphDBClusterReconciler) reconcileClientService(ctx context.Context, cluster *goraphdbv1alpha1.GoraphDBCluster) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clientServiceName(cluster),
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(cluster, svc, r.Scheme); err != nil {
			return err
		}

		labels := commonLabels(cluster)
		labels[LabelComponent] = "client"
		svc.Labels = labels

		// Build selector: base labels + role=leader (cluster mode only).
		selector := selectorLabels(cluster)
		if *cluster.Spec.Replicas > 1 {
			selector[LabelRole] = "leader"
		}

		// Preserve the existing clusterIP on update. Kubernetes assigns a
		// clusterIP on creation and it becomes immutable — overwriting the
		// entire Spec would clear it, causing an API validation error:
		//   "spec.clusterIPs[0]: Invalid value: primary clusterIP can not be unset"
		existingClusterIP := svc.Spec.ClusterIP

		svc.Spec.Type = corev1.ServiceTypeClusterIP
		svc.Spec.Selector = selector
		svc.Spec.Ports = []corev1.ServicePort{
			{
				Name:     "http",
				Port:     cluster.Spec.Ports.HTTP,
				Protocol: corev1.ProtocolTCP,
			},
		}

		if existingClusterIP != "" {
			svc.Spec.ClusterIP = existingClusterIP
		}
		return nil
	})
	return err
}

// reconcileReadService creates the read-replica Service.
//
// This Service routes to ALL ready pods regardless of role.
// GoraphDB followers maintain a full data copy via WAL replication,
// so reads are consistent (within replication lag).
//
// Clients that need read scaling or can tolerate slight lag should
// connect to this Service instead of the client Service.
//
// Note: followers also support write queries by transparently forwarding
// them to the leader via the Router (replication/router.go). So this
// Service technically supports both reads and writes, but writes incur
// an extra hop (follower → leader → response → follower → client).
func (r *GoraphDBClusterReconciler) reconcileReadService(ctx context.Context, cluster *goraphdbv1alpha1.GoraphDBCluster) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      readServiceName(cluster),
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(cluster, svc, r.Scheme); err != nil {
			return err
		}

		labels := commonLabels(cluster)
		labels[LabelComponent] = "read"
		svc.Labels = labels

		// Preserve existing clusterIP (same reason as client Service).
		existingClusterIP := svc.Spec.ClusterIP

		svc.Spec.Type = corev1.ServiceTypeClusterIP
		// No role selector — routes to all ready pods.
		svc.Spec.Selector = selectorLabels(cluster)
		svc.Spec.Ports = []corev1.ServicePort{
			{
				Name:     "http",
				Port:     cluster.Spec.Ports.HTTP,
				Protocol: corev1.ProtocolTCP,
			},
		}

		if existingClusterIP != "" {
			svc.Spec.ClusterIP = existingClusterIP
		}
		return nil
	})
	return err
}

// headlessServicePorts returns the port definitions for the headless Service.
func (r *GoraphDBClusterReconciler) headlessServicePorts(cluster *goraphdbv1alpha1.GoraphDBCluster) []corev1.ServicePort {
	ports := []corev1.ServicePort{
		{
			Name:     "http",
			Port:     cluster.Spec.Ports.HTTP,
			Protocol: corev1.ProtocolTCP,
		},
	}

	// Raft and gRPC ports only in cluster mode.
	if *cluster.Spec.Replicas > 1 {
		ports = append(ports,
			corev1.ServicePort{
				Name:     "raft",
				Port:     cluster.Spec.Ports.Raft,
				Protocol: corev1.ProtocolTCP,
			},
			corev1.ServicePort{
				Name:     "grpc",
				Port:     cluster.Spec.Ports.GRPC,
				Protocol: corev1.ProtocolTCP,
			},
		)
	}

	return ports
}
