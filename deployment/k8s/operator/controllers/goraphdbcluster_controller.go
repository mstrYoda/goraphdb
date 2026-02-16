// Package controllers implements the Kubernetes reconciliation logic for
// GoraphDBCluster custom resources.
//
// The reconciliation flow:
//
//  1. Receive GoraphDBCluster CR event (create/update/delete)
//  2. Apply defaults to spec (defaults.go)
//  3. Reconcile ConfigMap (entrypoint script for cluster topology)
//  4. Reconcile StatefulSet (pods with correct flags and volumes)
//  5. Reconcile Services (headless, client, read)
//  6. Reconcile PodDisruptionBudget (protect Raft quorum)
//  7. Check pod health and update status (phase, leader, members)
//  8. Handle leader failover (update pod labels for Service routing)
//
// The controller owns all created resources (via OwnerReference) so that
// deleting the GoraphDBCluster CR cascades to all managed resources.
package controllers

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	goraphdbv1alpha1 "github.com/mstrYoda/goraphdb/operator/api/v1alpha1"
)

// GoraphDBClusterReconciler reconciles GoraphDBCluster custom resources.
//
// It watches GoraphDBCluster objects and creates/updates the underlying
// Kubernetes resources (StatefulSet, Services, ConfigMap, PDB) to match
// the desired state declared in the CR spec.
type GoraphDBClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger
}

// Reconcile is the main reconciliation loop. It is called by controller-runtime
// whenever a GoraphDBCluster resource changes, or when owned resources
// (StatefulSet, Services, etc.) are modified.
//
// The reconciler is idempotent: running it multiple times with the same input
// produces the same output. This is critical for Kubernetes controllers.
func (r *GoraphDBClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("goraphdbcluster", req.NamespacedName)

	// -----------------------------------------------------------------------
	// Step 1: Fetch the GoraphDBCluster CR
	// -----------------------------------------------------------------------

	var cluster goraphdbv1alpha1.GoraphDBCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if errors.IsNotFound(err) {
			// CR was deleted — owned resources will be garbage collected
			// via OwnerReferences (cascade delete).
			log.Info("GoraphDBCluster resource deleted, owned resources will be cleaned up")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// -----------------------------------------------------------------------
	// Step 2: Apply defaults
	// -----------------------------------------------------------------------

	goraphdbv1alpha1.SetDefaults(&cluster.Spec)

	// -----------------------------------------------------------------------
	// Step 3: Reconcile ConfigMap (entrypoint script)
	// -----------------------------------------------------------------------

	if err := r.reconcileConfigMap(ctx, &cluster); err != nil {
		log.Error(err, "failed to reconcile ConfigMap")
		return ctrl.Result{}, err
	}

	// -----------------------------------------------------------------------
	// Step 4: Reconcile StatefulSet
	// -----------------------------------------------------------------------

	if err := r.reconcileStatefulSet(ctx, &cluster); err != nil {
		log.Error(err, "failed to reconcile StatefulSet")
		return ctrl.Result{}, err
	}

	// -----------------------------------------------------------------------
	// Step 5: Reconcile Services
	// -----------------------------------------------------------------------

	if err := r.reconcileServices(ctx, &cluster); err != nil {
		log.Error(err, "failed to reconcile Services")
		return ctrl.Result{}, err
	}

	// -----------------------------------------------------------------------
	// Step 6: Reconcile PodDisruptionBudget (cluster mode only)
	// -----------------------------------------------------------------------

	if *cluster.Spec.Replicas > 1 {
		if err := r.reconcilePDB(ctx, &cluster); err != nil {
			log.Error(err, "failed to reconcile PDB")
			return ctrl.Result{}, err
		}
	}

	// -----------------------------------------------------------------------
	// Step 7: Update cluster status
	// -----------------------------------------------------------------------

	if err := r.updateStatus(ctx, &cluster); err != nil {
		log.Error(err, "failed to update status")
		return ctrl.Result{}, err
	}

	// -----------------------------------------------------------------------
	// Step 8: Check health and handle failover
	// -----------------------------------------------------------------------

	if *cluster.Spec.Replicas > 1 {
		if err := r.reconcileLeaderLabels(ctx, &cluster); err != nil {
			log.Error(err, "failed to reconcile leader labels")
			// Don't return error — this is a best-effort operation.
			// Requeue after a short delay to retry.
		}
	}

	// Requeue after 30s to continuously monitor cluster health.
	// This periodic reconciliation updates:
	//   - Pod role labels (leader/follower)
	//   - Cluster status (phase, members, leader)
	//   - Health conditions
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// reconcileConfigMap creates or updates the ConfigMap containing the
// entrypoint script. This script runs inside each GoraphDB pod and
// computes the correct CLI flags based on the pod's ordinal index
// and the cluster's headless Service DNS.
func (r *GoraphDBClusterReconciler) reconcileConfigMap(ctx context.Context, cluster *goraphdbv1alpha1.GoraphDBCluster) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName(cluster),
			Namespace: cluster.Namespace,
			Labels:    commonLabels(cluster),
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		// Set owner reference for garbage collection.
		if err := controllerutil.SetControllerReference(cluster, cm, r.Scheme); err != nil {
			return err
		}

		cm.Labels = commonLabels(cluster)
		cm.Data = map[string]string{
			"entrypoint.sh": r.buildEntrypointScript(cluster),
		}
		return nil
	})
	return err
}

// buildEntrypointScript generates the shell script that starts GoraphDB
// with the correct flags for its position in the cluster.
//
// The script:
// 1. Extracts the pod ordinal from the hostname (e.g., goraphdb-ha-2 → 2)
// 2. Computes node ID, Raft address, gRPC address from DNS
// 3. Builds the peer list for all other nodes
// 4. Starts GoraphDB in standalone or cluster mode
//
// Key design decision: we use a shell script in a ConfigMap rather than
// an init container because:
//   - It avoids needing a separate image for the init container
//   - All configuration is visible and auditable in the ConfigMap
//   - The script can be updated by the operator without restarting pods
//     (though a restart is needed for new flag values to take effect)
func (r *GoraphDBClusterReconciler) buildEntrypointScript(cluster *goraphdbv1alpha1.GoraphDBCluster) string {
	replicas := *cluster.Spec.Replicas
	headlessSvc := headlessServiceName(cluster)
	httpPort := cluster.Spec.Ports.HTTP
	raftPort := cluster.Spec.Ports.Raft
	grpcPort := cluster.Spec.Ports.GRPC
	shards := *cluster.Spec.ShardCount

	if replicas <= 1 {
		// Standalone mode: no cluster flags needed.
		return fmt.Sprintf(`#!/bin/sh
# GoraphDB Standalone Entrypoint
# Generated by the GoraphDB Operator for single-node deployments.
#
# In standalone mode, GoraphDB runs without:
#   - Raft leader election (no -node-id, -raft-addr)
#   - WAL replication (no -grpc-addr, -peers)
#   - Write forwarding (all writes are local)
#
# This matches running: go run ./cmd/graphdb-ui -db /data/db -shards N
set -e

echo "Starting GoraphDB in standalone mode..."
exec /usr/local/bin/graphdb-ui \
  -db /data/db \
  -addr ":%d" \
  -shards %d
`, httpPort, shards)
	}

	// Cluster mode: compute topology from pod hostname and DNS.
	return fmt.Sprintf(`#!/bin/sh
# GoraphDB Cluster Entrypoint
# Generated by the GoraphDB Operator for multi-node deployments.
#
# This script computes the Raft/gRPC/HTTP addresses for each pod based on:
#   - Pod hostname (set by StatefulSet): {cluster-name}-{ordinal}
#   - Headless Service DNS: {pod}.{headless-svc}.{namespace}.svc.cluster.local
#
# Each pod discovers its peers through deterministic DNS names, avoiding
# the need for any external service discovery mechanism.
#
# Architecture:
#   Pod goraphdb-ha-0 → node-id=goraphdb-ha-0
#     raft-addr = 0.0.0.0:%[3]d (binds all interfaces for Raft transport)
#     grpc-addr = 0.0.0.0:%[4]d (binds all interfaces for WAL streaming)
#     http-addr = http://goraphdb-ha-0.goraphdb-ha-headless.ns.svc:7474
#     peers     = goraphdb-ha-1@...:%[3]d@...:%[4]d@http://...:%[2]d,...
#
# Bootstrap: all initial nodes pass -bootstrap. Hashicorp Raft safely
# ignores bootstrap on already-bootstrapped clusters (returns ErrCantBootstrap).
set -e

# Extract pod ordinal from hostname.
# StatefulSet pods are named: {name}-{ordinal} (e.g., goraphdb-ha-2)
HOSTNAME=$(hostname)
ORDINAL=$(echo "$HOSTNAME" | rev | cut -d'-' -f1 | rev)

# Cluster identification.
CLUSTER_NAME="%[1]s"
HEADLESS_SVC="%[5]s"
NAMESPACE="%[6]s"
REPLICAS=%[7]d

# This node's identity.
NODE_ID="${HOSTNAME}"

# Bind addresses (0.0.0.0 to accept connections from any pod).
RAFT_ADDR="0.0.0.0:%[3]d"
GRPC_ADDR="0.0.0.0:%[4]d"
HTTP_ADDR=":%[2]d"

# Construct DNS-based HTTP address for write forwarding.
# Followers use this to forward writes to the leader via HTTP.
MY_HTTP="http://${HOSTNAME}.${HEADLESS_SVC}.${NAMESPACE}.svc.cluster.local:%[2]d"

# Build peer list: all other nodes in the cluster.
# Format: id@raft_addr@grpc_addr@http_addr (4-part format)
# See: replication/cluster.go ParsePeers()
PEERS=""
for i in $(seq 0 $((REPLICAS - 1))); do
  if [ "$i" != "$ORDINAL" ]; then
    PEER_NAME="${CLUSTER_NAME}-${i}"
    PEER_HOST="${PEER_NAME}.${HEADLESS_SVC}.${NAMESPACE}.svc.cluster.local"
    PEER_ENTRY="${PEER_NAME}@${PEER_HOST}:%[3]d@${PEER_HOST}:%[4]d@http://${PEER_HOST}:%[2]d"
    if [ -n "$PEERS" ]; then
      PEERS="${PEERS},${PEER_ENTRY}"
    else
      PEERS="${PEER_ENTRY}"
    fi
  fi
done

echo "Starting GoraphDB in cluster mode..."
echo "  Node ID:   ${NODE_ID}"
echo "  Raft:      ${RAFT_ADDR}"
echo "  gRPC:      ${GRPC_ADDR}"
echo "  HTTP:      ${HTTP_ADDR}"
echo "  Peers:     ${PEERS}"

exec /usr/local/bin/graphdb-ui \
  -db /data/db \
  -addr "${HTTP_ADDR}" \
  -shards %[8]d \
  -node-id "${NODE_ID}" \
  -raft-addr "${RAFT_ADDR}" \
  -grpc-addr "${GRPC_ADDR}" \
  -bootstrap \
  -peers "${PEERS}"
`,
		cluster.Name,   // [1] CLUSTER_NAME
		httpPort,       // [2] HTTP port
		raftPort,       // [3] Raft port
		grpcPort,       // [4] gRPC port
		headlessSvc,    // [5] headless service name
		cluster.Namespace, // [6] namespace (will be injected at runtime via downward API if needed)
		replicas,       // [7] total replicas
		shards,         // [8] shard count
	)
}

// reconcilePDB creates or updates the PodDisruptionBudget.
//
// The PDB prevents voluntary disruptions (kubectl drain, node upgrades)
// from killing too many pods simultaneously and breaking Raft quorum.
//
// Default: minAvailable = ceil(replicas/2), which equals Raft quorum.
// This allows exactly (replicas - quorum) disruptions at a time.
func (r *GoraphDBClusterReconciler) reconcilePDB(ctx context.Context, cluster *goraphdbv1alpha1.GoraphDBCluster) error {
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		if err := controllerutil.SetControllerReference(cluster, pdb, r.Scheme); err != nil {
			return err
		}

		pdb.Labels = commonLabels(cluster)

		// Calculate minAvailable: default to Raft quorum.
		minAvail := (*cluster.Spec.Replicas / 2) + 1
		if cluster.Spec.PodDisruptionBudget != nil && cluster.Spec.PodDisruptionBudget.MinAvailable != nil {
			minAvail = *cluster.Spec.PodDisruptionBudget.MinAvailable
		}

		minAvailVal := intstr.FromInt32(minAvail)
		pdb.Spec = policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailVal,
			Selector: &metav1.LabelSelector{
				MatchLabels: selectorLabels(cluster),
			},
		}
		return nil
	})
	return err
}

// updateStatus reads the current state of the StatefulSet and pods,
// then updates the GoraphDBCluster status subresource.
func (r *GoraphDBClusterReconciler) updateStatus(ctx context.Context, cluster *goraphdbv1alpha1.GoraphDBCluster) error {
	// Get the StatefulSet to check ready replicas.
	var sts appsv1.StatefulSet
	stsKey := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	if err := r.Get(ctx, stsKey, &sts); err != nil {
		if errors.IsNotFound(err) {
			cluster.Status.Phase = goraphdbv1alpha1.ClusterPhaseCreating
			cluster.Status.ReadyReplicas = 0
		} else {
			return err
		}
	} else {
		cluster.Status.ReadyReplicas = sts.Status.ReadyReplicas
		cluster.Status.CurrentImage = cluster.Spec.Image

		// Determine cluster phase.
		desired := *cluster.Spec.Replicas
		ready := sts.Status.ReadyReplicas

		switch {
		case ready == 0:
			cluster.Status.Phase = goraphdbv1alpha1.ClusterPhaseBootstrapping
		case ready < desired:
			// Some pods are not ready — could be scaling or degraded.
			if sts.Status.CurrentReplicas != desired {
				cluster.Status.Phase = goraphdbv1alpha1.ClusterPhaseScaling
			} else {
				cluster.Status.Phase = goraphdbv1alpha1.ClusterPhaseDegraded
			}
		case ready == desired:
			cluster.Status.Phase = goraphdbv1alpha1.ClusterPhaseRunning
		}
	}

	// Build member status list.
	members := make([]goraphdbv1alpha1.MemberStatus, 0, *cluster.Spec.Replicas)
	for i := int32(0); i < *cluster.Spec.Replicas; i++ {
		name := podName(cluster, int(i))
		member := goraphdbv1alpha1.MemberStatus{
			Name:   name,
			Health: "unknown",
		}

		// Check if pod exists and is ready.
		var pod corev1.Pod
		podKey := types.NamespacedName{Name: name, Namespace: cluster.Namespace}
		if err := r.Get(ctx, podKey, &pod); err == nil {
			member.Ready = isPodReady(&pod)
			if role, ok := pod.Labels[LabelRole]; ok {
				member.Role = role
				if role == "leader" {
					cluster.Status.Leader = name
				}
			}
			if member.Ready {
				member.Health = "ok"
			} else {
				member.Health = "unavailable"
			}
		} else {
			member.Health = "unavailable"
		}

		members = append(members, member)
	}
	cluster.Status.Members = members

	// Write status update.
	return r.Status().Update(ctx, cluster)
}

// isPodReady checks if a pod has the Ready condition set to True.
func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// SetupWithManager registers the reconciler with the controller manager.
//
// The controller watches:
// - GoraphDBCluster (primary resource)
// - StatefulSet, Service, ConfigMap, PDB (owned resources)
//
// When any owned resource changes, the reconciler is triggered for the
// owning GoraphDBCluster (via OwnerReference back-reference).
func (r *GoraphDBClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&goraphdbv1alpha1.GoraphDBCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Complete(r)
}
