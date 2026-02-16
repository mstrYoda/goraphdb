package controllers

// ============================================================================
// Bootstrap Sequencing
//
// GoraphDB cluster bootstrap requires careful ordering:
//
//   1. All initial nodes start with -bootstrap flag
//   2. Hashicorp Raft's BootstrapCluster() is called on each node
//   3. Raft elects a leader (HeartbeatTimeout=1s, ElectionTimeout=1s)
//   4. The leader starts the gRPC replication server
//   5. Followers connect to the leader and begin WAL streaming
//
// In Kubernetes, the StatefulSet's OrderedReady policy ensures pod-0
// starts before pod-1, which starts before pod-2, etc. This gives us
// deterministic ordering without extra operator logic.
//
// Bootstrap safety:
//
// Hashicorp Raft safely handles re-bootstrap: if the cluster is already
// bootstrapped (Raft state exists in raft-log.db), BootstrapCluster()
// returns raft.ErrCantBootstrap which GoraphDB's election.go ignores.
// This means we can always pass -bootstrap on every startup without
// risking a split-brain.
//
// The operator's role in bootstrap:
//
//   - Sets -bootstrap flag for all pods (via entrypoint script)
//   - Monitors /api/health on pod-0 to detect when bootstrap completes
//   - Updates cluster status phase: Creating → Bootstrapping → Running
//   - Emits Kubernetes Events for bootstrap milestones
//
// Full cluster restart:
//
// When all pods restart simultaneously (e.g., node failure, cluster upgrade):
//   - Each pod has persisted Raft state in /data/db/raft/
//   - Raft detects existing state and skips bootstrap
//   - Leader election happens naturally (1-2s)
//   - Followers reconnect to the new leader for WAL streaming
//   - No data loss because shards are on PVCs
//
// Reference: replication/election.go, replication/cluster.go StartCluster()
// ============================================================================

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	goraphdbv1alpha1 "github.com/mstrYoda/goraphdb/operator/api/v1alpha1"
)

// isClusterBootstrapped checks if the cluster has completed initial bootstrap.
//
// We determine this by checking if pod-0 exists and is ready.
// In OrderedReady StatefulSet policy, pod-0 is always the first to start.
// Once pod-0 is ready, it means:
//   - The database is open and serving HTTP
//   - In cluster mode: Raft is initialized (bootstrap complete or rejoined)
//   - The /api/health endpoint returns 200
//
// This function is used to update the cluster phase from Bootstrapping → Running.
func (r *GoraphDBClusterReconciler) isClusterBootstrapped(ctx context.Context, cluster *goraphdbv1alpha1.GoraphDBCluster) bool {
	pod0Name := podName(cluster, 0)
	var pod corev1.Pod
	key := types.NamespacedName{Name: pod0Name, Namespace: cluster.Namespace}

	if err := r.Get(ctx, key, &pod); err != nil {
		return false
	}

	return isPodReady(&pod)
}

// getBootstrapPodCount returns the number of pods that should exist
// during the bootstrap phase.
//
// Design decision: we start all pods simultaneously rather than
// sequentially adding them one at a time. This is because:
//
//   1. Hashicorp Raft requires a quorum to elect a leader.
//      With 3 nodes, quorum=2, so at least 2 must be running.
//
//   2. StatefulSet's OrderedReady policy already ensures ordered startup:
//      pod-0 starts first, then pod-1 (after pod-0 is ready), etc.
//
//   3. All nodes pass -bootstrap, so they form the initial Raft cluster
//      together. This matches GoraphDB's documented 3-node example in
//      cmd/graphdb-ui/main.go where all nodes start with -bootstrap.
//
// The full desired replica count is returned for bootstrap.
func getBootstrapPodCount(cluster *goraphdbv1alpha1.GoraphDBCluster) int32 {
	return *cluster.Spec.Replicas
}

// isBootstrapNode returns true if the given ordinal is the bootstrap
// seed node (ordinal 0).
//
// While all nodes pass -bootstrap in the current implementation,
// this function exists as an extension point for future optimizations
// where only the seed node bootstraps and others join via AddVoter().
func isBootstrapNode(ordinal int) bool {
	return ordinal == 0
}

// needsBootstrap returns true if the cluster is in a state that requires
// initial bootstrap (no existing Raft state).
//
// The operator determines this by checking:
//   - Cluster phase is Creating or empty (first reconciliation)
//   - No pods are running yet
//
// If pods exist with Raft state on their PVCs, this returns false
// and the cluster will rejoin from existing state.
func (r *GoraphDBClusterReconciler) needsBootstrap(ctx context.Context, cluster *goraphdbv1alpha1.GoraphDBCluster) bool {
	return cluster.Status.Phase == "" ||
		cluster.Status.Phase == goraphdbv1alpha1.ClusterPhaseCreating
}
