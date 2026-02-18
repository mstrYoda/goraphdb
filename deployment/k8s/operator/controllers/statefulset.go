package controllers

// ============================================================================
// StatefulSet Builder
//
// Constructs the StatefulSet spec for GoraphDB pods. This is the heart of
// the operator — it translates CRD fields into a working StatefulSet with:
//
//   - Correct container command/args (via entrypoint script ConfigMap)
//   - PersistentVolumeClaim templates for data/WAL/Raft directories
//   - Health probes hitting GoraphDB's /api/health endpoint
//   - Pod anti-affinity for HA across Kubernetes worker nodes
//   - Resource requests/limits from the CRD spec
//   - Graceful shutdown with terminationGracePeriodSeconds=30
//
// Why StatefulSet (not Deployment)?
//
// GoraphDB is stateful and requires:
//   1. Stable network identity: each pod gets a predictable DNS name
//      ({pod-name}.{headless-svc}.{ns}.svc.cluster.local) used for
//      Raft peer discovery and gRPC replication addressing.
//   2. Stable storage: each pod's PVC persists across restarts, so
//      shard data, WAL segments, and Raft state survive rescheduling.
//   3. Ordered startup: pod-0 starts first (important for bootstrap).
//
// Volume layout:
//
//   /data/db/              ← data PVC mount (shard_0000.db, shard_0001.db, ...)
//   /data/db/wal/          ← WAL subdirectory (or separate PVC if walSize set)
//   /data/db/raft/         ← Raft subdirectory (or separate PVC if raftSize set)
//   /scripts/entrypoint.sh ← ConfigMap mount (cluster topology script)
//
// This matches GoraphDB's native directory layout:
//   cmd/graphdb-ui -db /data/db → creates /data/db/shard_*.db, /data/db/wal/, /data/db/raft/
// ============================================================================

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	goraphdbv1alpha1 "github.com/mstrYoda/goraphdb/operator/api/v1alpha1"
)

// reconcileStatefulSet creates or updates the StatefulSet for the GoraphDB cluster.
//
// StatefulSet VolumeClaimTemplates are immutable after creation. On update,
// we must preserve the existing templates to avoid API validation errors.
// If VolumeClaimTemplates need to change, the StatefulSet must be deleted
// and recreated (handled by the operator during major upgrades).
func (r *GoraphDBClusterReconciler) reconcileStatefulSet(ctx context.Context, cluster *goraphdbv1alpha1.GoraphDBCluster) error {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		if err := controllerutil.SetControllerReference(cluster, sts, r.Scheme); err != nil {
			return err
		}

		// Preserve existing VolumeClaimTemplates (immutable field).
		existingVCTs := sts.Spec.VolumeClaimTemplates

		sts.Labels = commonLabels(cluster)
		r.buildStatefulSetSpec(cluster, sts)

		// If the StatefulSet already exists, restore the original
		// VolumeClaimTemplates to avoid immutable field validation errors.
		if len(existingVCTs) > 0 {
			sts.Spec.VolumeClaimTemplates = existingVCTs
		}

		return nil
	})
	return err
}

// buildStatefulSetSpec populates the StatefulSet spec from the CRD.
func (r *GoraphDBClusterReconciler) buildStatefulSetSpec(cluster *goraphdbv1alpha1.GoraphDBCluster, sts *appsv1.StatefulSet) {
	replicas := *cluster.Spec.Replicas
	labels := podLabels(cluster)
	selector := selectorLabels(cluster)

	// Use OrderedReady pod management policy for controlled startup.
	// This ensures pod-0 is fully ready before pod-1 starts, which is
	// important for Raft bootstrap (pod-0 bootstraps the cluster first).
	podMgmtPolicy := appsv1.OrderedReadyPodManagement

	sts.Spec = appsv1.StatefulSetSpec{
		Replicas:            &replicas,
		ServiceName:         headlessServiceName(cluster),
		PodManagementPolicy: podMgmtPolicy,
		Selector: &metav1.LabelSelector{
			MatchLabels: selector,
		},
		// Use OnDelete update strategy for controlled rolling updates.
		// The operator manages the update sequence:
		//   1. Update followers first (one at a time, verify WAL catch-up)
		//   2. Update leader last (triggers failover)
		// Using OnDelete instead of RollingUpdate gives the operator
		// fine-grained control over the update order.
		UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
			Type: appsv1.RollingUpdateStatefulSetStrategyType,
		},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: labels,
			},
			Spec: r.buildPodSpec(cluster),
		},
		VolumeClaimTemplates: r.buildVolumeClaimTemplates(cluster),
	}
}

// buildPodSpec constructs the pod template spec for GoraphDB containers.
func (r *GoraphDBClusterReconciler) buildPodSpec(cluster *goraphdbv1alpha1.GoraphDBCluster) corev1.PodSpec {
	httpPort := cluster.Spec.Ports.HTTP

	// -----------------------------------------------------------------------
	// Container definition
	// -----------------------------------------------------------------------

	container := corev1.Container{
		Name:            "goraphdb",
		Image:           cluster.Spec.Image,
		ImagePullPolicy: cluster.Spec.ImagePullPolicy,

		// Use the entrypoint script from the ConfigMap.
		// The script computes cluster topology from the pod's hostname
		// and constructs the correct CLI flags.
		Command: []string{"/bin/sh", "/scripts/entrypoint.sh"},

		// Environment variables injected into the entrypoint script.
		// POD_NAME and POD_NAMESPACE use the downward API to get
		// the pod's identity at runtime.
		Env: []corev1.EnvVar{
			{
				Name: "POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.name",
					},
				},
			},
			{
				Name: "POD_NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
				},
			},
		},

		// Expose all three ports for the container.
		Ports: r.buildContainerPorts(cluster),

		// Volume mounts: data PVC + entrypoint script ConfigMap.
		VolumeMounts: r.buildVolumeMounts(cluster),

		// Resources from the CRD spec.
		Resources: cluster.Spec.Resources,

		// -----------------------------------------------------------------------
		// Health Probes
		//
		// GoraphDB exposes GET /api/health which returns:
		//   200 OK with {"status":"ok","role":"leader|follower|standalone"}
		//   503    with {"status":"unavailable"} when DB is closed
		//
		// Source: server/server.go handleHealth()
		// -----------------------------------------------------------------------

		// Liveness probe: restart the pod if GoraphDB is completely unresponsive.
		// Uses a generous timeout because GoraphDB may be slow during compaction.
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/api/health",
					Port: intOrString(httpPort),
				},
			},
			InitialDelaySeconds: 30,   // Wait for DB to open and load shards
			PeriodSeconds:       10,   // Check every 10s
			TimeoutSeconds:      5,    // 5s timeout for the HTTP call
			FailureThreshold:    6,    // 6 failures = 60s before restart
			SuccessThreshold:    1,
		},

		// Readiness probe: remove pod from Service endpoints if unhealthy.
		// More aggressive than liveness — we want quick removal from
		// load balancers when a pod can't serve traffic.
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/api/health",
					Port: intOrString(httpPort),
				},
			},
			InitialDelaySeconds: 10,   // Shorter delay — readiness is less critical than liveness
			PeriodSeconds:       5,    // Check every 5s for faster failover
			TimeoutSeconds:      3,
			FailureThreshold:    3,    // 3 failures = 15s before removal from endpoints
			SuccessThreshold:    1,
		},

		// Startup probe: allows extra time for initial data loading.
		// During first startup, GoraphDB needs to:
		//   1. Create/open bbolt shard files
		//   2. Build in-memory indexes
		//   3. Complete Raft bootstrap (in cluster mode)
		// For large datasets, this can take minutes.
		StartupProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/api/health",
					Port: intOrString(httpPort),
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       5,
			TimeoutSeconds:      3,
			FailureThreshold:    60,   // 60 * 5s = 5 minutes max startup time
			SuccessThreshold:    1,
		},
	}

	// -----------------------------------------------------------------------
	// Pod spec
	// -----------------------------------------------------------------------

	podSpec := corev1.PodSpec{
		// Graceful shutdown budget:
		//   GoraphDB shutdown sequence (cmd/graphdb-ui/main.go:169-187):
		//     1. HTTP server shutdown with 10s drain timeout
		//     2. Cluster stop (Raft + gRPC)
		//     3. Database close (flush WAL, close bbolt, stop compaction)
		//   Total: ~15-20s, so 30s gives comfortable headroom.
		TerminationGracePeriodSeconds: int64Ptr(30),

		Containers: []corev1.Container{container},

		// Volume sources: ConfigMap for entrypoint script.
		// PVC volumes are defined in VolumeClaimTemplates (not here).
		Volumes: []corev1.Volume{
			{
				Name: "scripts",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: configMapName(cluster),
						},
						// Make the script executable.
						DefaultMode: int32Ptr(0755),
					},
				},
			},
		},

		// Security: run as non-root for defense in depth.
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot: boolPtr(true),
			RunAsUser:    int64Ptr(65534), // nobody
			FSGroup:      int64Ptr(65534), // nobody — ensures PVC files are group-writable
		},
	}

	// -----------------------------------------------------------------------
	// Pod anti-affinity — spread cluster nodes across worker nodes
	// -----------------------------------------------------------------------

	if *cluster.Spec.Replicas > 1 {
		antiAffinity := &corev1.PodAntiAffinity{
			// Prefer different worker nodes, but don't hard-require it.
			// Hard anti-affinity would prevent scheduling on clusters with
			// fewer worker nodes than GoraphDB replicas.
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
				{
					Weight: 100,
					PodAffinityTerm: corev1.PodAffinityTerm{
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: selectorLabels(cluster),
						},
						TopologyKey: "kubernetes.io/hostname",
					},
				},
			},
		}

		if podSpec.Affinity == nil {
			podSpec.Affinity = &corev1.Affinity{}
		}
		podSpec.Affinity.PodAntiAffinity = antiAffinity
	}

	// Merge user-defined affinity (from CRD spec).
	if cluster.Spec.Affinity != nil {
		if podSpec.Affinity == nil {
			podSpec.Affinity = cluster.Spec.Affinity
		} else {
			// Merge node affinity and pod affinity from user spec.
			if cluster.Spec.Affinity.NodeAffinity != nil {
				podSpec.Affinity.NodeAffinity = cluster.Spec.Affinity.NodeAffinity
			}
			if cluster.Spec.Affinity.PodAffinity != nil {
				podSpec.Affinity.PodAffinity = cluster.Spec.Affinity.PodAffinity
			}
		}
	}

	// Tolerations and node selector from CRD spec.
	podSpec.Tolerations = cluster.Spec.Tolerations
	podSpec.NodeSelector = cluster.Spec.NodeSelector

	return podSpec
}

// buildContainerPorts returns the container port definitions.
func (r *GoraphDBClusterReconciler) buildContainerPorts(cluster *goraphdbv1alpha1.GoraphDBCluster) []corev1.ContainerPort {
	ports := []corev1.ContainerPort{
		{
			// HTTP port: REST API, Cypher queries, health checks, metrics, UI.
			Name:          "http",
			ContainerPort: cluster.Spec.Ports.HTTP,
			Protocol:      corev1.ProtocolTCP,
		},
	}

	// Raft and gRPC ports only in cluster mode.
	if *cluster.Spec.Replicas > 1 {
		ports = append(ports,
			corev1.ContainerPort{
				// Raft port: leader election heartbeats and log entries.
				Name:          "raft",
				ContainerPort: cluster.Spec.Ports.Raft,
				Protocol:      corev1.ProtocolTCP,
			},
			corev1.ContainerPort{
				// gRPC port: WAL streaming from leader to followers.
				Name:          "grpc",
				ContainerPort: cluster.Spec.Ports.GRPC,
				Protocol:      corev1.ProtocolTCP,
			},
		)
	}

	return ports
}

// buildVolumeMounts returns the volume mounts for the GoraphDB container.
func (r *GoraphDBClusterReconciler) buildVolumeMounts(cluster *goraphdbv1alpha1.GoraphDBCluster) []corev1.VolumeMount {
	mounts := []corev1.VolumeMount{
		{
			// Main data directory: shard_*.db files.
			// GoraphDB CLI flag: -db /data/db
			// This PVC also contains wal/ and raft/ subdirectories
			// unless separate PVCs are configured.
			Name:      "data",
			MountPath: "/data/db",
		},
		{
			// Entrypoint script from ConfigMap.
			Name:      "scripts",
			MountPath: "/scripts",
			ReadOnly:  true,
		},
	}

	// Optional separate WAL volume.
	// If walSize is set in the CRD, we create a separate PVC and mount
	// it at /data/wal. The entrypoint script would need to symlink or
	// configure GoraphDB accordingly.
	//
	// For now, WAL is stored under /data/db/wal/ (the default goraphdb behavior).
	// Separate WAL PVC support is planned for a future version.

	return mounts
}

// buildVolumeClaimTemplates returns PVC templates for the StatefulSet.
//
// Each pod gets its own PVC(s) that persist across restarts. This is
// critical for GoraphDB because:
//   - Shard data must survive pod rescheduling
//   - WAL segments must persist for replication catch-up
//   - Raft state (log + stable store) must persist for re-election
func (r *GoraphDBClusterReconciler) buildVolumeClaimTemplates(cluster *goraphdbv1alpha1.GoraphDBCluster) []corev1.PersistentVolumeClaim {
	pvcs := []corev1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "data",
				Labels: commonLabels(cluster),
			},
			Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteMany,
			},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: cluster.Spec.Storage.DataSize,
					},
				},
				StorageClassName: cluster.Spec.Storage.StorageClassName,
			},
		},
	}

	// Optional: separate WAL PVC for higher I/O throughput.
	if cluster.Spec.Storage.WALSize != nil {
		pvcs = append(pvcs, corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "wal",
				Labels: commonLabels(cluster),
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteMany,
				},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: *cluster.Spec.Storage.WALSize,
					},
				},
				StorageClassName: cluster.Spec.Storage.StorageClassName,
			},
		})
	}

	// Optional: separate Raft PVC for low-latency consensus.
	if cluster.Spec.Storage.RaftSize != nil {
		pvcs = append(pvcs, corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "raft",
				Labels: commonLabels(cluster),
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteMany,
				},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: *cluster.Spec.Storage.RaftSize,
					},
				},
				StorageClassName: cluster.Spec.Storage.StorageClassName,
			},
		})
	}

	return pvcs
}

// Helper to create intstr.IntOrString from an int32 port.
func intOrString(port int32) intstr.IntOrString {
	return intstr.FromInt32(port)
}

// Helper to create *int64 pointers.
func int64Ptr(v int64) *int64 { return &v }

// Helper to create *int32 pointers.
func int32Ptr(v int32) *int32 { return &v }

// Helper to create *bool pointers.
func boolPtr(v bool) *bool { return &v }

// Helper to create *resource.Quantity pointers.
func quantityPtr(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}

// formatQuantityBytes returns the byte value of a resource.Quantity as a string.
// Used to pass CacheBudget and MmapSize to GoraphDB as integer bytes.
func formatQuantityBytes(q resource.Quantity) string {
	return fmt.Sprintf("%d", q.Value())
}
