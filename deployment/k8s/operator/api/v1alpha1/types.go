package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ============================================================================
// GoraphDBCluster — Custom Resource Definition
//
// This CRD represents a complete GoraphDB cluster deployment. Each field is
// documented with:
//   - Its purpose in Kubernetes
//   - The corresponding goraphdb.Options field or CLI flag it maps to
//   - Default values and tuning guidance
//
// The operator reconciles this resource into:
//   StatefulSet + Headless Service + Client Services + ConfigMap + PDB
//
// Reference source files in goraphdb:
//   - types.go:              Options struct (all database configuration)
//   - cmd/graphdb-ui/main.go: CLI flags (-db, -addr, -shards, -node-id, etc.)
//   - replication/cluster.go: ClusterConfig struct (Raft + gRPC configuration)
//   - server/server.go:       HTTP endpoints (/api/health, /metrics, etc.)
//   - metrics.go:             Prometheus metrics definitions
// ============================================================================

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=gdb
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Leader",type=string,JSONPath=`.status.leader`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// GoraphDBCluster is the top-level custom resource that defines a GoraphDB
// database cluster. The operator watches these resources and creates the
// necessary Kubernetes objects to run and manage the cluster.
type GoraphDBCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GoraphDBClusterSpec   `json:"spec,omitempty"`
	Status GoraphDBClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// GoraphDBClusterList contains a list of GoraphDBCluster resources.
type GoraphDBClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GoraphDBCluster `json:"items"`
}

// ============================================================================
// Spec — Desired State
// ============================================================================

// GoraphDBClusterSpec defines the desired state of a GoraphDB cluster.
// Fields are organized by concern: cluster topology, storage engine, query
// governor, write backpressure, persistence, networking, and observability.
type GoraphDBClusterSpec struct {

	// -----------------------------------------------------------------------
	// Cluster Topology
	// -----------------------------------------------------------------------

	// Replicas is the number of database nodes in the cluster.
	//
	// MUST be an odd number (1, 3, 5) for Raft consensus quorum.
	// Raft quorum = floor(N/2) + 1, so:
	//   - 1 node:  no fault tolerance (standalone mode, no Raft)
	//   - 3 nodes: tolerates 1 failure (quorum = 2)
	//   - 5 nodes: tolerates 2 failures (quorum = 3)
	//
	// When replicas=1, the operator runs GoraphDB in standalone mode
	// (no Raft, no WAL, no gRPC replication) for simplicity.
	// When replicas>1, cluster mode is enabled automatically with:
	//   - Raft-based leader election (hashicorp/raft)
	//   - WAL-based replication over gRPC
	//   - Automatic write forwarding from followers to leader
	//
	// Maps to: StatefulSet .spec.replicas
	// Related: replication/election.go (Raft), replication/cluster.go
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=7
	// +kubebuilder:default=1
	Replicas *int32 `json:"replicas"`

	// Image is the container image for GoraphDB server nodes.
	// This should point to a built image of cmd/graphdb-ui which includes
	// the HTTP API server, Cypher engine, and cluster support.
	//
	// Example: "ghcr.io/mstryoda/goraphdb:v0.1.0"
	//
	// Maps to: StatefulSet container image
	// +kubebuilder:validation:Required
	Image string `json:"image"`

	// ImagePullPolicy controls when the kubelet pulls the GoraphDB image.
	// Use "Always" during development, "IfNotPresent" for tagged releases.
	//
	// +kubebuilder:default="IfNotPresent"
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// -----------------------------------------------------------------------
	// Storage Engine Configuration
	//
	// These fields map directly to graphdb.Options (types.go) and control
	// the bbolt storage engine, sharding, caching, and compaction.
	// -----------------------------------------------------------------------

	// ShardCount is the number of bbolt database shards.
	// Each shard is a separate bbolt file (shard_0000.db, shard_0001.db, ...).
	// More shards = higher write concurrency (writes to different shards
	// are fully parallel). Read queries fan out across all shards.
	//
	// IMPORTANT: All nodes in a cluster MUST use the same ShardCount.
	// Changing this value on a running cluster requires a full data migration.
	//
	// Maps to: graphdb.Options.ShardCount (default: 1)
	// CLI flag: -shards
	// Source: types.go:112
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=64
	// +kubebuilder:default=1
	ShardCount *int32 `json:"shardCount,omitempty"`

	// WorkerPoolSize is the number of goroutines in the query worker pool.
	// Controls how many Cypher queries can execute concurrently.
	// The pool is used by ExecuteConcurrent() for parallel shard fan-out.
	//
	// Maps to: graphdb.Options.WorkerPoolSize (default: 8)
	// Source: types.go:114, pool.go
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=256
	// +kubebuilder:default=8
	WorkerPoolSize *int32 `json:"workerPoolSize,omitempty"`

	// CacheBudget is the LRU node cache memory budget.
	// Hot nodes are cached in memory and evicted LRU-first when the total
	// estimated size exceeds this budget. Larger values improve read
	// performance for frequently-accessed nodes at the cost of memory.
	//
	// Maps to: graphdb.Options.CacheBudget (default: 128Mi = 134217728 bytes)
	// Source: types.go:117-119
	// +kubebuilder:default="128Mi"
	CacheBudget resource.Quantity `json:"cacheBudget,omitempty"`

	// MmapSize is the initial memory-mapped file size for bbolt.
	// bbolt uses mmap for read access; a larger initial size avoids
	// repeated mmap growth for large datasets. This does NOT pre-allocate
	// disk space — it only reserves virtual address space.
	//
	// Maps to: graphdb.Options.MmapSize (default: 256Mi = 268435456 bytes)
	// Source: types.go:125-126
	// +kubebuilder:default="256Mi"
	MmapSize resource.Quantity `json:"mmapSize,omitempty"`

	// NoSync disables per-transaction fsync for higher write throughput.
	// When true (default), a background goroutine syncs each shard every
	// ~200ms. This means at most 200ms of committed writes may be lost on
	// an unclean shutdown. In cluster mode, the WAL provides additional
	// durability (group commit fsync every 2ms).
	//
	// Set to false for per-transaction durability at ~20x lower throughput.
	//
	// Maps to: graphdb.Options.NoSync (default: true)
	// Source: types.go:121, DefaultOptions() at types.go:195
	// +kubebuilder:default=true
	NoSync *bool `json:"noSync,omitempty"`

	// CompactionInterval controls automatic background compaction.
	// bbolt files grow after deletes but never shrink. Compaction rewrites
	// the entire shard file using bbolt's Tx.WriteTo() to reclaim space.
	//
	// Set to "0" to disable (manual compaction via db.Compact() only).
	// Typical production value: "1h".
	//
	// Maps to: graphdb.Options.CompactionInterval (default: 0 = disabled)
	// Source: types.go:183-184
	// +kubebuilder:default="0s"
	CompactionInterval metav1.Duration `json:"compactionInterval,omitempty"`

	// -----------------------------------------------------------------------
	// Query Governor
	//
	// These fields control query execution limits to prevent runaway queries
	// from consuming unbounded resources.
	// -----------------------------------------------------------------------

	// SlowQueryThreshold is the duration above which queries are logged
	// as slow at WARN level and counted in the graphdb_slow_queries_total
	// Prometheus metric.
	//
	// Maps to: graphdb.Options.SlowQueryThreshold (default: 100ms)
	// Source: types.go:129-131
	// +kubebuilder:default="100ms"
	SlowQueryThreshold metav1.Duration `json:"slowQueryThreshold,omitempty"`

	// MaxResultRows is the maximum number of rows a single Cypher query
	// can return. Queries exceeding this limit fail with ErrResultTooLarge.
	// Set to 0 for unlimited (not recommended in production).
	//
	// Maps to: graphdb.Options.MaxResultRows (default: 0 = unlimited)
	// Source: types.go:139-141
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=0
	MaxResultRows *int32 `json:"maxResultRows,omitempty"`

	// DefaultQueryTimeout is applied when the caller's context has no
	// deadline. Prevents runaway queries from holding resources indefinitely.
	// Set to "0s" to disable (callers must set their own deadlines).
	//
	// Maps to: graphdb.Options.DefaultQueryTimeout (default: 0 = no timeout)
	// Source: types.go:143-145
	// +kubebuilder:default="0s"
	DefaultQueryTimeout metav1.Duration `json:"defaultQueryTimeout,omitempty"`

	// -----------------------------------------------------------------------
	// Write Backpressure
	//
	// Per-shard semaphore that limits concurrent write operations.
	// Prevents write storms from exhausting bbolt's serialized write lock.
	// -----------------------------------------------------------------------

	// WriteQueueSize is the maximum number of concurrent write operations
	// per shard. Additional writers block until a slot is available or
	// WriteTimeout expires (returning ErrWriteQueueFull).
	//
	// Maps to: graphdb.Options.WriteQueueSize (default: 64)
	// Source: types.go:149-151
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=64
	WriteQueueSize *int32 `json:"writeQueueSize,omitempty"`

	// WriteTimeout is the maximum time a write operation waits for a slot
	// in the write queue before returning ErrWriteQueueFull.
	// Set to "0s" for unlimited waiting.
	//
	// Maps to: graphdb.Options.WriteTimeout (default: 5s)
	// Source: types.go:153-155
	// +kubebuilder:default="5s"
	WriteTimeout metav1.Duration `json:"writeTimeout,omitempty"`

	// -----------------------------------------------------------------------
	// Persistent Storage
	//
	// GoraphDB uses three distinct data directories that benefit from
	// separate PersistentVolumeClaims with potentially different
	// performance characteristics:
	//
	//   /data/db     — shard_*.db files (bbolt B+tree, mmap-heavy reads)
	//   /data/db/wal — WAL segments (sequential append, fsync every 2ms)
	//   /data/db/raft — Raft log + stable store (small, latency-sensitive)
	//
	// The WAL and Raft directories can use faster storage (e.g., local NVMe)
	// while data can use cheaper networked storage if needed.
	// -----------------------------------------------------------------------

	// Storage configures PersistentVolumeClaim sizes and storage class
	// for the database's data directories.
	Storage StorageSpec `json:"storage"`

	// -----------------------------------------------------------------------
	// Compute Resources
	// -----------------------------------------------------------------------

	// Resources defines CPU and memory requests/limits for GoraphDB pods.
	//
	// Memory sizing guidance:
	//   Base memory = CacheBudget + MmapSize + ~100Mi overhead
	//   For defaults: 128Mi + 256Mi + 100Mi = ~484Mi minimum
	//   Recommended: set memory request >= CacheBudget + MmapSize + 256Mi
	//
	// CPU sizing guidance:
	//   WorkerPoolSize goroutines execute queries concurrently.
	//   Set CPU request >= WorkerPoolSize * 0.1 cores for moderate load.
	//
	// Maps to: StatefulSet container resources
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// -----------------------------------------------------------------------
	// Networking
	// -----------------------------------------------------------------------

	// Ports configures the network ports for GoraphDB services.
	Ports PortSpec `json:"ports,omitempty"`

	// -----------------------------------------------------------------------
	// Observability
	// -----------------------------------------------------------------------

	// Monitoring configures Prometheus metrics scraping.
	Monitoring MonitoringSpec `json:"monitoring,omitempty"`

	// -----------------------------------------------------------------------
	// Pod Scheduling & Placement
	// -----------------------------------------------------------------------

	// Affinity defines pod scheduling constraints.
	// The operator automatically adds pod anti-affinity to spread cluster
	// nodes across different Kubernetes worker nodes for HA. Custom
	// affinity rules here are merged with the operator's defaults.
	//
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// Tolerations for GoraphDB pods.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// NodeSelector constrains pods to nodes with matching labels.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// -----------------------------------------------------------------------
	// Availability
	// -----------------------------------------------------------------------

	// PodDisruptionBudget configures voluntary disruption protection.
	// The operator defaults to minAvailable = ceil(replicas/2) to protect
	// Raft quorum during voluntary disruptions (node drains, upgrades).
	//
	// +optional
	PodDisruptionBudget *PDBSpec `json:"podDisruptionBudget,omitempty"`
}

// ============================================================================
// Sub-specs
// ============================================================================

// StorageSpec defines persistent volume sizes for GoraphDB's data directories.
//
// GoraphDB stores data in three distinct directories with different I/O patterns:
//
//	/data/db/       — bbolt shard files (random read via mmap, sequential write)
//	/data/db/wal/   — WAL segments (sequential append-only, fsync every 2ms)
//	/data/db/raft/  — Raft state (small files, latency-sensitive reads/writes)
//
// For single-PVC simplicity, set only DataSize. The WAL and Raft directories
// will be subdirectories of the data volume (this is the default behavior
// of goraphdb — wal/ and raft/ are created under the -db path).
//
// For maximum performance, use separate PVCs with different storage classes:
//   - Data: general-purpose SSD (gp3, pd-ssd)
//   - WAL:  high-IOPS local NVMe (for 2ms fsync)
//   - Raft: low-latency local storage (small but latency-sensitive)
type StorageSpec struct {
	// DataSize is the PVC size for the main data directory containing
	// shard_*.db files. This is the primary storage consumer.
	//
	// Sizing guidance: each node stores the full dataset (no sharding
	// across nodes — sharding is within a single node's bbolt files).
	// Start with 2x your expected data size for compaction headroom.
	//
	// Maps to: PVC for the -db directory (cmd/graphdb-ui -db flag)
	// +kubebuilder:default="10Gi"
	DataSize resource.Quantity `json:"dataSize"`

	// WALSize is the PVC size for WAL segments (only used in cluster mode).
	// Each WAL segment is 64MB (wal.go maxSegmentSize). The WAL grows
	// until segments are pruned after all followers catch up.
	//
	// Sizing: (expected write throughput * max follower lag) + safety margin.
	// For most workloads, 5Gi provides hours of WAL retention.
	//
	// If empty, WAL is stored as a subdirectory of the data volume.
	//
	// Maps to: WAL directory at {db_path}/wal/ (wal.go)
	// +optional
	WALSize *resource.Quantity `json:"walSize,omitempty"`

	// RaftSize is the PVC size for Raft consensus state.
	// Raft state is small (raft-log.db + raft-stable.db, typically <100MB)
	// but latency-sensitive for leader election heartbeats.
	//
	// If empty, Raft state is stored as a subdirectory of the data volume.
	//
	// Maps to: Raft directory at {db_path}/raft/ (replication/election.go)
	// +optional
	RaftSize *resource.Quantity `json:"raftSize,omitempty"`

	// StorageClassName is the Kubernetes StorageClass to use for all PVCs.
	// If not specified, the cluster's default StorageClass is used.
	//
	// For production, use an SSD-backed StorageClass. Example:
	//   AWS:  "gp3" or "io2"
	//   GCP:  "premium-rwo"
	//   Azure: "managed-premium"
	//
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
}

// PortSpec defines the network ports for GoraphDB services.
//
// GoraphDB uses three ports for different protocols:
//   - HTTP:  Client API, health checks, Prometheus metrics, write forwarding
//   - Raft:  Hashicorp Raft transport (leader election, log replication)
//   - gRPC:  WAL streaming from leader to followers
//
// These map to the CLI flags in cmd/graphdb-ui/main.go:
//   -addr      → HTTP port
//   -raft-addr → Raft port
//   -grpc-addr → gRPC port
type PortSpec struct {
	// HTTP is the port for the GoraphDB HTTP API.
	// Serves: REST API, Cypher queries, health checks (/api/health),
	// Prometheus metrics (/metrics), cluster status (/api/cluster),
	// and the management UI.
	//
	// Maps to: -addr flag (default ":7474")
	// Source: cmd/graphdb-ui/main.go:46
	// +kubebuilder:default=7474
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	HTTP int32 `json:"http,omitempty"`

	// Raft is the port for Hashicorp Raft transport.
	// Used for leader election heartbeats and Raft log entries.
	// Only used when replicas > 1 (cluster mode).
	//
	// Maps to: -raft-addr flag (default "0.0.0.0:7000")
	// Source: cmd/graphdb-ui/main.go:52
	// +kubebuilder:default=7000
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Raft int32 `json:"raft,omitempty"`

	// GRPC is the port for gRPC WAL replication streaming.
	// The leader runs a gRPC server that streams WAL entries to followers
	// via the StreamWAL RPC (replication/proto/replication.proto).
	// Only used when replicas > 1 (cluster mode).
	//
	// Maps to: -grpc-addr flag (default "0.0.0.0:7001")
	// Source: cmd/graphdb-ui/main.go:53
	// +kubebuilder:default=7001
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	GRPC int32 `json:"grpc,omitempty"`
}

// MonitoringSpec configures Prometheus monitoring integration.
//
// GoraphDB exposes Prometheus metrics at GET /metrics in text exposition
// format and GET /api/metrics as JSON. The metrics include:
//
// Counters:
//   - graphdb_queries_total           — all Cypher executions
//   - graphdb_slow_queries_total      — queries exceeding SlowQueryThreshold
//   - graphdb_query_errors_total      — failed queries
//   - graphdb_cache_hits_total        — node cache + query cache hits
//   - graphdb_cache_misses_total      — cache misses
//   - graphdb_nodes_created_total     — nodes created
//   - graphdb_nodes_deleted_total     — nodes deleted
//   - graphdb_edges_created_total     — edges created
//   - graphdb_edges_deleted_total     — edges deleted
//   - graphdb_index_lookups_total     — property index lookups
//   - graphdb_bloom_negatives_total   — bloom filter true negatives
//
// Gauges:
//   - graphdb_nodes_current           — current node count
//   - graphdb_edges_current           — current edge count
//   - graphdb_node_cache_bytes_used   — node cache memory usage
//   - graphdb_query_cache_entries     — LRU query cache size
//
// Source: metrics.go
type MonitoringSpec struct {
	// Enabled controls whether a Prometheus ServiceMonitor is created.
	// Requires the Prometheus Operator to be installed in the cluster.
	//
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`

	// Interval is the Prometheus scrape interval.
	// How often Prometheus collects metrics from /metrics.
	//
	// +kubebuilder:default="30s"
	Interval string `json:"interval,omitempty"`

	// ScrapeTimeout is the maximum time for a single scrape.
	// Must be less than Interval.
	//
	// +kubebuilder:default="10s"
	ScrapeTimeout string `json:"scrapeTimeout,omitempty"`
}

// PDBSpec configures the PodDisruptionBudget for the GoraphDB cluster.
//
// The PDB protects Raft quorum during voluntary disruptions (kubectl drain,
// node upgrades, etc.). The operator calculates a safe default:
//
//	minAvailable = ceil(replicas / 2)
//
// This ensures enough nodes remain to maintain Raft quorum:
//   - 3 replicas → minAvailable=2 (quorum=2, 1 disruption allowed)
//   - 5 replicas → minAvailable=3 (quorum=3, 2 disruptions allowed)
type PDBSpec struct {
	// MinAvailable is the minimum number of pods that must remain available
	// during voluntary disruptions. Overrides the operator's default
	// quorum-based calculation.
	//
	// +optional
	MinAvailable *int32 `json:"minAvailable,omitempty"`
}

// ============================================================================
// Status — Observed State
// ============================================================================

// GoraphDBClusterStatus defines the observed state of a GoraphDB cluster.
// The operator continuously updates this status by polling the /api/health
// and /api/cluster endpoints on each pod.
type GoraphDBClusterStatus struct {
	// Phase is the high-level lifecycle state of the cluster.
	Phase ClusterPhase `json:"phase,omitempty"`

	// ReadyReplicas is the number of pods that are healthy and ready
	// to serve traffic (passing readiness probe).
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Leader is the pod name of the current Raft leader.
	// Empty in standalone mode or when no leader is elected.
	//
	// Determined by: GET /api/cluster → response.leader_id
	// Source: server/server.go handleClusterStatus()
	Leader string `json:"leader,omitempty"`

	// CurrentImage is the container image currently running.
	CurrentImage string `json:"currentImage,omitempty"`

	// Members is the per-pod status of each cluster member.
	Members []MemberStatus `json:"members,omitempty"`

	// Conditions represent the latest available observations of the
	// cluster's current state. Standard condition types:
	//   - Available:   at least one pod is serving traffic
	//   - Progressing: a rollout or scaling operation is in progress
	//   - Degraded:    fewer than desired replicas are ready
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// MemberStatus represents the observed state of a single cluster member (pod).
type MemberStatus struct {
	// Name is the pod name (e.g., "goraphdb-sample-0").
	Name string `json:"name"`

	// Role is the node's current role: "leader", "follower", or "standalone".
	//
	// Determined by: GET /api/health → response.role
	// Source: server/server.go handleHealth()
	Role string `json:"role"`

	// Ready indicates whether the pod is passing health checks.
	Ready bool `json:"ready"`

	// WAL_LSN is the last WAL Log Sequence Number on this node.
	// For leaders, this is the latest committed LSN.
	// For followers, this indicates replication progress.
	//
	// Determined by: GET /api/cluster → response.wal_last_lsn / applied_lsn
	// Source: replication/cluster.go WALLastLSN() / AppliedLSN()
	WAL_LSN uint64 `json:"walLSN,omitempty"`

	// Health is the health status: "ok", "readonly", or "unavailable".
	//
	// "ok"       — fully operational (reads + writes)
	// "readonly" — reads work but no leader available for writes
	// "unavailable" — pod is not responding
	//
	// Determined by: GET /api/health → response.status
	// Source: server/server.go handleHealth()
	Health string `json:"health"`
}

// ClusterPhase represents the high-level lifecycle state of the cluster.
type ClusterPhase string

const (
	// ClusterPhaseCreating means the operator is creating initial resources
	// (StatefulSet, Services, ConfigMap) for the first time.
	ClusterPhaseCreating ClusterPhase = "Creating"

	// ClusterPhaseBootstrapping means the cluster is performing initial
	// Raft bootstrap. Pod-0 is starting with -bootstrap flag and the
	// operator is waiting for the first leader election to complete.
	ClusterPhaseBootstrapping ClusterPhase = "Bootstrapping"

	// ClusterPhaseRunning means all desired replicas are ready and the
	// cluster has a healthy leader (or is running in standalone mode).
	ClusterPhaseRunning ClusterPhase = "Running"

	// ClusterPhaseScaling means the StatefulSet is scaling up or down.
	// During scale-up, new pods join the Raft cluster and catch up via WAL.
	// During scale-down, pods are removed from Raft before termination.
	ClusterPhaseScaling ClusterPhase = "Scaling"

	// ClusterPhaseDegraded means the cluster is operational but not fully
	// healthy. Some replicas may be down or replication may be lagging.
	// The cluster can still serve reads (and writes if a leader exists).
	ClusterPhaseDegraded ClusterPhase = "Degraded"

	// ClusterPhaseFailed means the cluster cannot serve traffic.
	// All pods are down or Raft quorum is lost.
	ClusterPhaseFailed ClusterPhase = "Failed"
)

// DeepCopyObject implements runtime.Object for GoraphDBCluster.
func (in *GoraphDBCluster) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(GoraphDBCluster)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies all fields into another GoraphDBCluster.
func (in *GoraphDBCluster) DeepCopyInto(out *GoraphDBCluster) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopyObject implements runtime.Object for GoraphDBClusterList.
func (in *GoraphDBClusterList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(GoraphDBClusterList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies all fields into another GoraphDBClusterList.
func (in *GoraphDBClusterList) DeepCopyInto(out *GoraphDBClusterList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]GoraphDBCluster, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

// DeepCopyInto for GoraphDBClusterSpec.
func (in *GoraphDBClusterSpec) DeepCopyInto(out *GoraphDBClusterSpec) {
	*out = *in
	if in.Replicas != nil {
		out.Replicas = new(int32)
		*out.Replicas = *in.Replicas
	}
	if in.ShardCount != nil {
		out.ShardCount = new(int32)
		*out.ShardCount = *in.ShardCount
	}
	if in.WorkerPoolSize != nil {
		out.WorkerPoolSize = new(int32)
		*out.WorkerPoolSize = *in.WorkerPoolSize
	}
	if in.NoSync != nil {
		out.NoSync = new(bool)
		*out.NoSync = *in.NoSync
	}
	if in.MaxResultRows != nil {
		out.MaxResultRows = new(int32)
		*out.MaxResultRows = *in.MaxResultRows
	}
	if in.WriteQueueSize != nil {
		out.WriteQueueSize = new(int32)
		*out.WriteQueueSize = *in.WriteQueueSize
	}
	out.CacheBudget = in.CacheBudget.DeepCopy()
	out.MmapSize = in.MmapSize.DeepCopy()
	in.Storage.DeepCopyInto(&out.Storage)
	in.Resources.DeepCopyInto(&out.Resources)
	if in.Affinity != nil {
		out.Affinity = in.Affinity.DeepCopy()
	}
	if in.Tolerations != nil {
		out.Tolerations = make([]corev1.Toleration, len(in.Tolerations))
		copy(out.Tolerations, in.Tolerations)
	}
	if in.NodeSelector != nil {
		out.NodeSelector = make(map[string]string, len(in.NodeSelector))
		for k, v := range in.NodeSelector {
			out.NodeSelector[k] = v
		}
	}
	if in.PodDisruptionBudget != nil {
		out.PodDisruptionBudget = new(PDBSpec)
		in.PodDisruptionBudget.DeepCopyInto(out.PodDisruptionBudget)
	}
	if in.Monitoring.Enabled != nil {
		out.Monitoring.Enabled = new(bool)
		*out.Monitoring.Enabled = *in.Monitoring.Enabled
	}
}

// DeepCopyInto for StorageSpec.
func (in *StorageSpec) DeepCopyInto(out *StorageSpec) {
	*out = *in
	out.DataSize = in.DataSize.DeepCopy()
	if in.WALSize != nil {
		q := in.WALSize.DeepCopy()
		out.WALSize = &q
	}
	if in.RaftSize != nil {
		q := in.RaftSize.DeepCopy()
		out.RaftSize = &q
	}
	if in.StorageClassName != nil {
		out.StorageClassName = new(string)
		*out.StorageClassName = *in.StorageClassName
	}
}

// DeepCopyInto for GoraphDBClusterStatus.
func (in *GoraphDBClusterStatus) DeepCopyInto(out *GoraphDBClusterStatus) {
	*out = *in
	if in.Members != nil {
		out.Members = make([]MemberStatus, len(in.Members))
		copy(out.Members, in.Members)
	}
	if in.Conditions != nil {
		out.Conditions = make([]metav1.Condition, len(in.Conditions))
		for i := range in.Conditions {
			in.Conditions[i].DeepCopyInto(&out.Conditions[i])
		}
	}
}

// DeepCopyInto for PDBSpec.
func (in *PDBSpec) DeepCopyInto(out *PDBSpec) {
	*out = *in
	if in.MinAvailable != nil {
		out.MinAvailable = new(int32)
		*out.MinAvailable = *in.MinAvailable
	}
}
