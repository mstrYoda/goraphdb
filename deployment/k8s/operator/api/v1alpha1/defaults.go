package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
)

// ============================================================================
// Default Values
//
// These defaults mirror graphdb.DefaultOptions() in types.go and the CLI
// flag defaults in cmd/graphdb-ui/main.go. They ensure that a minimal CRD
// spec (just image + storage) produces a working cluster with sensible
// production-ready settings.
//
// Reference: types.go:195-205 (DefaultOptions function)
// ============================================================================

// SetDefaults applies default values to a GoraphDBClusterSpec.
// Called by the reconciler before building Kubernetes resources.
func SetDefaults(spec *GoraphDBClusterSpec) {
	// --- Cluster Topology ---

	if spec.Replicas == nil {
		replicas := int32(1)
		spec.Replicas = &replicas
	}

	if spec.ImagePullPolicy == "" {
		spec.ImagePullPolicy = "IfNotPresent"
	}

	// --- Storage Engine ---
	// Maps to graphdb.DefaultOptions() in types.go:195-205

	if spec.ShardCount == nil {
		// Default: 1 shard (same as graphdb.DefaultOptions().ShardCount)
		shards := int32(1)
		spec.ShardCount = &shards
	}

	if spec.WorkerPoolSize == nil {
		// Default: 8 workers (same as graphdb.DefaultOptions().WorkerPoolSize)
		workers := int32(8)
		spec.WorkerPoolSize = &workers
	}

	if spec.CacheBudget.IsZero() {
		// Default: 128Mi (same as graphdb.DefaultOptions().CacheBudget = 128*1024*1024)
		spec.CacheBudget = resource.MustParse("128Mi")
	}

	if spec.MmapSize.IsZero() {
		// Default: 256Mi (same as graphdb.DefaultOptions().MmapSize = 256*1024*1024)
		spec.MmapSize = resource.MustParse("256Mi")
	}

	if spec.NoSync == nil {
		// Default: true (same as graphdb.DefaultOptions().NoSync)
		// NoSync=true gives ~20x write throughput with 200ms max data loss window.
		// In cluster mode, WAL provides additional durability (2ms group commit).
		noSync := true
		spec.NoSync = &noSync
	}

	// --- Query Governor ---

	if spec.SlowQueryThreshold.Duration == 0 {
		// We don't set a default here because Duration==0 means "disabled"
		// in goraphdb, and the CRD default ("100ms") is set via kubebuilder tags.
		// The reconciler uses the value as-is.
	}

	if spec.MaxResultRows == nil {
		maxRows := int32(0) // 0 = unlimited (same as goraphdb default)
		spec.MaxResultRows = &maxRows
	}

	// --- Write Backpressure ---

	if spec.WriteQueueSize == nil {
		// Default: 64 (same as graphdb default, sufficient for most workloads)
		queueSize := int32(64)
		spec.WriteQueueSize = &queueSize
	}

	// --- Storage ---

	if spec.Storage.DataSize.IsZero() {
		spec.Storage.DataSize = resource.MustParse("10Gi")
	}

	// --- Ports ---
	// Maps to CLI flags in cmd/graphdb-ui/main.go:46-53

	if spec.Ports.HTTP == 0 {
		spec.Ports.HTTP = 7474 // GoraphDB default HTTP port
	}
	if spec.Ports.Raft == 0 {
		spec.Ports.Raft = 7000 // GoraphDB default Raft port
	}
	if spec.Ports.GRPC == 0 {
		spec.Ports.GRPC = 7001 // GoraphDB default gRPC port
	}

	// --- Monitoring ---

	if spec.Monitoring.Enabled == nil {
		enabled := true
		spec.Monitoring.Enabled = &enabled
	}
	if spec.Monitoring.Interval == "" {
		spec.Monitoring.Interval = "30s"
	}
	if spec.Monitoring.ScrapeTimeout == "" {
		spec.Monitoring.ScrapeTimeout = "10s"
	}
}
