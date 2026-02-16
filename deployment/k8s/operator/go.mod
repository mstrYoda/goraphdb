module github.com/mstrYoda/goraphdb/operator

go 1.24

// ---------------------------------------------------------------------------
// Dependencies
//
// This operator uses controller-runtime (the standard framework behind
// kubebuilder and operator-sdk) to reconcile GoraphDBCluster custom resources
// into Kubernetes-native objects (StatefulSets, Services, ConfigMaps).
//
// Run `go mod tidy` after cloning to resolve the full dependency tree.
// ---------------------------------------------------------------------------

require (
	k8s.io/api v0.32.0
	k8s.io/apimachinery v0.32.0
	k8s.io/client-go v0.32.0
	sigs.k8s.io/controller-runtime v0.20.0
)
