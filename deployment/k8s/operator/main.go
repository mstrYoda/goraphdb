// Package main is the entrypoint for the GoraphDB Kubernetes Operator.
//
// The operator watches GoraphDBCluster custom resources and reconciles them
// into the appropriate Kubernetes objects:
//
//   - StatefulSet:    one pod per database node with stable network identity
//   - Headless Svc:   peer discovery for Raft election and gRPC replication
//   - Client Svc:     leader-only service for read/write traffic
//   - Read Svc:       all-replicas service for read-only traffic
//   - ConfigMap:      entrypoint script that computes cluster topology from DNS
//   - PDB:            PodDisruptionBudget to protect Raft quorum
//   - ServiceMonitor: Prometheus scraping of /metrics endpoint
//
// Architecture decision: we use controller-runtime (the framework behind
// kubebuilder) rather than a raw client-go informer because it provides
// structured reconciliation, leader election for the operator itself,
// health/ready probes, and metrics out of the box.
package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	goraphdbv1alpha1 "github.com/mstrYoda/goraphdb/operator/api/v1alpha1"
	"github.com/mstrYoda/goraphdb/operator/controllers"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	// Register core Kubernetes types (Pod, Service, StatefulSet, etc.)
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	// Register our custom GoraphDBCluster type so the controller can
	// watch and reconcile it.
	utilruntime.Must(goraphdbv1alpha1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var probeAddr string
	var enableLeaderElection bool

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080",
		"The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081",
		"The address the health probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for the operator controller manager. "+
			"Enabling this ensures only one active controller manager instance "+
			"processes GoraphDBCluster resources, preventing duplicate reconciliation.")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Create the controller manager. This is the top-level component that:
	// - Maintains a shared cache of Kubernetes objects
	// - Runs controllers (our GoraphDBCluster reconciler)
	// - Serves metrics and health probes
	// - Handles leader election when running multiple operator replicas
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "goraphdb-operator-lock",
		// LeaderElectionID is used as the name of the ConfigMap/Lease object
		// for operator-level leader election. This is separate from the
		// Raft-based leader election inside GoraphDB itself.
	})
	if err != nil {
		setupLog.Error(err, "unable to create manager")
		os.Exit(1)
	}

	// Register the GoraphDBCluster reconciler.
	if err := (&controllers.GoraphDBClusterReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Log:    ctrl.Log.WithName("controllers").WithName("GoraphDBCluster"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "GoraphDBCluster")
		os.Exit(1)
	}

	// Health and readiness probes for the operator itself.
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting GoraphDB operator")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
