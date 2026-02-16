// Package v1alpha1 contains API schema definitions for GoraphDB v1alpha1.
//
// The v1alpha1 version indicates this API is in early development and may
// change without notice. Once the operator reaches stability, we will
// promote to v1beta1 and eventually v1 with conversion webhooks.
//
// API Group: goraphdb.io
// Version:   v1alpha1
// Kind:      GoraphDBCluster
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the API group and version for GoraphDB resources.
	GroupVersion = schema.GroupVersion{Group: "goraphdb.io", Version: "v1alpha1"}

	// SchemeBuilder is used to add Go types to the GroupVersionResource scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the types in this group-version to the given scheme.
	// Called from main.go during operator initialization.
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes registers GoraphDBCluster and GoraphDBClusterList with the scheme.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion,
		&GoraphDBCluster{},
		&GoraphDBClusterList{},
	)
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
