package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RedisClusterSpec defines the desired state of RedisCluster
type RedisClusterSpec struct {
	// Replicas is the number of Redis cluster nodes (must be >= 3)
	// +kubebuilder:validation:Minimum=3
	Replicas int32 `json:"replicas"`

	// PasswordSecretRef references the secret containing REDIS_PASSWORD
	PasswordSecretRef SecretRef `json:"passwordSecretRef"`

	// StatefulSetName is the name of the StatefulSet managing Redis pods
	// +optional
	StatefulSetName string `json:"statefulSetName,omitempty"`

	// MinReady is the minimum number of ready pods before cluster bootstrap (default: 3)
	// +kubebuilder:validation:Minimum=3
	// +kubebuilder:default=3
	// +optional
	MinReady int32 `json:"minReady,omitempty"`

	// AutoRebalance enables automatic slot rebalancing on scale down
	// +optional
	AutoRebalance bool `json:"autoRebalance,omitempty"`

	// AutoPvcCleanup enables automatic PVC deletion on scale down
	// +optional
	AutoPvcCleanup bool `json:"autoPvcCleanup,omitempty"`

	// ServiceName is the headless service name (default: redis-headless)
	// +optional
	ServiceName string `json:"serviceName,omitempty"`

	// Namespace for the Redis resources (default: same as RedisCluster)
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// SecretRef references a secret
type SecretRef struct {
	// Name is the name of the secret
	Name string `json:"name"`

	// Key is the key in the secret (default: REDIS_PASSWORD)
	// +optional
	Key string `json:"key,omitempty"`
}

// RedisClusterStatus defines the observed state of RedisCluster
type RedisClusterStatus struct {
	// Phase represents the current phase of the cluster
	// +optional
	Phase string `json:"phase,omitempty"`

	// ReadyReplicas is the number of ready Redis pods
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// ClusterState is the Redis cluster state (ok, fail, etc.)
	// +optional
	ClusterState string `json:"clusterState,omitempty"`

	// ClusterNodes is the number of nodes in the cluster
	// +optional
	ClusterNodes int32 `json:"clusterNodes,omitempty"`

	// LastError contains the last error message if any
	// +optional
	LastError string `json:"lastError,omitempty"`

	// Conditions represent the latest available observations
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
//+kubebuilder:printcolumn:name="Ready",type="integer",JSONPath=".status.readyReplicas"
//+kubebuilder:printcolumn:name="Cluster State",type="string",JSONPath=".status.clusterState"
//+kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// RedisCluster is the Schema for the redisclusters API
type RedisCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RedisClusterSpec   `json:"spec,omitempty"`
	Status RedisClusterStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// RedisClusterList contains a list of RedisCluster
type RedisClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RedisCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RedisCluster{}, &RedisClusterList{})
}
