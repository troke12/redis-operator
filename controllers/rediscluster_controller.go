package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	redisv1alpha1 "github.com/redis-operator/redis-operator/api/v1alpha1"
	"github.com/redis-operator/redis-operator/pkg/rediscli"
)

// RedisClusterReconciler reconciles a RedisCluster object
type RedisClusterReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Clientset kubernetes.Interface
	Log       logr.Logger
}

//+kubebuilder:rbac:groups=redis.redis-operator.io,resources=redisclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=redis.redis-operator.io,resources=redisclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=redis.redis-operator.io,resources=redisclusters/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete

// Reconcile is part of the main kubernetes reconciliation loop
func (r *RedisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	redisCluster := &redisv1alpha1.RedisCluster{}
	if err := r.Get(ctx, req.NamespacedName, redisCluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Set defaults
	if redisCluster.Spec.MinReady == 0 {
		redisCluster.Spec.MinReady = 3
	}
	if redisCluster.Spec.ServiceName == "" {
		redisCluster.Spec.ServiceName = "redis-headless"
	}
	targetNs := redisCluster.Spec.Namespace
	if targetNs == "" {
		targetNs = redisCluster.Namespace
	}
	statefulSetName := redisCluster.Spec.StatefulSetName
	if statefulSetName == "" {
		statefulSetName = "redis"
	}

	// Get password from secret
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Name:      redisCluster.Spec.PasswordSecretRef.Name,
		Namespace: targetNs,
	}
	if err := r.Get(ctx, secretKey, secret); err != nil {
		return r.updateStatus(ctx, redisCluster, "Error", "", fmt.Sprintf("failed to get password secret: %v", err))
	}

	passwordKey := redisCluster.Spec.PasswordSecretRef.Key
	if passwordKey == "" {
		passwordKey = "REDIS_PASSWORD"
	}
	password := string(secret.Data[passwordKey])
	if password == "" {
		return r.updateStatus(ctx, redisCluster, "Error", "", "password secret key not found or empty")
	}

	// Get StatefulSet
	statefulSet := &appsv1.StatefulSet{}
	ssKey := types.NamespacedName{
		Name:      statefulSetName,
		Namespace: targetNs,
	}
	if err := r.Get(ctx, ssKey, statefulSet); err != nil {
		if apierrors.IsNotFound(err) {
			return r.updateStatus(ctx, redisCluster, "Pending", "", fmt.Sprintf("StatefulSet %s not found", statefulSetName))
		}
		return r.updateStatus(ctx, redisCluster, "Error", "", fmt.Sprintf("failed to get StatefulSet: %v", err))
	}

	// List pods
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(targetNs), client.MatchingLabels(statefulSet.Spec.Selector.MatchLabels)); err != nil {
		return r.updateStatus(ctx, redisCluster, "Error", "", fmt.Sprintf("failed to list pods: %v", err))
	}

	// Filter ready pods
	var readyPods []corev1.Pod
	var readyEndpoints []string
	for _, pod := range pods.Items {
		if isPodReady(&pod) {
			readyPods = append(readyPods, pod)
			// Redis 6.2 cluster meet expects IP:port (hostnames may be rejected)
			endpoint := buildEndpoint(pod.Status.PodIP)
			readyEndpoints = append(readyEndpoints, endpoint)
		}
	}

	readyCount := int32(len(readyPods))
	redisCluster.Status.ReadyReplicas = readyCount

	// Check minimum ready requirement
	if readyCount < redisCluster.Spec.MinReady {
		return r.updateStatus(ctx, redisCluster, "Pending", "", fmt.Sprintf("waiting for %d ready pods (current: %d)", redisCluster.Spec.MinReady, readyCount))
	}

	// Create redis-cli runner
	runner := rediscli.NewRunner(r.Client, r.Clientset, targetNs, password)

	// Get cluster state from first ready pod
	firstEndpoint := readyEndpoints[0]
	clusterInfo, err := runner.ClusterInfo(ctx, firstEndpoint)
	if err != nil {
		logger.Info("Failed to get cluster info, cluster may not exist yet", "error", err)
		clusterInfo = nil
	}

	clusterState := ""
	if clusterInfo != nil {
		clusterState = clusterInfo["cluster_state"]
	}

	// Bootstrap cluster if needed
	if clusterState == "" || clusterState == "fail" {
		if readyCount >= redisCluster.Spec.MinReady {
			logger.Info("Bootstrapping Redis cluster", "endpoints", readyEndpoints)
			if err := runner.ClusterCreate(ctx, readyEndpoints); err != nil {
				return r.updateStatus(ctx, redisCluster, "Error", clusterState, fmt.Sprintf("failed to create cluster: %v", err))
			}
			// Re-check cluster state
			time.Sleep(2 * time.Second)
			clusterInfo, err = runner.ClusterInfo(ctx, firstEndpoint)
			if err == nil && clusterInfo != nil {
				clusterState = clusterInfo["cluster_state"]
			}
		}
	}

	// Get current cluster nodes
	clusterNodes, err := runner.ClusterNodes(ctx, firstEndpoint)
	if err != nil {
		logger.Info("Failed to get cluster nodes", "error", err)
		clusterNodes = nil
	}

	// Build endpoint to node ID map
	endpointToNodeID := make(map[string]string)
	nodeIDToEndpoint := make(map[string]string)
	for _, node := range clusterNodes {
		endpointToNodeID[node.Addr] = node.ID
		nodeIDToEndpoint[node.ID] = node.Addr
	}

	// Handle scale up: add missing nodes
	for _, endpoint := range readyEndpoints {
		if _, exists := endpointToNodeID[endpoint]; !exists {
			logger.Info("Adding node to cluster", "endpoint", endpoint)
			if err := runner.ClusterAddNode(ctx, endpoint, firstEndpoint); err != nil {
				logger.Error(err, "Failed to add node", "endpoint", endpoint)
				continue
			}
			// Rebalance if enabled
			if redisCluster.Spec.AutoRebalance {
				time.Sleep(1 * time.Second)
				if err := runner.ClusterRebalance(ctx, firstEndpoint, true); err != nil {
					logger.Error(err, "Failed to rebalance cluster")
				}
			}
		}
	}

	// Handle scale down: remove nodes that no longer exist
	for _, node := range clusterNodes {
		endpoint := node.Addr
		// Check if this endpoint corresponds to a ready pod
		found := false
		for _, ep := range readyEndpoints {
			if ep == endpoint || strings.Contains(endpoint, ep) {
				found = true
				break
			}
		}
		// Also check by pod name pattern
		if !found {
			for _, pod := range readyPods {
				if strings.Contains(endpoint, pod.Name) {
					found = true
					break
				}
			}
		}

		if !found {
			logger.Info("Removing node from cluster", "nodeID", node.ID, "endpoint", endpoint)
			// If node has slots, migrate them first
			if strings.Contains(node.Flags, "master") && redisCluster.Spec.AutoRebalance {
				logger.Info("Rebalancing before removing master node", "nodeID", node.ID)
				if err := runner.ClusterRebalance(ctx, firstEndpoint, false); err != nil {
					logger.Error(err, "Failed to rebalance before removal")
				}
			}
			// Remove node
			if err := runner.ClusterDelNode(ctx, firstEndpoint, node.ID); err != nil {
				logger.Error(err, "Failed to delete node", "nodeID", node.ID)
			} else {
				// Forget node from all remaining nodes
				for _, remainingNode := range clusterNodes {
					if remainingNode.ID != node.ID {
						_ = runner.ClusterForget(ctx, remainingNode.Addr, node.ID)
					}
				}
			}
		}
	}

	// Update status
	return r.updateStatus(ctx, redisCluster, "Ready", clusterState, "")
}

func (r *RedisClusterReconciler) updateStatus(ctx context.Context, rc *redisv1alpha1.RedisCluster, phase, clusterState, lastError string) (ctrl.Result, error) {
	rc.Status.Phase = phase
	rc.Status.ClusterState = clusterState
	rc.Status.LastError = lastError
	if clusterState != "" {
		rc.Status.ClusterNodes = rc.Status.ReadyReplicas
	}

	if err := r.Status().Update(ctx, rc); err != nil {
		return ctrl.Result{}, err
	}

	if phase == "Pending" || phase == "Error" {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func isPodReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func buildEndpoint(podIP string) string {
	return fmt.Sprintf("%s:6379", podIP)
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redisv1alpha1.RedisCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}
