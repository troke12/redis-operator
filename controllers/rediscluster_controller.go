package controllers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	redisv1alpha1 "github.com/troke12/redis-operator/api/v1alpha1"
	"github.com/troke12/redis-operator/pkg/rediscli"
)

// RedisClusterReconciler reconciles a RedisCluster object
type RedisClusterReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Clientset  kubernetes.Interface
	RestConfig *rest.Config
	Log        logr.Logger
}

//+kubebuilder:rbac:groups=redis.redis-operator.io,resources=redisclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=redis.redis-operator.io,resources=redisclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=redis.redis-operator.io,resources=redisclusters/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile is the main reconciliation loop
func (r *RedisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Get the RedisCluster resource
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
	password, err := r.getPassword(ctx, redisCluster, targetNs)
	if err != nil {
		return r.updateStatus(ctx, redisCluster, "Error", "", err.Error())
	}

	// Get StatefulSet
	statefulSet := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: statefulSetName, Namespace: targetNs}, statefulSet); err != nil {
		if apierrors.IsNotFound(err) {
			return r.updateStatus(ctx, redisCluster, "Pending", "", fmt.Sprintf("StatefulSet %s not found", statefulSetName))
		}
		return r.updateStatus(ctx, redisCluster, "Error", "", fmt.Sprintf("failed to get StatefulSet: %v", err))
	}

	// Get ready pods and their endpoints
	readyEndpoints, err := r.getReadyEndpoints(ctx, targetNs, statefulSet)
	if err != nil {
		return r.updateStatus(ctx, redisCluster, "Error", "", err.Error())
	}

	readyCount := int32(len(readyEndpoints))
	redisCluster.Status.ReadyReplicas = readyCount

	// Wait for minimum pods to be ready
	if readyCount < redisCluster.Spec.MinReady {
		return r.updateStatus(ctx, redisCluster, "Pending", "",
			fmt.Sprintf("waiting for %d ready pods (current: %d)", redisCluster.Spec.MinReady, readyCount))
	}

	// Create the redis-cli runner
	runner := rediscli.NewRunner(r.Client, r.Clientset, r.RestConfig, targetNs, password)

	// Get current cluster state
	firstEndpoint := readyEndpoints[0]
	clusterInfo, _ := runner.ClusterInfo(ctx, firstEndpoint)
	clusterState := ""
	if clusterInfo != nil {
		clusterState = clusterInfo["cluster_state"]
	}

	// Get cluster nodes
	clusterNodes, _ := runner.ClusterNodes(ctx, firstEndpoint)

	// CASE 1: No cluster exists yet - need to bootstrap
	if len(clusterNodes) == 0 || (clusterState == "" && len(clusterNodes) <= 1) {
		logger.Info("No cluster detected, bootstrapping", "readyEndpoints", len(readyEndpoints))
		return r.bootstrapCluster(ctx, logger, runner, redisCluster, readyEndpoints)
	}

	// CASE 2: Cluster exists but is in fail state with no slots
	if clusterState == "fail" {
		slotsAssigned := 0
		if clusterInfo != nil {
			fmt.Sscanf(clusterInfo["cluster_slots_assigned"], "%d", &slotsAssigned)
		}

		if slotsAssigned == 0 {
			logger.Info("Cluster in fail state with no slots, attempting to fix or re-bootstrap")
			// Try to fix first
			if err := runner.ClusterFix(ctx, firstEndpoint); err != nil {
				logger.Info("Fix failed, will try to continue", "error", err)
			}
			time.Sleep(2 * time.Second)

			// Re-check
			clusterInfo, _ = runner.ClusterInfo(ctx, firstEndpoint)
			if clusterInfo != nil {
				clusterState = clusterInfo["cluster_state"]
			}
		}
	}

	// Build maps for quick lookup
	endpointToNodeID := make(map[string]string)
	nodeIDToEndpoint := make(map[string]string)
	for _, node := range clusterNodes {
		endpointToNodeID[node.Addr] = node.ID
		nodeIDToEndpoint[node.ID] = node.Addr
	}

	// CASE 3: Scale up - add new nodes
	var newEndpoints []string
	for _, endpoint := range readyEndpoints {
		if _, exists := endpointToNodeID[endpoint]; !exists {
			newEndpoints = append(newEndpoints, endpoint)
		}
	}

	if len(newEndpoints) > 0 {
		logger.Info("Scale up detected", "newNodes", len(newEndpoints))
		if err := r.scaleUp(ctx, logger, runner, redisCluster, firstEndpoint, newEndpoints); err != nil {
			return r.updateStatus(ctx, redisCluster, "Error", clusterState, fmt.Sprintf("scale-up failed: %v", err))
		}
	}

	// CASE 4: Scale down - remove nodes that no longer exist
	readyIPs := make(map[string]bool)
	for _, endpoint := range readyEndpoints {
		ip := strings.Split(endpoint, ":")[0]
		readyIPs[ip] = true
	}

	var nodesToRemove []rediscli.NodeInfo
	for _, node := range clusterNodes {
		nodeIP := strings.Split(node.Addr, ":")[0]
		if !readyIPs[nodeIP] {
			nodesToRemove = append(nodesToRemove, node)
		}
	}

	if len(nodesToRemove) > 0 {
		logger.Info("Scale down detected", "nodesToRemove", len(nodesToRemove))
		if err := r.scaleDown(ctx, logger, runner, clusterNodes, nodesToRemove, firstEndpoint, redisCluster.Spec.AutoRebalance); err != nil {
			return r.updateStatus(ctx, redisCluster, "Error", clusterState, fmt.Sprintf("scale-down failed: %v", err))
		}
	}

	// Get final cluster state
	clusterInfo, _ = runner.ClusterInfo(ctx, firstEndpoint)
	if clusterInfo != nil {
		clusterState = clusterInfo["cluster_state"]
	}

	return r.updateStatus(ctx, redisCluster, "Ready", clusterState, "")
}

// getPassword retrieves the Redis password from the secret
func (r *RedisClusterReconciler) getPassword(ctx context.Context, rc *redisv1alpha1.RedisCluster, namespace string) (string, error) {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: rc.Spec.PasswordSecretRef.Name, Namespace: namespace}, secret); err != nil {
		return "", fmt.Errorf("failed to get password secret: %w", err)
	}

	key := rc.Spec.PasswordSecretRef.Key
	if key == "" {
		key = "REDIS_PASSWORD"
	}

	password := string(secret.Data[key])
	if password == "" {
		return "", fmt.Errorf("password secret key %s is empty", key)
	}

	return password, nil
}

// getReadyEndpoints returns the endpoints of all ready Redis pods
func (r *RedisClusterReconciler) getReadyEndpoints(ctx context.Context, namespace string, ss *appsv1.StatefulSet) ([]string, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabels(ss.Spec.Selector.MatchLabels)); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	var endpoints []string
	for _, pod := range pods.Items {
		if isPodReady(&pod) && pod.Status.PodIP != "" {
			endpoints = append(endpoints, fmt.Sprintf("%s:6379", pod.Status.PodIP))
		}
	}

	return endpoints, nil
}

// bootstrapCluster creates a new Redis cluster
func (r *RedisClusterReconciler) bootstrapCluster(
	ctx context.Context,
	logger logr.Logger,
	runner *rediscli.Runner,
	rc *redisv1alpha1.RedisCluster,
	endpoints []string,
) (ctrl.Result, error) {
	logger.Info("Bootstrapping Redis cluster", "endpoints", endpoints)

	// Reset all nodes first to ensure clean state
	for _, endpoint := range endpoints {
		if err := runner.ClusterReset(ctx, endpoint, true); err != nil {
			logger.Info("Reset failed (may be ok for fresh nodes)", "endpoint", endpoint, "error", err)
		}
	}

	time.Sleep(1 * time.Second)

	// Create the cluster
	if err := runner.ClusterCreate(ctx, endpoints); err != nil {
		if strings.Contains(err.Error(), "is not empty") {
			// Nodes have data, try to flush and retry
			logger.Info("Nodes not empty, attempting flush and retry")
			for _, endpoint := range endpoints {
				_ = runner.FlushAll(ctx, endpoint)
				_ = runner.ClusterReset(ctx, endpoint, true)
			}
			time.Sleep(1 * time.Second)

			// Retry
			if err := runner.ClusterCreate(ctx, endpoints); err != nil {
				return r.updateStatus(ctx, rc, "Error", "", fmt.Sprintf("cluster create failed after flush: %v", err))
			}
		} else {
			return r.updateStatus(ctx, rc, "Error", "", fmt.Sprintf("cluster create failed: %v", err))
		}
	}

	// Wait for cluster to stabilize
	time.Sleep(3 * time.Second)

	// Verify cluster state
	info, err := runner.ClusterInfo(ctx, endpoints[0])
	if err != nil {
		return r.updateStatus(ctx, rc, "Error", "", fmt.Sprintf("failed to verify cluster: %v", err))
	}

	clusterState := info["cluster_state"]
	if clusterState != "ok" {
		// Try to fix
		_ = runner.ClusterFix(ctx, endpoints[0])
		time.Sleep(2 * time.Second)

		info, _ = runner.ClusterInfo(ctx, endpoints[0])
		if info != nil {
			clusterState = info["cluster_state"]
		}
	}

	logger.Info("Cluster bootstrap completed", "state", clusterState)
	return r.updateStatus(ctx, rc, "Ready", clusterState, "")
}

// scaleUp adds new nodes to the cluster
func (r *RedisClusterReconciler) scaleUp(
	ctx context.Context,
	logger logr.Logger,
	runner *rediscli.Runner,
	rc *redisv1alpha1.RedisCluster,
	existingEndpoint string,
	newEndpoints []string,
) error {
	for _, newEndpoint := range newEndpoints {
		logger.Info("Adding node to cluster", "endpoint", newEndpoint)

		// First, reset the new node to ensure it's clean
		if err := runner.ClusterReset(ctx, newEndpoint, true); err != nil {
			logger.Info("Reset failed (may be ok)", "endpoint", newEndpoint, "error", err)
		}

		time.Sleep(500 * time.Millisecond)

		// Use CLUSTER MEET to add the node
		host := strings.Split(newEndpoint, ":")[0]
		if err := runner.ClusterMeet(ctx, existingEndpoint, host, 6379); err != nil {
			logger.Error(err, "Failed to add node via CLUSTER MEET", "endpoint", newEndpoint)
			continue
		}

		logger.Info("Node added successfully", "endpoint", newEndpoint)
		time.Sleep(1 * time.Second)
	}

	// Wait for nodes to be recognized
	time.Sleep(3 * time.Second)

	// Rebalance if enabled
	if rc.Spec.AutoRebalance {
		logger.Info("Rebalancing cluster after scale-up")
		if err := runner.ClusterRebalance(ctx, existingEndpoint, true); err != nil {
			logger.Error(err, "Rebalance failed, trying fix first")
			_ = runner.ClusterFix(ctx, existingEndpoint)
			time.Sleep(2 * time.Second)
			if retryErr := runner.ClusterRebalance(ctx, existingEndpoint, true); retryErr != nil {
				logger.Error(retryErr, "Rebalance failed after fix")
			}
		}
	}

	return nil
}

// scaleDown removes nodes from the cluster
func (r *RedisClusterReconciler) scaleDown(
	ctx context.Context,
	logger logr.Logger,
	runner *rediscli.Runner,
	allNodes []rediscli.NodeInfo,
	nodesToRemove []rediscli.NodeInfo,
	clusterEndpoint string,
	autoRebalance bool,
) error {
	// Separate masters and replicas
	var mastersToRemove, replicasToRemove []rediscli.NodeInfo
	removeNodeIDs := make(map[string]bool)

	for _, node := range nodesToRemove {
		removeNodeIDs[node.ID] = true
		if node.Role == "master" {
			mastersToRemove = append(mastersToRemove, node)
		} else {
			replicasToRemove = append(replicasToRemove, node)
		}
	}

	// Find remaining masters
	var remainingMasters []rediscli.NodeInfo
	for _, node := range allNodes {
		if node.Role == "master" && !removeNodeIDs[node.ID] {
			remainingMasters = append(remainingMasters, node)
		}
	}

	// Sort by slot count
	sort.Slice(remainingMasters, func(i, j int) bool {
		return remainingMasters[i].SlotCount() < remainingMasters[j].SlotCount()
	})

	// Migrate slots from masters being removed
	for _, master := range mastersToRemove {
		if master.HasSlots() && len(remainingMasters) > 0 {
			slotCount := master.SlotCount()
			logger.Info("Migrating slots from master", "nodeID", master.ID, "slots", slotCount)

			// Move all slots to the first remaining master
			target := remainingMasters[0]
			if err := runner.ClusterReshard(ctx, clusterEndpoint, master.ID, target.ID, slotCount); err != nil {
				logger.Error(err, "Failed to migrate slots", "from", master.ID, "to", target.ID)
			}

			time.Sleep(2 * time.Second)
		}
	}

	// Remove replicas first
	for _, replica := range replicasToRemove {
		logger.Info("Removing replica", "nodeID", replica.ID)
		if err := runner.ClusterDelNode(ctx, clusterEndpoint, replica.ID); err != nil {
			logger.Error(err, "Failed to remove replica", "nodeID", replica.ID)
		}
	}

	// Remove masters
	for _, master := range mastersToRemove {
		logger.Info("Removing master", "nodeID", master.ID)
		if err := runner.ClusterDelNode(ctx, clusterEndpoint, master.ID); err != nil {
			logger.Error(err, "Failed to remove master", "nodeID", master.ID)
		}
	}

	// Rebalance if enabled
	if autoRebalance && len(remainingMasters) > 0 {
		logger.Info("Rebalancing after scale-down")
		_ = runner.ClusterRebalance(ctx, clusterEndpoint, false)
	}

	return nil
}

// updateStatus updates the RedisCluster status
func (r *RedisClusterReconciler) updateStatus(ctx context.Context, rc *redisv1alpha1.RedisCluster, phase, clusterState, lastError string) (ctrl.Result, error) {
	rc.Status.Phase = phase
	rc.Status.ClusterState = clusterState
	rc.Status.LastError = lastError
	if clusterState != "" {
		rc.Status.ClusterNodes = rc.Status.ReadyReplicas
	}

	if err := r.Status().Update(ctx, rc); err != nil {
		// Ignore conflict errors, just requeue
		if apierrors.IsConflict(err) {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
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

// SetupWithManager sets up the controller with the Manager
func (r *RedisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redisv1alpha1.RedisCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}
