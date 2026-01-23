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
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

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
//+kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;delete

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
	// MinReady defaults to Replicas if set, otherwise 3
	if redisCluster.Spec.MinReady == 0 {
		if redisCluster.Spec.Replicas > 0 {
			redisCluster.Spec.MinReady = redisCluster.Spec.Replicas
		} else {
			redisCluster.Spec.MinReady = 3
		}
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

	// Sync StatefulSet replicas with RedisCluster spec
	// This ensures StatefulSet always matches the desired state from RedisCluster CRD
	desiredReplicas := redisCluster.Spec.Replicas
	currentStatefulSetReplicas := int32(0)
	if statefulSet.Spec.Replicas != nil {
		currentStatefulSetReplicas = *statefulSet.Spec.Replicas
	}

	if currentStatefulSetReplicas != desiredReplicas {
		logger.Info("StatefulSet replicas out of sync, updating",
			"current", currentStatefulSetReplicas,
			"desired", desiredReplicas)

		statefulSet.Spec.Replicas = &desiredReplicas
		if err := r.Update(ctx, statefulSet); err != nil {
			return r.updateStatus(ctx, redisCluster, "Error", "", fmt.Sprintf("failed to update StatefulSet replicas: %v", err))
		}

		logger.Info("StatefulSet replicas updated successfully",
			"statefulSet", statefulSetName,
			"replicas", desiredReplicas)

		// Requeue to wait for pods to be created/deleted
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
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

	// CASE 2: Cluster exists but is in fail state with no slots - needs re-bootstrap
	if clusterState == "fail" {
		slotsAssigned := 0
		if clusterInfo != nil {
			fmt.Sscanf(clusterInfo["cluster_slots_assigned"], "%d", &slotsAssigned)
		}

		if slotsAssigned == 0 {
			logger.Info("Cluster in fail state with no slots assigned, re-bootstrapping")
			// This happens when --cluster create was interrupted or failed
			// We need to reset and re-create the cluster
			return r.bootstrapCluster(ctx, logger, runner, redisCluster, readyEndpoints)
		}

		// If we have slots but cluster is in fail state, try to fix
		logger.Info("Cluster in fail state but has slots, attempting fix", "slotsAssigned", slotsAssigned)
		if err := runner.ClusterFix(ctx, firstEndpoint); err != nil {
			logger.Info("Fix failed, will try to continue", "error", err)
		}
		time.Sleep(2 * time.Second)

		// After fix, forget any nodes that are failed and have NO pod at all
		// (not just "not ready", but completely gone - this prevents data loss on pod restarts)
		clusterNodesAfterFix, _ := runner.ClusterNodes(ctx, firstEndpoint)

		// Get all pods (including not-ready) to check if they exist
		var podListForCleanup corev1.PodList
		allPodIPsForCleanup := make(map[string]bool)
		if err := r.List(ctx, &podListForCleanup, client.InNamespace(targetNs), client.MatchingLabels{
			"app": statefulSetName,
		}); err == nil {
			for _, pod := range podListForCleanup.Items {
				if pod.Status.PodIP != "" {
					allPodIPsForCleanup[pod.Status.PodIP] = true
				}
			}
		}

		var failedNodesToForget []string
		for _, node := range clusterNodesAfterFix {
			nodeIP := strings.Split(node.Addr, ":")[0]
			// Only forget if node is failed AND pod completely doesn't exist
			// This allows pods to restart/recover without losing their cluster membership
			if strings.Contains(node.Flags, "fail") && !allPodIPsForCleanup[nodeIP] {
				logger.Info("Found failed node with no pod, marking for forget",
					"nodeID", node.ID, "addr", node.Addr, "flags", node.Flags)
				failedNodesToForget = append(failedNodesToForget, node.ID)
			} else if strings.Contains(node.Flags, "fail") && allPodIPsForCleanup[nodeIP] {
				logger.Info("Failed node has pod (may be restarting), skipping forget to preserve data",
					"nodeID", node.ID, "addr", node.Addr, "flags", node.Flags)
			}
		}

		// Forget failed nodes from all healthy nodes
		if len(failedNodesToForget) > 0 {
			for _, endpoint := range readyEndpoints {
				for _, nodeID := range failedNodesToForget {
					if err := runner.ClusterForget(ctx, endpoint, nodeID); err != nil {
						logger.V(1).Info("Forget failed (may be ok)", "endpoint", endpoint, "nodeID", nodeID, "error", err)
					}
				}
			}
			time.Sleep(2 * time.Second)

			// Run fix again after forget
			if err := runner.ClusterFix(ctx, firstEndpoint); err != nil {
				logger.Info("Fix after forget failed", "error", err)
			}
			time.Sleep(2 * time.Second)
		}

		// Re-check
		clusterInfo, _ = runner.ClusterInfo(ctx, firstEndpoint)
		if clusterInfo != nil {
			clusterState = clusterInfo["cluster_state"]
		}
		logger.Info("Cluster state after fix and cleanup", "state", clusterState)
	}

	// Build ready IPs set for quick lookup and log all ready pods
	readyIPs := make(map[string]bool)
	var readyIPList []string
	for _, endpoint := range readyEndpoints {
		ip := strings.Split(endpoint, ":")[0]
		readyIPs[ip] = true
		readyIPList = append(readyIPList, ip)
	}

	// Build cluster node IPs (excluding handshake/failed nodes)
	clusterIPs := make(map[string]bool)
	var healthyClusterNodes []rediscli.NodeInfo
	var clusterIPList []string
	var handshakeCount int
	for _, node := range clusterNodes {
		nodeIP := strings.Split(node.Addr, ":")[0]

		// Skip nodes in handshake - they're still joining
		if strings.Contains(node.Flags, "handshake") {
			handshakeCount++
			logger.Info("Node in handshake, waiting to stabilize", "nodeID", node.ID, "addr", node.Addr, "flags", node.Flags)
			continue
		}

		clusterIPs[nodeIP] = true
		clusterIPList = append(clusterIPList, nodeIP)
		healthyClusterNodes = append(healthyClusterNodes, node)
	}

	logger.Info("Scale detection", "readyPods", len(readyEndpoints), "readyIPs", readyIPList, "healthyClusterNodes", len(healthyClusterNodes), "clusterIPs", clusterIPList, "handshakeNodes", handshakeCount)

	// Check if rebalance is running before attempting any scale operations
	isRebalancing, err := runner.IsRebalancing(ctx, firstEndpoint)
	if err != nil {
		logger.Error(err, "Failed to check rebalance status")

		// If cluster check failed, it might be in broken state with open slots
		// Try to cleanup importing/migrating slots
		if strings.Contains(err.Error(), "open slots") || strings.Contains(err.Error(), "don't agree") {
			logger.Info("Cluster appears to have open slots or inconsistent state, attempting cleanup")

			cleanupCtx, cleanupCancel := context.WithTimeout(ctx, 30*time.Second)
			defer cleanupCancel()

			if cleanupErr := runner.CleanupImportingSlots(cleanupCtx, firstEndpoint); cleanupErr != nil {
				logger.Error(cleanupErr, "Failed to cleanup open slots")
			} else {
				logger.Info("Open slots cleanup completed, requeuing")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
		}
	} else if isRebalancing {
		logger.Info("Cluster is currently rebalancing, skipping scale operations until complete")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Get desired replicas from RedisCluster spec (source of truth)
	// Note: desiredReplicas was already set earlier when syncing StatefulSet
	currentClusterSize := int32(len(healthyClusterNodes))

	logger.Info("Cluster state comparison",
		"desiredReplicas", desiredReplicas,
		"currentClusterSize", currentClusterSize,
		"readyPods", len(readyEndpoints),
		"handshakeNodes", handshakeCount)

	// IMPORTANT: If there are nodes in handshake state, wait for them to complete
	// Don't perform any scale operations while nodes are joining
	if handshakeCount > 0 {
		logger.Info("Nodes in handshake state, waiting for cluster to stabilize before any scale operations",
			"handshakeCount", handshakeCount)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// CASE 3: Cluster is already at desired size - no action needed
	if currentClusterSize == desiredReplicas {
		logger.Info("Cluster size matches desired replicas, no scale operation needed",
			"currentSize", currentClusterSize,
			"desired", desiredReplicas)
		// Continue to final status update
	} else if currentClusterSize < desiredReplicas {
		// CASE 4: Scale up - current cluster size is less than desired
		// Find pods that are ready but not in cluster yet
		var newEndpoints []string
		for _, endpoint := range readyEndpoints {
			ip := strings.Split(endpoint, ":")[0]
			if !clusterIPs[ip] {
				newEndpoints = append(newEndpoints, endpoint)
				logger.Info("New pod detected not in cluster", "ip", ip, "endpoint", endpoint)
			}
		}

		// Only proceed with scale-up if:
		// 1. We have new endpoints to add
		// 2. Adding them won't exceed desired replicas
		if len(newEndpoints) > 0 {
			// Calculate how many nodes we should add
			nodesToAdd := int(desiredReplicas - currentClusterSize)
			if nodesToAdd > len(newEndpoints) {
				nodesToAdd = len(newEndpoints)
			}

			// Only add the number of nodes needed to reach desired replicas
			endpointsToAdd := newEndpoints[:nodesToAdd]

			logger.Info("Scale-up operation",
				"currentSize", currentClusterSize,
				"desired", desiredReplicas,
				"nodesToAdd", nodesToAdd,
				"newEndpoints", endpointsToAdd)

			if err := r.scaleUp(ctx, logger, runner, redisCluster, firstEndpoint, endpointsToAdd); err != nil {
				return r.updateStatus(ctx, redisCluster, "Error", clusterState, fmt.Sprintf("scale-up failed: %v", err))
			}

			// Return immediately after scale-up to allow cluster to stabilize
			logger.Info("Scale-up complete, requeuing to verify cluster state")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		} else {
			// We need to scale up but don't have ready pods yet
			logger.Info("Waiting for more pods to be ready for scale-up",
				"currentSize", currentClusterSize,
				"desired", desiredReplicas,
				"readyPods", len(readyEndpoints))
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
	} else if currentClusterSize > desiredReplicas {
		// CASE 5: Scale down - current cluster size is greater than desired
		// This is an intentional scale-down operation
		logger.Info("Scale-down needed",
			"currentSize", currentClusterSize,
			"desired", desiredReplicas,
			"nodesToRemove", currentClusterSize-desiredReplicas)

		// Determine which nodes to remove
		// Strategy: Remove nodes with highest ordinal numbers first (redis-5, redis-4, etc.)
		// This matches StatefulSet scale-down behavior
		nodesToRemove := int(currentClusterSize - desiredReplicas)

		// Sort nodes by ordinal (extract from address/name)
		sortedNodes := make([]rediscli.NodeInfo, len(healthyClusterNodes))
		copy(sortedNodes, healthyClusterNodes)

		// Simple sort by IP address (which corresponds to ordinal in StatefulSet)
		sort.Slice(sortedNodes, func(i, j int) bool {
			return sortedNodes[i].Addr > sortedNodes[j].Addr // Descending order
		})

		// Take the nodes with highest ordinals
		var nodesToRemoveList []rediscli.NodeInfo
		for i := 0; i < nodesToRemove && i < len(sortedNodes); i++ {
			nodesToRemoveList = append(nodesToRemoveList, sortedNodes[i])
			logger.Info("Marking node for removal",
				"nodeID", sortedNodes[i].ID,
				"addr", sortedNodes[i].Addr,
				"role", sortedNodes[i].Role)
		}

		if len(nodesToRemoveList) > 0 {
			logger.Info("Executing scale-down operation",
				"nodesToRemove", len(nodesToRemoveList),
				"currentSize", currentClusterSize,
				"desired", desiredReplicas)

			if err := r.scaleDown(ctx, logger, runner, clusterNodes, nodesToRemoveList, firstEndpoint, redisCluster.Spec.AutoRebalance); err != nil {
				return r.updateStatus(ctx, redisCluster, "Error", clusterState, fmt.Sprintf("scale-down failed: %v", err))
			}

			// Cleanup PVCs if enabled
			if redisCluster.Spec.AutoPvcCleanup {
				if err := r.cleanupOrphanedPVCs(ctx, logger, redisCluster, targetNs, statefulSetName, nodesToRemoveList); err != nil {
					logger.Error(err, "Failed to cleanup PVCs, continuing anyway")
				}
			}

			// Return immediately after scale-down
			logger.Info("Scale-down complete, requeuing to verify cluster state")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	// Get final cluster state and size
	clusterInfo, _ = runner.ClusterInfo(ctx, firstEndpoint)
	if clusterInfo != nil {
		clusterState = clusterInfo["cluster_state"]
	}

	// Update cluster nodes count
	finalClusterNodes, _ := runner.ClusterNodes(ctx, firstEndpoint)
	clusterSize := int32(len(finalClusterNodes))

	logger.Info("Reconciliation complete", "clusterState", clusterState, "clusterSize", clusterSize, "readyPods", readyCount)

	return r.updateStatusWithSize(ctx, redisCluster, "Ready", clusterState, "", clusterSize)
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

	// IMPORTANT: Skip auto-rebalance after scale-up to prevent cluster corruption
	// Rebalancing during scale-up is problematic because:
	// 1. Slot migration takes a long time (can timeout after 5 minutes)
	// 2. Failed rebalance leaves cluster in broken state with open slots
	// 3. New nodes can safely exist with 0 slots - this is valid Redis cluster state
	//
	// Users should manually rebalance after confirming cluster is stable:
	//   redis-cli --cluster rebalance <node>:6379 -a <password> --cluster-use-empty-masters
	//
	// If you really want auto-rebalance, set AutoRebalance: true in RedisCluster spec
	// But be aware this can cause cluster instability during scale operations
	logger.Info("Scale-up completed - new nodes added with 0 slots",
		"note", "Run manual rebalance if you want to redistribute slots to new nodes")

	logger.Info("Scale-up completed")
	return nil
}

// cleanupOrphanedPVCs deletes PVCs for removed nodes
func (r *RedisClusterReconciler) cleanupOrphanedPVCs(
	ctx context.Context,
	logger logr.Logger,
	rc *redisv1alpha1.RedisCluster,
	namespace string,
	statefulSetName string,
	removedNodes []rediscli.NodeInfo,
) error {
	if len(removedNodes) == 0 {
		return nil
	}

	// Get StatefulSet to find PVC template name
	statefulSet := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: statefulSetName, Namespace: namespace}, statefulSet); err != nil {
		return fmt.Errorf("failed to get StatefulSet: %w", err)
	}

	// List all PVCs for this StatefulSet
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcList, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("failed to list PVCs: %w", err)
	}

	// Get list of current pod names (pods that still exist)
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(namespace), client.MatchingLabels(statefulSet.Spec.Selector.MatchLabels)); err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	activePods := make(map[string]bool)
	for _, pod := range podList.Items {
		activePods[pod.Name] = true
	}

	// Find and delete orphaned PVCs
	for _, pvc := range pvcList.Items {
		// Check if PVC belongs to this StatefulSet
		// PVC naming pattern: <volumeClaimTemplate>-<statefulset>-<ordinal>
		// Example: data-redis-0, data-redis-1, etc.
		if !strings.HasPrefix(pvc.Name, statefulSetName+"-") && !strings.Contains(pvc.Name, "-"+statefulSetName+"-") {
			continue
		}

		// Extract pod name from PVC name
		// For PVC like "data-redis-0", the pod name is "redis-0"
		var podName string
		for _, vct := range statefulSet.Spec.VolumeClaimTemplates {
			prefix := vct.Name + "-" + statefulSetName + "-"
			if strings.HasPrefix(pvc.Name, prefix) {
				ordinal := strings.TrimPrefix(pvc.Name, prefix)
				podName = statefulSetName + "-" + ordinal
				break
			}
		}

		// If pod doesn't exist anymore, delete the PVC
		if podName != "" && !activePods[podName] {
			logger.Info("Deleting orphaned PVC", "pvc", pvc.Name, "pod", podName)
			if err := r.Delete(ctx, &pvc); err != nil {
				logger.Error(err, "Failed to delete PVC", "pvc", pvc.Name)
			} else {
				logger.Info("Successfully deleted PVC", "pvc", pvc.Name)
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
	// Separate masters and replicas, and track failed nodes
	var mastersToRemove, replicasToRemove []rediscli.NodeInfo
	var failedMasters []rediscli.NodeInfo
	removeNodeIDs := make(map[string]bool)

	for _, node := range nodesToRemove {
		removeNodeIDs[node.ID] = true

		// Check if node is actually failed (not just handshake or noaddr)
		// Only consider truly failed nodes that are marked with "fail" but not "handshake"
		isFailed := strings.Contains(node.Flags, "fail") && !strings.Contains(node.Flags, "handshake")

		if node.Role == "master" {
			if isFailed {
				logger.Info("Detected failed master for removal", "nodeID", node.ID, "addr", node.Addr, "flags", node.Flags)
				failedMasters = append(failedMasters, node)
			} else {
				mastersToRemove = append(mastersToRemove, node)
			}
		} else {
			replicasToRemove = append(replicasToRemove, node)
		}
	}

	// Find remaining masters (healthy ones)
	var remainingMasters []rediscli.NodeInfo
	var remainingEndpoints []string
	for _, node := range allNodes {
		if !removeNodeIDs[node.ID] && !strings.Contains(node.Flags, "fail") {
			if node.Role == "master" {
				remainingMasters = append(remainingMasters, node)
			}
			remainingEndpoints = append(remainingEndpoints, node.Addr)
		}
	}

	// Sort by slot count
	sort.Slice(remainingMasters, func(i, j int) bool {
		return remainingMasters[i].SlotCount() < remainingMasters[j].SlotCount()
	})

	// Handle failed masters - they need special treatment
	// We can't migrate slots from them, so we need to fix the cluster and forget them
	if len(failedMasters) > 0 {
		logger.Info("Handling failed masters", "count", len(failedMasters))

		// First, run cluster fix to recover orphaned slots
		if err := runner.ClusterFix(ctx, clusterEndpoint); err != nil {
			logger.Info("Cluster fix attempt", "error", err)
		}
		time.Sleep(2 * time.Second)

		// Forget failed nodes from all remaining nodes
		for _, failedNode := range failedMasters {
			logger.Info("Forgetting failed master", "nodeID", failedNode.ID, "addr", failedNode.Addr)
			for _, endpoint := range remainingEndpoints {
				if err := runner.ClusterForget(ctx, endpoint, failedNode.ID); err != nil {
					// It's ok if some forgets fail
					logger.V(1).Info("Forget failed (may be ok)", "endpoint", endpoint, "nodeID", failedNode.ID, "error", err)
				}
			}
		}

		// Run fix again to reassign orphaned slots
		time.Sleep(2 * time.Second)
		if err := runner.ClusterFix(ctx, clusterEndpoint); err != nil {
			logger.Info("Cluster fix after forget", "error", err)
		}
	}

	// Migrate slots from healthy masters being removed
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

	// Remove healthy masters
	for _, master := range mastersToRemove {
		logger.Info("Removing master", "nodeID", master.ID)
		if err := runner.ClusterDelNode(ctx, clusterEndpoint, master.ID); err != nil {
			logger.Error(err, "Failed to remove master", "nodeID", master.ID)
		}
	}

	// Rebalance if enabled
	if autoRebalance && len(remainingMasters) > 0 {
		logger.Info("Rebalancing after scale-down")
		rebalanceCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		_ = runner.ClusterRebalance(rebalanceCtx, clusterEndpoint, false)
	}

	return nil
}

// updateStatus updates the RedisCluster status
func (r *RedisClusterReconciler) updateStatus(ctx context.Context, rc *redisv1alpha1.RedisCluster, phase, clusterState, lastError string) (ctrl.Result, error) {
	return r.updateStatusWithSize(ctx, rc, phase, clusterState, lastError, 0)
}

// updateStatusWithSize updates the RedisCluster status with cluster size
func (r *RedisClusterReconciler) updateStatusWithSize(ctx context.Context, rc *redisv1alpha1.RedisCluster, phase, clusterState, lastError string, clusterSize int32) (ctrl.Result, error) {
	rc.Status.Phase = phase
	rc.Status.ClusterState = clusterState
	rc.Status.LastError = lastError
	if clusterSize > 0 {
		rc.Status.ClusterNodes = clusterSize
	} else if clusterState != "" {
		// Fallback to ready replicas if cluster size not provided
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
	// Background scale watcher is disabled because:
	// 1. Pod watch already triggers reconciliation when pods change
	// 2. Having both can cause race conditions and duplicate scale operations
	// 3. Reconcile loop already handles all scale detection logic
	//
	// If background watcher is needed in the future, uncomment below:
	// if err := mgr.Add(&scaleWatcherRunnable{reconciler: r}); err != nil {
	// 	return err
	// }

	return ctrl.NewControllerManagedBy(mgr).
		For(&redisv1alpha1.RedisCluster{}).
		Owns(&appsv1.StatefulSet{}).
		// Watch Pods with redis label and map them to RedisCluster
		// This ensures reconciliation is triggered when pods are added/removed/changed
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.findRedisClusterForPod),
		).
		Complete(r)
}

// scaleWatcherRunnable is a manager.Runnable that runs the scale watcher
type scaleWatcherRunnable struct {
	reconciler *RedisClusterReconciler
}

// Start implements manager.Runnable
func (s *scaleWatcherRunnable) Start(ctx context.Context) error {
	s.reconciler.startScaleWatcher(ctx)
	return nil
}

// startScaleWatcher runs a background goroutine that continuously watches for scale events
func (r *RedisClusterReconciler) startScaleWatcher(ctx context.Context) {
	logger := r.Log.WithName("scale-watcher")
	logger.Info("Background scale watcher started")

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	watchCount := 0
	for {
		select {
		case <-ctx.Done():
			logger.Info("Scale watcher stopped", "totalChecks", watchCount)
			return
		case <-ticker.C:
			watchCount++
			logger.V(1).Info("Watcher tick", "count", watchCount)

			// List all RedisCluster resources
			var redisClusters redisv1alpha1.RedisClusterList
			if err := r.List(ctx, &redisClusters); err != nil {
				logger.Error(err, "Failed to list RedisCluster resources")
				continue
			}

			logger.V(1).Info("Checking clusters", "count", len(redisClusters.Items))

			// Check each RedisCluster for scale events
			for _, rc := range redisClusters.Items {
				func() {
					defer func() {
						if r := recover(); r != nil {
							logger.Error(fmt.Errorf("panic in checkAndProcessScale: %v", r), "Watcher panic recovered", "cluster", rc.Name)
						}
					}()
					r.checkAndProcessScale(ctx, &rc)
				}()
			}
		}
	}
}

// checkAndProcessScale checks a single RedisCluster for scale events and processes them
func (r *RedisClusterReconciler) checkAndProcessScale(ctx context.Context, rc *redisv1alpha1.RedisCluster) {
	logger := r.Log.WithName("scale-watcher").WithValues("cluster", rc.Name, "namespace", rc.Namespace)

	// Get target namespace
	targetNs := rc.Spec.Namespace
	if targetNs == "" {
		targetNs = rc.Namespace
	}

	statefulSetName := rc.Spec.StatefulSetName
	if statefulSetName == "" {
		statefulSetName = "redis"
	}

	// Get StatefulSet
	statefulSet := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: statefulSetName, Namespace: targetNs}, statefulSet); err != nil {
		logger.V(1).Info("Failed to get StatefulSet", "error", err)
		return
	}

	// Get ready pods
	readyEndpoints, err := r.getReadyEndpoints(ctx, targetNs, statefulSet)
	if err != nil {
		logger.V(1).Info("Failed to get ready endpoints", "error", err)
		return
	}

	if len(readyEndpoints) == 0 {
		logger.V(1).Info("No ready pods yet")
		return
	}

	// Get password
	password, err := r.getPassword(ctx, rc, targetNs)
	if err != nil {
		logger.V(1).Info("Failed to get password", "error", err)
		return
	}

	// Create runner
	runner := rediscli.NewRunner(r.Client, r.Clientset, r.RestConfig, targetNs, password)
	firstEndpoint := readyEndpoints[0]

	// Get cluster nodes
	clusterNodes, err := runner.ClusterNodes(ctx, firstEndpoint)
	if err != nil {
		logger.V(1).Info("Failed to get cluster nodes", "error", err)
		return
	}

	if len(clusterNodes) == 0 {
		logger.V(1).Info("Cluster not initialized yet")
		return
	}

	// Build ready IPs set and log all ready pods
	readyIPs := make(map[string]bool)
	var readyIPList []string
	for _, endpoint := range readyEndpoints {
		ip := strings.Split(endpoint, ":")[0]
		readyIPs[ip] = true
		readyIPList = append(readyIPList, ip)
	}
	logger.Info("Ready pods IPs", "count", len(readyIPList), "ips", readyIPList)

	// Build cluster IPs (excluding handshake/failed nodes)
	clusterIPs := make(map[string]bool)
	var healthyClusterNodes []rediscli.NodeInfo
	var clusterIPList []string
	var handshakeCount int
	for _, node := range clusterNodes {
		nodeIP := strings.Split(node.Addr, ":")[0]

		// Skip nodes in handshake - they're still joining, but continue watching
		if strings.Contains(node.Flags, "handshake") {
			handshakeCount++
			logger.Info("Node in handshake, skipping for now", "nodeID", node.ID, "addr", node.Addr, "flags", node.Flags)
			continue
		}

		clusterIPs[nodeIP] = true
		clusterIPList = append(clusterIPList, nodeIP)
		healthyClusterNodes = append(healthyClusterNodes, node)
	}
	logger.Info("Cluster node IPs (excluding handshake)", "count", len(clusterIPList), "ips", clusterIPList, "handshakeNodes", handshakeCount)

	// Check if rebalance is running before attempting any scale operations
	isRebalancing, err := runner.IsRebalancing(ctx, firstEndpoint)
	if err != nil {
		logger.V(1).Info("Failed to check rebalance status", "error", err)
	} else if isRebalancing {
		logger.Info("Cluster is currently rebalancing, skipping scale operations until complete")
		return
	}

	// Check for scale-up: new pods not in cluster
	var newEndpoints []string
	for _, endpoint := range readyEndpoints {
		ip := strings.Split(endpoint, ":")[0]
		if !clusterIPs[ip] {
			newEndpoints = append(newEndpoints, endpoint)
			logger.Info("New pod detected not in cluster", "ip", ip, "endpoint", endpoint)
		}
	}

	if len(newEndpoints) > 0 {
		logger.Info("Scale-up detected", "newNodes", len(newEndpoints), "endpoints", newEndpoints)
		if err := r.scaleUp(ctx, logger, runner, rc, firstEndpoint, newEndpoints); err != nil {
			logger.Error(err, "Scale-up failed, will retry next cycle")
		} else {
			logger.Info("Scale-up successful")
		}
		// Continue watching after scale-up
		return
	}

	// Check for scale-down: ONLY if StatefulSet replicas decreased (intentional scale-down)
	// This prevents removing nodes when pods are just temporarily down/crashed
	desiredReplicas := int32(0)
	if statefulSet.Spec.Replicas != nil {
		desiredReplicas = *statefulSet.Spec.Replicas
	}

	// Get all pods (not just ready ones) to check if they exist
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(targetNs), client.MatchingLabels{
		"app": statefulSetName,
	}); err != nil {
		logger.V(1).Info("Failed to list pods", "error", err)
		return
	}

	// Build set of all pod IPs (including not-ready pods)
	allPodIPs := make(map[string]bool)
	for _, pod := range podList.Items {
		if pod.Status.PodIP != "" {
			allPodIPs[pod.Status.PodIP] = true
		}
	}

	// Only consider scale-down if:
	// 1. StatefulSet replicas < number of cluster nodes (intentional scale-down)
	// 2. Node IP has NO pod at all (not just "not ready", but completely gone)
	if int32(len(healthyClusterNodes)) > desiredReplicas {
		var nodesToRemove []rediscli.NodeInfo
		for _, node := range healthyClusterNodes {
			nodeIP := strings.Split(node.Addr, ":")[0]

			// Only mark for removal if:
			// 1. Pod doesn't exist at all (not in allPodIPs)
			// 2. OR pod is terminating/being deleted
			if !allPodIPs[nodeIP] {
				logger.Info("Node has no pod, marking for removal",
					"nodeID", node.ID, "addr", node.Addr, "desiredReplicas", desiredReplicas, "clusterNodes", len(healthyClusterNodes))
				nodesToRemove = append(nodesToRemove, node)
			}
		}

		if len(nodesToRemove) > 0 {
			logger.Info("Intentional scale-down detected",
				"nodesToRemove", len(nodesToRemove),
				"desiredReplicas", desiredReplicas,
				"currentClusterNodes", len(healthyClusterNodes))

			if err := r.scaleDown(ctx, logger, runner, clusterNodes, nodesToRemove, firstEndpoint, rc.Spec.AutoRebalance); err != nil {
				logger.Error(err, "Scale-down failed, will retry next cycle")
			} else {
				logger.Info("Scale-down successful")

				// Cleanup PVCs if enabled
				if rc.Spec.AutoPvcCleanup {
					if err := r.cleanupOrphanedPVCs(ctx, logger, rc, targetNs, statefulSetName, nodesToRemove); err != nil {
						logger.Error(err, "Failed to cleanup PVCs")
					}
				}
			}
			// Continue watching after scale-down
			return
		}
	} else if int32(len(healthyClusterNodes)) < desiredReplicas {
		// Cluster nodes < desired replicas, but we already handled scale-up above
		// This means some pods might be starting up, just log and continue watching
		logger.V(1).Info("Waiting for pods to join cluster",
			"desiredReplicas", desiredReplicas,
			"currentClusterNodes", len(healthyClusterNodes),
			"readyPods", len(readyEndpoints))
	}

	logger.V(1).Info("No scale operations needed", "readyPods", len(readyEndpoints), "clusterNodes", len(healthyClusterNodes))
}

// findRedisClusterForPod maps a Pod event to the RedisCluster that manages it
func (r *RedisClusterReconciler) findRedisClusterForPod(ctx context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}

	// Check if this is a redis pod by looking at labels
	labels := pod.GetLabels()
	if labels == nil {
		return nil
	}

	// Look for common redis labels
	_, hasApp := labels["app"]
	_, hasRedis := labels["app.kubernetes.io/name"]
	if !hasApp && !hasRedis {
		return nil
	}

	// List all RedisCluster resources in the same namespace
	var redisClusters redisv1alpha1.RedisClusterList
	if err := r.List(ctx, &redisClusters, client.InNamespace(pod.Namespace)); err != nil {
		return nil
	}

	var requests []reconcile.Request
	for _, rc := range redisClusters.Items {
		// Check if the namespace matches (considering spec.namespace override)
		targetNs := rc.Spec.Namespace
		if targetNs == "" {
			targetNs = rc.Namespace
		}

		if pod.Namespace == targetNs {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      rc.Name,
					Namespace: rc.Namespace,
				},
			})
		}
	}

	return requests
}
