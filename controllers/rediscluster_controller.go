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

	// If cluster info is unavailable, try to detect whether the cluster already exists
	// to avoid endlessly retrying `--cluster create` on nodes that are already initialized.
	existingCluster := false
	if clusterInfo == nil {
		if nodes, nodesErr := runner.ClusterNodes(ctx, firstEndpoint); nodesErr == nil && len(nodes) > 0 {
			existingCluster = true
		}
	}

	// Bootstrap cluster if needed
	if (clusterState == "" || clusterState == "fail") && !existingCluster {
		if readyCount >= redisCluster.Spec.MinReady {
			logger.Info("Bootstrapping Redis cluster", "endpoints", readyEndpoints)
			if err := runner.ClusterCreate(ctx, readyEndpoints); err != nil {
				// Redis refuses create if nodes already have cluster metadata or data.
				// Treat that as "already initialized" to prevent a tight job loop.
				if strings.Contains(err.Error(), "is not empty") || strings.Contains(err.Error(), "already knows other nodes") {
					logger.Info("Cluster create refused because node is not empty; assuming existing cluster and skipping bootstrap", "error", err)
				} else {
					return r.updateStatus(ctx, redisCluster, "Error", clusterState, fmt.Sprintf("failed to create cluster: %v", err))
				}
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

	// If we cannot see cluster nodes, avoid attempting add-node to prevent
	// repeated "node is not empty" loops; wait for next reconcile when
	// permissions/network are healthy.
	if clusterNodes == nil {
		return r.updateStatus(ctx, redisCluster, "Pending", clusterState, "waiting for cluster nodes visibility")
	}

	// Build endpoint to node ID map
	endpointToNodeID := make(map[string]string)
	nodeIDToEndpoint := make(map[string]string)
	for _, node := range clusterNodes {
		endpointToNodeID[node.Addr] = node.ID
		nodeIDToEndpoint[node.ID] = node.Addr
	}

	// Check cluster health before any operations
	health, healthErr := runner.ClusterCheckHealth(ctx, firstEndpoint)
	if healthErr != nil {
		logger.Info("Failed to check cluster health", "error", healthErr)
	} else if !health.IsHealthy {
		logger.Info("Cluster has issues, attempting to fix before operations",
			"nodesAgree", health.NodesAgree,
			"hasOpenSlots", health.HasOpenSlots,
			"warnings", health.Warnings)

		// Try to fix the cluster first
		if fixErr := runner.ClusterFix(ctx, firstEndpoint); fixErr != nil {
			logger.Error(fixErr, "Failed to fix cluster issues")
		} else {
			logger.Info("Cluster fix completed, waiting for convergence")
			time.Sleep(3 * time.Second)
		}
	}

	// Handle scale up: add missing nodes
	nodesAdded := false
	var newEndpoints []string
	for _, endpoint := range readyEndpoints {
		if _, exists := endpointToNodeID[endpoint]; !exists {
			logger.Info("Adding node to cluster", "endpoint", endpoint)
			if err := runner.ClusterAddNode(ctx, endpoint, firstEndpoint); err != nil {
				// If the node is already initialized/non-empty, skip to avoid tight loop
				if strings.Contains(err.Error(), "is not empty") || strings.Contains(err.Error(), "already knows other nodes") {
					logger.Info("Add-node refused: node not empty; trying flushall + cluster reset", "endpoint", endpoint, "error", err)
					if flushErr := runner.FlushAll(ctx, endpoint); flushErr != nil {
						logger.Error(flushErr, "Failed to flushall before reset; will skip add-node", "endpoint", endpoint)
						continue
					}
					if resetErr := runner.ClusterResetHard(ctx, endpoint); resetErr != nil {
						logger.Error(resetErr, "Failed to cluster reset node; will skip add-node", "endpoint", endpoint)
						continue
					}
					if retryErr := runner.ClusterAddNode(ctx, endpoint, firstEndpoint); retryErr != nil {
						logger.Error(retryErr, "Retry add-node failed after reset", "endpoint", endpoint)
						continue
					}
					logger.Info("Add-node succeeded after reset", "endpoint", endpoint)
				} else {
					logger.Error(err, "Failed to add node", "endpoint", endpoint)
					continue
				}
			}
			nodesAdded = true
			newEndpoints = append(newEndpoints, endpoint)
			logger.Info("Successfully added node to cluster", "endpoint", endpoint)
		}
	}

	// If nodes were added, wait for cluster to converge before rebalancing
	if nodesAdded {
		logger.Info("Waiting for cluster to recognize new nodes", "newNodes", len(newEndpoints))

		// Wait for cluster to propagate node information
		time.Sleep(5 * time.Second)

		// Re-check cluster health before rebalance
		health, healthErr = runner.ClusterCheckHealth(ctx, firstEndpoint)
		if healthErr != nil {
			logger.Info("Failed to check cluster health after adding nodes", "error", healthErr)
		} else if !health.IsHealthy {
			logger.Info("Cluster not healthy after adding nodes, fixing before rebalance",
				"nodesAgree", health.NodesAgree,
				"hasOpenSlots", health.HasOpenSlots)

			if fixErr := runner.ClusterFix(ctx, firstEndpoint); fixErr != nil {
				logger.Error(fixErr, "Failed to fix cluster after adding nodes")
			}
			time.Sleep(3 * time.Second)
		}

		// Rebalance if auto-rebalance is enabled
		if redisCluster.Spec.AutoRebalance {
			logger.Info("Rebalancing cluster after scale-up")

			// Check health again before rebalance
			health, _ = runner.ClusterCheckHealth(ctx, firstEndpoint)
			if health != nil && !health.IsHealthy {
				logger.Info("Cluster still not healthy, attempting fix before rebalance")
				_ = runner.ClusterFix(ctx, firstEndpoint)
				time.Sleep(2 * time.Second)
			}

			if err := runner.ClusterRebalance(ctx, firstEndpoint, true); err != nil {
				// If rebalance fails, try to fix and retry once
				if strings.Contains(err.Error(), "fix your cluster") ||
					strings.Contains(err.Error(), "open slots") ||
					strings.Contains(err.Error(), "don't agree") {
					logger.Info("Rebalance failed due to cluster issues, attempting fix and retry")
					if fixErr := runner.ClusterFix(ctx, firstEndpoint); fixErr != nil {
						logger.Error(fixErr, "Failed to fix cluster before rebalance retry")
					} else {
						time.Sleep(3 * time.Second)
						// Retry rebalance
						if retryErr := runner.ClusterRebalance(ctx, firstEndpoint, true); retryErr != nil {
							logger.Error(retryErr, "Rebalance retry also failed")
						} else {
							logger.Info("Rebalance succeeded after fix")
						}
					}
				} else {
					logger.Error(err, "Failed to rebalance cluster after scale-up")
				}
			} else {
				logger.Info("Cluster rebalance completed successfully")
			}
		}
	}

	// Handle scale down: remove nodes that no longer exist
	// Build a set of ready pod IPs for quick lookup
	readyPodIPs := make(map[string]bool)
	for _, pod := range readyPods {
		readyPodIPs[pod.Status.PodIP] = true
	}

	// Identify nodes to remove
	var nodesToRemove []rediscli.NodeInfo
	for _, node := range clusterNodes {
		// Extract IP from node address (format: IP:port)
		nodeIP := extractIP(node.Addr)

		// Check if node IP is in the set of ready pod IPs
		if !readyPodIPs[nodeIP] {
			nodesToRemove = append(nodesToRemove, node)
		}
	}

	if len(nodesToRemove) > 0 {
		logger.Info("Nodes to remove from cluster", "count", len(nodesToRemove))

		// First, handle masters with slots - migrate their data safely
		if err := r.handleScaleDown(ctx, logger, runner, clusterNodes, nodesToRemove, firstEndpoint, redisCluster.Spec.AutoRebalance); err != nil {
			logger.Error(err, "Failed to handle scale-down safely")
			return r.updateStatus(ctx, redisCluster, "Error", clusterState, fmt.Sprintf("scale-down failed: %v", err))
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

// extractIP extracts the IP address from an endpoint string (IP:port)
func extractIP(endpoint string) string {
	if idx := strings.Index(endpoint, ":"); idx != -1 {
		return endpoint[:idx]
	}
	return endpoint
}

// handleScaleDown safely removes nodes from the cluster, migrating slots first
func (r *RedisClusterReconciler) handleScaleDown(
	ctx context.Context,
	logger logr.Logger,
	runner *rediscli.Runner,
	allNodes []rediscli.NodeInfo,
	nodesToRemove []rediscli.NodeInfo,
	clusterEndpoint string,
	autoRebalance bool,
) error {
	// Separate masters and replicas being removed
	var mastersToRemove []rediscli.NodeInfo
	var replicasToRemove []rediscli.NodeInfo

	for _, node := range nodesToRemove {
		if node.Role == "master" {
			mastersToRemove = append(mastersToRemove, node)
		} else {
			replicasToRemove = append(replicasToRemove, node)
		}
	}

	// Find remaining healthy masters (masters that will stay in the cluster)
	remainingMasters := make([]rediscli.NodeInfo, 0)
	removeNodeIDs := make(map[string]bool)
	for _, n := range nodesToRemove {
		removeNodeIDs[n.ID] = true
	}
	for _, node := range allNodes {
		if node.Role == "master" && !removeNodeIDs[node.ID] {
			remainingMasters = append(remainingMasters, node)
		}
	}

	// Sort remaining masters by slot count (ascending) to balance the load
	sort.Slice(remainingMasters, func(i, j int) bool {
		return remainingMasters[i].SlotCount() < remainingMasters[j].SlotCount()
	})

	// Step 1: Migrate slots from masters being removed
	for _, master := range mastersToRemove {
		if master.HasSlots() {
			slotCount := master.SlotCount()
			if slotCount > 0 && len(remainingMasters) > 0 {
				logger.Info("Migrating slots from master being removed",
					"nodeID", master.ID,
					"endpoint", master.Addr,
					"slotCount", slotCount)

				// Distribute slots among remaining masters
				slotsPerMaster := slotCount / len(remainingMasters)
				remainder := slotCount % len(remainingMasters)

				for i, targetMaster := range remainingMasters {
					slotsToMove := slotsPerMaster
					if i < remainder {
						slotsToMove++
					}
					if slotsToMove == 0 {
						continue
					}

					logger.Info("Resharding slots to remaining master",
						"fromNodeID", master.ID,
						"toNodeID", targetMaster.ID,
						"slots", slotsToMove)

					if err := runner.ClusterReshard(ctx, clusterEndpoint, master.ID, targetMaster.ID, slotsToMove); err != nil {
						logger.Error(err, "Failed to reshard slots",
							"fromNodeID", master.ID,
							"toNodeID", targetMaster.ID)
						// Try to continue with other masters
						continue
					}
				}

				// Wait for slot migration to complete
				time.Sleep(2 * time.Second)
			}
		}
	}

	// Step 2: Remove replicas first (they don't hold slots)
	for _, replica := range replicasToRemove {
		logger.Info("Removing replica node", "nodeID", replica.ID, "endpoint", replica.Addr)
		if err := runner.ClusterDelNode(ctx, clusterEndpoint, replica.ID); err != nil {
			logger.Error(err, "Failed to delete replica node", "nodeID", replica.ID)
			// Continue trying to remove other nodes
		} else {
			// Forget node from all remaining nodes
			for _, node := range allNodes {
				if node.ID != replica.ID && !removeNodeIDs[node.ID] {
					_ = runner.ClusterForget(ctx, node.Addr, replica.ID)
				}
			}
		}
	}

	// Step 3: Remove masters (now with no slots)
	for _, master := range mastersToRemove {
		logger.Info("Removing master node", "nodeID", master.ID, "endpoint", master.Addr)

		// Double-check the master has no slots before removing
		// Re-fetch node info to get current slot status
		updatedNodes, err := runner.ClusterNodes(ctx, clusterEndpoint)
		if err == nil {
			for _, n := range updatedNodes {
				if n.ID == master.ID && n.HasSlots() {
					logger.Error(nil, "Master still has slots, cannot remove safely",
						"nodeID", master.ID,
						"slotCount", n.SlotCount())
					continue
				}
			}
		}

		if err := runner.ClusterDelNode(ctx, clusterEndpoint, master.ID); err != nil {
			logger.Error(err, "Failed to delete master node", "nodeID", master.ID)
			continue
		}

		// Forget node from all remaining nodes
		for _, node := range allNodes {
			if node.ID != master.ID && !removeNodeIDs[node.ID] {
				_ = runner.ClusterForget(ctx, node.Addr, master.ID)
			}
		}
	}

	// Step 4: Check cluster health and fix if needed
	time.Sleep(2 * time.Second)
	health, healthErr := runner.ClusterCheckHealth(ctx, clusterEndpoint)
	if healthErr == nil && !health.IsHealthy {
		logger.Info("Cluster has issues after scale-down, attempting fix",
			"hasOpenSlots", health.HasOpenSlots,
			"nodesAgree", health.NodesAgree)
		if fixErr := runner.ClusterFix(ctx, clusterEndpoint); fixErr != nil {
			logger.Error(fixErr, "Failed to fix cluster after scale-down")
		} else {
			time.Sleep(2 * time.Second)
		}
	}

	// Step 5: Rebalance if enabled to redistribute slots evenly
	if autoRebalance && len(remainingMasters) > 0 {
		logger.Info("Rebalancing cluster after scale-down")

		// Check health before rebalance
		health, _ = runner.ClusterCheckHealth(ctx, clusterEndpoint)
		if health != nil && !health.IsHealthy {
			logger.Info("Cluster not healthy, fixing before rebalance")
			_ = runner.ClusterFix(ctx, clusterEndpoint)
			time.Sleep(2 * time.Second)
		}

		if err := runner.ClusterRebalance(ctx, clusterEndpoint, false); err != nil {
			if strings.Contains(err.Error(), "fix your cluster") {
				logger.Info("Rebalance failed, attempting fix and retry")
				_ = runner.ClusterFix(ctx, clusterEndpoint)
				time.Sleep(2 * time.Second)
				if retryErr := runner.ClusterRebalance(ctx, clusterEndpoint, false); retryErr != nil {
					logger.Error(retryErr, "Rebalance retry failed after scale-down")
				}
			} else {
				logger.Error(err, "Failed to rebalance after scale-down")
			}
		}
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redisv1alpha1.RedisCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}
