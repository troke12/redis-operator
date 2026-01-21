package rediscli

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Runner executes redis-cli commands via pod exec
type Runner struct {
	client     client.Client
	clientset  kubernetes.Interface
	restConfig *rest.Config
	namespace  string
	password   string
}

// NewRunner creates a new redis-cli runner
func NewRunner(c client.Client, clientset kubernetes.Interface, restConfig *rest.Config, namespace, password string) *Runner {
	return &Runner{
		client:     c,
		clientset:  clientset,
		restConfig: restConfig,
		namespace:  namespace,
		password:   password,
	}
}

// execInPod executes a command in a redis pod
func (r *Runner) execInPod(ctx context.Context, podName string, command []string) (string, error) {
	req := r.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(r.namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "redis",
			Command:   command,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(r.restConfig, "POST", req.URL())
	if err != nil {
		return "", fmt.Errorf("failed to create executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})

	output := stdout.String()
	if stderr.Len() > 0 {
		// Include stderr in output for debugging, but filter warnings
		stderrStr := stderr.String()
		if !strings.Contains(stderrStr, "Warning:") {
			output += stderrStr
		}
	}

	if err != nil {
		return output, fmt.Errorf("exec failed: %w, output: %s", err, output)
	}

	return output, nil
}

// getPodForEndpoint finds the pod name for a given endpoint IP
func (r *Runner) getPodForEndpoint(ctx context.Context, endpoint string) (string, error) {
	ip := strings.Split(endpoint, ":")[0]

	pods := &corev1.PodList{}
	if err := r.client.List(ctx, pods, client.InNamespace(r.namespace), client.MatchingLabels{"app": "redis"}); err != nil {
		return "", err
	}

	for _, pod := range pods.Items {
		if pod.Status.PodIP == ip {
			return pod.Name, nil
		}
	}

	return "", fmt.Errorf("no pod found for endpoint %s", endpoint)
}

// redisCliCmd builds a redis-cli command with auth
func (r *Runner) redisCliCmd(args ...string) []string {
	cmd := []string{"redis-cli", "-a", r.password}
	cmd = append(cmd, args...)
	return cmd
}

// ClusterInfo runs CLUSTER INFO and parses the state
func (r *Runner) ClusterInfo(ctx context.Context, endpoint string) (map[string]string, error) {
	podName, err := r.getPodForEndpoint(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	output, err := r.execInPod(ctx, podName, r.redisCliCmd("cluster", "info"))
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Warning:") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			result[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}

	return result, nil
}

// ClusterNodes runs CLUSTER NODES and returns parsed node info
func (r *Runner) ClusterNodes(ctx context.Context, endpoint string) ([]NodeInfo, error) {
	podName, err := r.getPodForEndpoint(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	output, err := r.execInPod(ctx, podName, r.redisCliCmd("cluster", "nodes"))
	if err != nil {
		return nil, err
	}

	var nodes []NodeInfo
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "Warning:") {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		nodeID := parts[0]
		addr := parts[1]
		if !strings.Contains(addr, ":") {
			continue
		}

		// Extract just host:port from host:port@cport
		addr = normalizeClusterAddr(addr)

		flags := ""
		if len(parts) > 2 {
			flags = parts[2]
		}

		role := "slave"
		if strings.Contains(flags, "master") {
			role = "master"
		}

		// Parse slot ranges if present (parts[8:] for masters)
		var slots []string
		if role == "master" && len(parts) > 8 {
			slots = parts[8:]
		}

		nodes = append(nodes, NodeInfo{
			ID:    nodeID,
			Addr:  addr,
			Role:  role,
			Flags: flags,
			Slots: slots,
		})
	}

	return nodes, nil
}

// normalizeClusterAddr extracts host:port from cluster node address format (host:port@cport)
func normalizeClusterAddr(addr string) string {
	if idx := strings.Index(addr, "@"); idx != -1 {
		return addr[:idx]
	}
	return addr
}

// ClusterCreate creates a new Redis cluster with the given endpoints
func (r *Runner) ClusterCreate(ctx context.Context, endpoints []string) error {
	if len(endpoints) == 0 {
		return fmt.Errorf("no endpoints provided")
	}

	podName, err := r.getPodForEndpoint(ctx, endpoints[0])
	if err != nil {
		return err
	}

	// Build the cluster create command
	args := []string{"--cluster", "create"}
	args = append(args, endpoints...)
	args = append(args, "--cluster-yes", "-a", r.password)

	cmd := []string{"redis-cli"}
	cmd = append(cmd, args...)

	output, err := r.execInPod(ctx, podName, cmd)
	if err != nil {
		// Check if the error is because nodes already have data
		if strings.Contains(output, "is not empty") || strings.Contains(err.Error(), "is not empty") {
			return fmt.Errorf("node is not empty: %s", output)
		}
		return fmt.Errorf("cluster create failed: %w, output: %s", err, output)
	}

	return nil
}

// ClusterMeet forces a node to join the cluster
func (r *Runner) ClusterMeet(ctx context.Context, existingEndpoint, newHost string, newPort int) error {
	podName, err := r.getPodForEndpoint(ctx, existingEndpoint)
	if err != nil {
		return err
	}

	output, err := r.execInPod(ctx, podName, r.redisCliCmd("cluster", "meet", newHost, fmt.Sprintf("%d", newPort)))
	if err != nil {
		return fmt.Errorf("cluster meet failed: %w, output: %s", err, output)
	}

	return nil
}

// ClusterAddNode adds a new node to the cluster using --cluster add-node
func (r *Runner) ClusterAddNode(ctx context.Context, newEndpoint, existingEndpoint string) error {
	podName, err := r.getPodForEndpoint(ctx, existingEndpoint)
	if err != nil {
		return err
	}

	cmd := []string{"redis-cli", "--cluster", "add-node", newEndpoint, existingEndpoint, "-a", r.password}
	output, err := r.execInPod(ctx, podName, cmd)
	if err != nil {
		return fmt.Errorf("add-node failed: %w, output: %s", err, output)
	}

	return nil
}

// ClusterDelNode removes a node from the cluster
func (r *Runner) ClusterDelNode(ctx context.Context, clusterEndpoint, nodeID string) error {
	podName, err := r.getPodForEndpoint(ctx, clusterEndpoint)
	if err != nil {
		return err
	}

	cmd := []string{"redis-cli", "--cluster", "del-node", clusterEndpoint, nodeID, "-a", r.password}
	output, err := r.execInPod(ctx, podName, cmd)
	if err != nil {
		return fmt.Errorf("del-node failed: %w, output: %s", err, output)
	}

	return nil
}

// ClusterForget removes a node from the cluster's view
func (r *Runner) ClusterForget(ctx context.Context, endpoint, nodeID string) error {
	podName, err := r.getPodForEndpoint(ctx, endpoint)
	if err != nil {
		return err
	}

	output, err := r.execInPod(ctx, podName, r.redisCliCmd("cluster", "forget", nodeID))
	if err != nil {
		return fmt.Errorf("cluster forget failed: %w, output: %s", err, output)
	}

	return nil
}

// ClusterRebalance rebalances slots across masters
func (r *Runner) ClusterRebalance(ctx context.Context, endpoint string, useEmptyMasters bool) error {
	podName, err := r.getPodForEndpoint(ctx, endpoint)
	if err != nil {
		return err
	}

	cmd := []string{"redis-cli", "--cluster", "rebalance", endpoint, "-a", r.password}
	if useEmptyMasters {
		cmd = append(cmd, "--cluster-use-empty-masters")
	}

	output, err := r.execInPod(ctx, podName, cmd)
	if err != nil {
		return fmt.Errorf("rebalance failed: %w, output: %s", err, output)
	}

	return nil
}

// ClusterReshard moves slots from one node to another
func (r *Runner) ClusterReshard(ctx context.Context, clusterEndpoint, fromNodeID, toNodeID string, slotCount int) error {
	podName, err := r.getPodForEndpoint(ctx, clusterEndpoint)
	if err != nil {
		return err
	}

	cmd := []string{
		"redis-cli", "--cluster", "reshard", clusterEndpoint,
		"--cluster-from", fromNodeID,
		"--cluster-to", toNodeID,
		"--cluster-slots", fmt.Sprintf("%d", slotCount),
		"--cluster-yes",
		"-a", r.password,
	}

	output, err := r.execInPod(ctx, podName, cmd)
	if err != nil {
		return fmt.Errorf("reshard failed: %w, output: %s", err, output)
	}

	return nil
}

// ClusterFix attempts to fix cluster slot inconsistencies
func (r *Runner) ClusterFix(ctx context.Context, endpoint string) error {
	podName, err := r.getPodForEndpoint(ctx, endpoint)
	if err != nil {
		return err
	}

	cmd := []string{"redis-cli", "--cluster", "fix", endpoint, "--cluster-yes", "-a", r.password}
	output, err := r.execInPod(ctx, podName, cmd)
	if err != nil {
		return fmt.Errorf("cluster fix failed: %w, output: %s", err, output)
	}

	return nil
}

// ClusterCheck runs cluster check
func (r *Runner) ClusterCheck(ctx context.Context, endpoint string) (string, error) {
	podName, err := r.getPodForEndpoint(ctx, endpoint)
	if err != nil {
		return "", err
	}

	cmd := []string{"redis-cli", "--cluster", "check", endpoint, "-a", r.password}
	return r.execInPod(ctx, podName, cmd)
}

// ClusterReset resets the cluster state of a node (soft or hard)
func (r *Runner) ClusterReset(ctx context.Context, endpoint string, hard bool) error {
	podName, err := r.getPodForEndpoint(ctx, endpoint)
	if err != nil {
		return err
	}

	resetType := "soft"
	if hard {
		resetType = "hard"
	}

	output, err := r.execInPod(ctx, podName, r.redisCliCmd("cluster", "reset", resetType))
	if err != nil {
		return fmt.Errorf("cluster reset %s failed: %w, output: %s", resetType, err, output)
	}

	return nil
}

// FlushAll wipes all data on the node
func (r *Runner) FlushAll(ctx context.Context, endpoint string) error {
	podName, err := r.getPodForEndpoint(ctx, endpoint)
	if err != nil {
		return err
	}

	output, err := r.execInPod(ctx, podName, r.redisCliCmd("flushall"))
	if err != nil {
		return fmt.Errorf("flushall failed: %w, output: %s", err, output)
	}

	return nil
}

// DBSize returns the number of keys in the database
func (r *Runner) DBSize(ctx context.Context, endpoint string) (int64, error) {
	podName, err := r.getPodForEndpoint(ctx, endpoint)
	if err != nil {
		return 0, err
	}

	output, err := r.execInPod(ctx, podName, r.redisCliCmd("dbsize"))
	if err != nil {
		return 0, err
	}

	var count int64
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Warning:") {
			continue
		}
		// Format: "keys=N" or "(integer) N"
		if strings.Contains(line, "keys=") {
			fmt.Sscanf(line, "keys=%d", &count)
		} else {
			fmt.Sscanf(line, "(integer) %d", &count)
		}
	}

	return count, nil
}

// IsClusterEnabled checks if cluster mode is enabled on a node
func (r *Runner) IsClusterEnabled(ctx context.Context, endpoint string) (bool, error) {
	podName, err := r.getPodForEndpoint(ctx, endpoint)
	if err != nil {
		return false, err
	}

	output, err := r.execInPod(ctx, podName, r.redisCliCmd("config", "get", "cluster-enabled"))
	if err != nil {
		return false, err
	}

	return strings.Contains(output, "yes"), nil
}

// WaitForClusterState waits for the cluster to reach a specific state
func (r *Runner) WaitForClusterState(ctx context.Context, endpoint string, targetState string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		info, err := r.ClusterInfo(ctx, endpoint)
		if err == nil {
			if info["cluster_state"] == targetState {
				return nil
			}
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timeout waiting for cluster state %s", targetState)
}

// NodeInfo represents a Redis cluster node
type NodeInfo struct {
	ID    string
	Addr  string
	Role  string
	Flags string
	Slots []string
}

// HasSlots returns true if the node has any slots assigned
func (n *NodeInfo) HasSlots() bool {
	return len(n.Slots) > 0
}

// SlotCount returns the approximate number of slots assigned to this node
func (n *NodeInfo) SlotCount() int {
	count := 0
	for _, slot := range n.Slots {
		if strings.Contains(slot, "-") {
			parts := strings.Split(slot, "-")
			if len(parts) == 2 {
				var start, end int
				fmt.Sscanf(parts[0], "%d", &start)
				fmt.Sscanf(parts[1], "%d", &end)
				count += end - start + 1
			}
		} else {
			count++
		}
	}
	return count
}
