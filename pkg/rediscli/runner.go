package rediscli

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	RedisCLIImage = "redis:6.2-alpine"
	JobTimeout    = 5 * time.Minute
)

// Runner executes redis-cli commands via Kubernetes Jobs
type Runner struct {
	client    client.Client
	clientset kubernetes.Interface
	namespace string
	password  string
}

// NewRunner creates a new redis-cli runner
func NewRunner(client client.Client, clientset kubernetes.Interface, namespace, password string) *Runner {
	return &Runner{
		client:    client,
		clientset: clientset,
		namespace: namespace,
		password:  password,
	}
}

// Execute runs a redis-cli command and returns the output
func (r *Runner) Execute(ctx context.Context, command []string) (string, error) {
	jobName := fmt.Sprintf("redis-cli-%d", time.Now().UnixNano())

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: r.namespace,
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: func() *int32 { t := int32(300); return &t }(),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    "redis-cli",
							Image:   RedisCLIImage,
							Command: command,
							Env: []corev1.EnvVar{
								{
									Name:  "REDISCLI_AUTH",
									Value: r.password,
								},
							},
						},
					},
				},
			},
		},
	}

	if err := r.client.Create(ctx, job); err != nil {
		return "", fmt.Errorf("failed to create job: %w", err)
	}

	defer func() {
		// Clean up job
		_ = r.client.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground))
	}()

	// Wait for job completion
	if err := r.waitForJob(ctx, jobName); err != nil {
		return "", err
	}

	// Get pod logs
	return r.getJobLogs(ctx, jobName)
}

func (r *Runner) waitForJob(ctx context.Context, jobName string) error {
	return wait.PollUntilContextTimeout(ctx, 2*time.Second, JobTimeout, true, func(ctx context.Context) (bool, error) {
		job := &batchv1.Job{}
		if err := r.client.Get(ctx, client.ObjectKey{Name: jobName, Namespace: r.namespace}, job); err != nil {
			return false, err
		}

		for _, condition := range job.Status.Conditions {
			if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
				return true, nil
			}
			if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
				return false, fmt.Errorf("job failed: %s", condition.Message)
			}
		}
		return false, nil
	})
}

func (r *Runner) getJobLogs(ctx context.Context, jobName string) (string, error) {
	pods := &corev1.PodList{}
	if err := r.client.List(ctx, pods, client.InNamespace(r.namespace), client.MatchingLabels{"job-name": jobName}); err != nil {
		return "", err
	}

	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no pods found for job %s", jobName)
	}

	pod := pods.Items[0]
	req := r.clientset.CoreV1().Pods(r.namespace).GetLogs(pod.Name, &corev1.PodLogOptions{})
	logStream, err := req.Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get log stream: %w", err)
	}
	defer logStream.Close()

	logBytes, err := io.ReadAll(logStream)
	if err != nil {
		return "", fmt.Errorf("failed to read logs: %w", err)
	}

	return string(logBytes), nil
}

// ClusterInfo runs CLUSTER INFO and parses the state
func (r *Runner) ClusterInfo(ctx context.Context, endpoint string) (map[string]string, error) {
	// Endpoint format: host:port, split for redis-cli
	host, port := r.splitEndpoint(endpoint)
	cmd := []string{
		"sh", "-c",
		fmt.Sprintf("redis-cli -h %s -p %s -a \"$REDISCLI_AUTH\" cluster info", host, port),
	}

	output, err := r.Execute(ctx, cmd)
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)
	for _, line := range strings.Split(output, "\n") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			result[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}

	return result, nil
}

// ClusterNodes runs CLUSTER NODES and returns parsed node info
func (r *Runner) ClusterNodes(ctx context.Context, endpoint string) ([]NodeInfo, error) {
	// Endpoint format: host:port, split for redis-cli
	host, port := r.splitEndpoint(endpoint)
	cmd := []string{
		"sh", "-c",
		fmt.Sprintf("redis-cli -h %s -p %s -a \"$REDISCLI_AUTH\" cluster nodes", host, port),
	}

	output, err := r.Execute(ctx, cmd)
	if err != nil {
		return nil, err
	}

	var nodes []NodeInfo
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		nodeID := parts[0]
		addr := parts[1]
		flags := ""
		if len(parts) > 2 {
			flags = parts[2]
		}

		role := "slave"
		if strings.Contains(flags, "master") {
			role = "master"
		}

		nodes = append(nodes, NodeInfo{
			ID:    nodeID,
			Addr:  addr,
			Role:  role,
			Flags: flags,
		})
	}

	return nodes, nil
}

// ClusterCreate creates a new Redis cluster
func (r *Runner) ClusterCreate(ctx context.Context, endpoints []string) error {
	endpointsStr := strings.Join(endpoints, " ")
	cmd := []string{
		"sh", "-c",
		fmt.Sprintf("redis-cli --cluster create %s --cluster-yes -a \"$REDISCLI_AUTH\"", endpointsStr),
	}

	_, err := r.Execute(ctx, cmd)
	return err
}

// ClusterAddNode adds a new node to the cluster
func (r *Runner) ClusterAddNode(ctx context.Context, newEndpoint, existingEndpoint string) error {
	cmd := []string{
		"sh", "-c",
		fmt.Sprintf("redis-cli --cluster add-node %s %s -a \"$REDISCLI_AUTH\"", newEndpoint, existingEndpoint),
	}

	_, err := r.Execute(ctx, cmd)
	return err
}

// ClusterDelNode removes a node from the cluster
func (r *Runner) ClusterDelNode(ctx context.Context, endpoint, nodeID string) error {
	cmd := []string{
		"sh", "-c",
		fmt.Sprintf("redis-cli --cluster del-node %s %s -a \"$REDISCLI_AUTH\"", endpoint, nodeID),
	}

	_, err := r.Execute(ctx, cmd)
	return err
}

// ClusterForget removes a node from other nodes' view
func (r *Runner) ClusterForget(ctx context.Context, endpoint, nodeID string) error {
	// Endpoint format: host:port, split for redis-cli
	host, port := r.splitEndpoint(endpoint)
	cmd := []string{
		"sh", "-c",
		fmt.Sprintf("redis-cli -h %s -p %s -a \"$REDISCLI_AUTH\" cluster forget %s", host, port, nodeID),
	}

	_, err := r.Execute(ctx, cmd)
	return err
}

// ClusterRebalance rebalances slots across masters
func (r *Runner) ClusterRebalance(ctx context.Context, endpoint string, useEmptyMasters bool) error {
	args := ""
	if useEmptyMasters {
		args = "--cluster-use-empty-masters"
	}
	cmd := []string{
		"sh", "-c",
		fmt.Sprintf("redis-cli --cluster rebalance %s %s -a \"$REDISCLI_AUTH\"", endpoint, args),
	}

	_, err := r.Execute(ctx, cmd)
	return err
}

// splitEndpoint splits endpoint "host:port" into host and port
func (r *Runner) splitEndpoint(endpoint string) (string, string) {
	parts := strings.Split(endpoint, ":")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	// Fallback: assume default port if no port specified
	return endpoint, "6379"
}

// NodeInfo represents a Redis cluster node
type NodeInfo struct {
	ID    string
	Addr  string
	Role  string
	Flags string
}
