# Redis Cluster Operator for Kubernetes

A Kubernetes operator that automatically manages Redis 6.2 Cluster deployments, handling cluster bootstrap, scale-up, and scale-down operations.

## Features

- **Automatic cluster bootstrap** when ≥3 pods are ready
- **Scale-up**: Automatically adds new nodes to the cluster
- **Scale-down**: Safely removes nodes, migrating slots if needed
- **Password authentication** support
- **Stable DNS-based addressing** using StatefulSet pod names
- **Continuous reconciliation** of cluster state

## Architecture

The operator consists of:

1. **RedisCluster CRD**: Custom resource defining desired cluster state
2. **Controller**: Reconciles RedisCluster resources, managing cluster membership
3. **Redis CLI Runner**: Executes `redis-cli` commands via Kubernetes Jobs
4. **Kubernetes Manifests**: StatefulSet, Services, ConfigMaps for Redis deployment

## Prerequisites

- Kubernetes cluster (1.20+)
- kubectl configured
- Go 1.21+ (for building)
- StorageClass available for persistent volumes

## Quick Start

### 1. Deploy Redis Resources

```bash
kubectl apply -f manifests/redis/
```

This creates:
- Secret with Redis password
- Headless Service for pod DNS
- ConfigMap with Redis configuration
- StatefulSet with 3 Redis pods

### 2. Deploy Operator

```bash
# Install CRD
kubectl apply -f config/crd/bases/

# Install RBAC
kubectl apply -f config/rbac/

# Build and deploy operator (or use your preferred method)
make docker-build docker-push IMG=your-registry/redis-operator:latest
kubectl apply -f config/manager/manager.yaml
```

### 3. Create RedisCluster Resource

```bash
kubectl apply -f manifests/redis/rediscluster.yaml
```

### 4. Watch Cluster Status

```bash
kubectl get rediscluster redis-cluster -w
```

The operator will:
1. Wait for 3 pods to be ready
2. Bootstrap the Redis cluster automatically
3. Update status to `Ready` with `cluster_state: ok`

## Scaling

### Scale Up

```bash
# Scale StatefulSet
kubectl scale statefulset redis --replicas=5

# Operator will automatically add new nodes to the cluster
```

### Scale Down

```bash
# Scale StatefulSet
kubectl scale statefulset redis --replicas=3

# Operator will:
# 1. Rebalance slots if autoRebalance=true
# 2. Remove nodes from cluster
# 3. Issue CLUSTER FORGET on remaining nodes
```

## Configuration

### RedisCluster Spec

```yaml
apiVersion: redis.redis-operator.io/v1alpha1
kind: RedisCluster
metadata:
  name: redis-cluster
spec:
  replicas: 3                    # Number of Redis nodes (min 3)
  minReady: 3                    # Minimum ready pods before bootstrap
  autoRebalance: true            # Auto-rebalance slots on scale down
  serviceName: redis-headless    # Headless service name
  statefulSetName: redis         # StatefulSet name
  passwordSecretRef:
    name: redis-auth             # Secret name
    key: REDIS_PASSWORD         # Secret key
```

### Redis Configuration

The ConfigMap (`manifests/redis/configmap.yaml`) contains:
- Cluster mode settings
- Hostname-based cluster announcement (Redis 6.2+)
- Persistence configuration
- Password authentication

## Testing

### Manual Verification

1. **Check cluster status**:
```bash
kubectl exec -it redis-0 -- redis-cli -a $(kubectl get secret redis-auth -o jsonpath='{.data.REDIS_PASSWORD}' | base64 -d) cluster info
```

2. **List cluster nodes**:
```bash
kubectl exec -it redis-0 -- redis-cli -a $(kubectl get secret redis-auth -o jsonpath='{.data.REDIS_PASSWORD}' | base64 -d) cluster nodes
```

3. **Test scaling**:
```bash
# Scale up
kubectl scale statefulset redis --replicas=4
# Watch operator logs
kubectl logs -f deployment/redis-operator-controller-manager -n system

# Scale down
kubectl scale statefulset redis --replicas=3
```

## Development

### Build

```bash
go mod download
go build -o bin/manager cmd/manager/main.go
```

### Run Locally

```bash
# Install CRD
kubectl apply -f config/crd/bases/

# Run operator
make run
```

### Generate Code

```bash
# Generate deepcopy (if using controller-gen)
controller-gen object:headerFile="hack/boilerplate.go.txt" paths="./..."
```

## Troubleshooting

### Cluster Not Forming

- Check pod readiness: `kubectl get pods -l app=redis`
- Check operator logs: `kubectl logs -f deployment/redis-operator-controller-manager -n system`
- Verify password secret exists and is correct
- Check Redis pod logs: `kubectl logs redis-0`

### Nodes Not Joining

- Verify DNS resolution: `kubectl exec redis-0 -- nslookup redis-headless`
- Check cluster state: `kubectl exec redis-0 -- redis-cli -a $PASSWORD cluster info`
- Review operator reconciliation logs

### Scale Down Issues

- Ensure `autoRebalance: true` for automatic slot migration
- Check that target replica count is ≥3
- Verify no nodes are in `fail` state before scaling

## License

MIT
