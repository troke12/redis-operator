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

## Installation

### Option A: Helm (recommended)

The operator is published as a Helm chart and Docker image.

- **Docker image**: [troke12/redis-operator](https://hub.docker.com/r/troke12/redis-operator)
- **Helm repo**: [https://troke12.github.io/redis-operator](https://troke12.github.io/redis-operator)

1. Add the Helm repo and install the operator (CRDs are installed by the chart):

```bash
helm repo add troke12 https://troke12.github.io/redis-operator
helm repo update

# install operator only
helm install redis-operator troke12/redis-operator \
  --namespace redis-operator \
  --create-namespace
```

2. (Optional) Enable a `RedisCluster` managed by the operator (and create a password Secret automatically):

```bash
helm install redis-operator troke12/redis-operator \
  --namespace redis-operator \
  --create-namespace \
  --set redisCluster.enabled=true \
  --set redisCluster.password='changeme'
```

You can override the operator image and other settings via `values.yaml`, for example:

```bash
helm install redis-operator troke12/redis-operator \
  --namespace redis-operator \
  --create-namespace \
  --set image.repository=troke12/redis-operator \
  --set image.tag=latest
```

### Option B: Raw manifests

You can still deploy everything using the provided Kubernetes manifests.

1. Deploy Redis resources:

```bash
kubectl apply -f manifests/redis/
```

This creates:
- Secret with Redis password
- Headless Service for pod DNS
- ConfigMap with Redis configuration
- StatefulSet with 3 Redis pods

2. Deploy operator:

```bash
# Install CRD
kubectl apply -f config/crd/bases/

# Install RBAC
kubectl apply -f config/rbac/

# Build and deploy operator
make docker-build docker-push IMG=troke12/redis-operator:latest
kubectl apply -f config/manager/manager.yaml
```

3. Create RedisCluster resource:

```bash
kubectl apply -f manifests/redis/rediscluster.yaml
```

4. Watch cluster status:

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
