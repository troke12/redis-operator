# Redis Cluster Scaling Guide

## Overview

The Redis operator automatically manages cluster scaling by:
1. Syncing StatefulSet replicas with `RedisCluster.spec.replicas`
2. Adding/removing nodes from the Redis cluster
3. **NOT** auto-rebalancing by default (to prevent cluster corruption)

## Scaling Up

### Step 1: Update RedisCluster replicas

Edit your RedisCluster resource:

```bash
kubectl edit rediscluster redis-cluster -n <namespace>
```

Change the `spec.replicas` value:

```yaml
apiVersion: redis.redis-operator.io/v1alpha1
kind: RedisCluster
metadata:
  name: redis-cluster
spec:
  replicas: 5  # Change from 3 to 5
  # ...
```

Or using kubectl patch:

```bash
kubectl patch rediscluster redis-cluster -n <namespace> \
  --type='merge' -p '{"spec":{"replicas":5}}'
```

Or if using Helm:

```bash
helm upgrade redis-operator ./charts/redis-operator \
  --set redisCluster.replicas=5 \
  --reuse-values
```

### Step 2: Wait for new nodes to join

The operator will:
1. Update the StatefulSet replicas (5)
2. Wait for new pods to be ready
3. Add new nodes to the cluster with **0 slots** (empty masters)

Check the status:

```bash
kubectl get pods -n <namespace>
kubectl get rediscluster redis-cluster -n <namespace>
```

### Step 3: Manually rebalance (IMPORTANT!)

After scaling is complete, you should manually rebalance to distribute slots to new nodes:

```bash
# Get password from secret
REDIS_PASSWORD=$(kubectl get secret redis-auth -n <namespace> \
  -o jsonpath='{.data.REDIS_PASSWORD}' | base64 -d)

# Connect to any redis pod and rebalance
kubectl exec -it redis-0 -n <namespace> -- redis-cli -a "$REDIS_PASSWORD" \
  --cluster rebalance 127.0.0.1:6379 --cluster-use-empty-masters
```

This will redistribute slots evenly across all nodes including the new ones.

## Scaling Down

### Step 1: Update RedisCluster replicas

```bash
kubectl patch rediscluster redis-cluster -n <namespace> \
  --type='merge' -p '{"spec":{"replicas":3}}'
```

Or using Helm:

```bash
helm upgrade redis-operator ./charts/redis-operator \
  --set redisCluster.replicas=3 \
  --reuse-values
```

### Step 2: Wait for scale-down to complete

The operator will:
1. Update the StatefulSet replicas (3)
2. Migrate slots from nodes being removed
3. Remove nodes from the cluster
4. Delete pods with highest ordinals (redis-4, redis-5, etc.)
5. Optionally delete PVCs if `autoPvcCleanup: true`

Monitor the progress:

```bash
kubectl logs -f deployment/redis-operator-controller-manager -n <namespace>
```

## Configuration Options

### values.yaml

```yaml
redisCluster:
  # SINGLE SOURCE OF TRUTH for cluster size
  replicas: 3

  # Minimum ready pods before operations
  minReady: 3

  # Auto-rebalance after scale operations (NOT RECOMMENDED)
  # Set to false to prevent cluster corruption during scale
  autoRebalance: false

  # Auto-delete PVCs when scaling down
  autoPvcCleanup: false
```

## Why is auto-rebalance disabled?

Auto-rebalance after scale-up can cause problems:

1. **Timeout issues**: Slot migration can take 5+ minutes for large clusters
2. **Cluster corruption**: Failed rebalance leaves "open slots" in migrating/importing state
3. **Unnecessary**: New nodes with 0 slots are valid and don't harm the cluster

Manual rebalancing gives you:
- Control over when rebalancing happens
- Ability to monitor the process
- Recovery options if something goes wrong

## Troubleshooting

### Cluster has open slots (broken state)

If rebalance fails and leaves the cluster in broken state:

```bash
# Check cluster status
kubectl exec -it redis-0 -n <namespace> -- redis-cli -a "$REDIS_PASSWORD" \
  --cluster check 127.0.0.1:6379

# The operator will automatically attempt to cleanup open slots
# Or you can manually fix
kubectl exec -it redis-0 -n <namespace> -- redis-cli -a "$REDIS_PASSWORD" \
  --cluster fix 127.0.0.1:6379
```

### Scale operation stuck

Check operator logs:

```bash
kubectl logs -f deployment/redis-operator-controller-manager -n <namespace>
```

Common issues:
- Pods not ready (check pod events)
- Password secret not found
- Network issues between nodes

### Nodes not joining cluster

The operator waits for nodes in "handshake" state to complete. Check:

```bash
# View cluster nodes
kubectl exec -it redis-0 -n <namespace> -- redis-cli -a "$REDIS_PASSWORD" \
  cluster nodes
```

## Best Practices

1. **Scale incrementally**: Scale by 1-2 nodes at a time, not large jumps
2. **Monitor during scale**: Watch operator logs and cluster state
3. **Rebalance off-peak**: Manual rebalancing should be done during low traffic periods
4. **Backup before scale-down**: Ensure data is backed up before removing nodes
5. **Test in staging**: Test scaling operations in non-production environment first

## Emergency Recovery

If cluster is completely broken:

1. Stop the operator:
   ```bash
   kubectl scale deployment redis-operator-controller-manager -n <namespace> --replicas=0
   ```

2. Manually fix the cluster using redis-cli

3. Restart the operator:
   ```bash
   kubectl scale deployment redis-operator-controller-manager -n <namespace> --replicas=1
   ```
