# OpenIM Service Dependency Matrix

**Quick Reference for Operational Use**

## Dependency Legend

- **RPC Deps (Init):** Services that must be resolvable before service starts
- **RPC Deps (Lazy):** Services that may be required during runtime
- **External Deps:** Infrastructure services needed

---

## Dependency Matrix

### RPC Services Layer

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ SERVICE          │ INIT RPC DEPS      │ LAZY RPC DEPS    │ EXTERNAL DEPS    │
├─────────────────────────────────────────────────────────────────────────────┤
│ auth-rpc         │ (none)             │ (none)           │ MongoDB, Redis   │
│ (2 replicas)     │                    │                  │                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ user-rpc         │ msg                │ (none)           │ MongoDB, Redis   │
│ (1 replica)      │ group              │                  │                  │
│                  │ friend             │                  │                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ msg-rpc          │ user               │ (none)           │ MongoDB, Redis   │
│ (2 replicas)     │ group              │                  │ Kafka            │
│                  │ friend             │                  │                  │
│                  │ conversation       │                  │                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ group-rpc        │ user               │ (none)           │ MongoDB, Redis   │
│ (2 replicas)     │ msg                │                  │                  │
│                  │ conversation       │                  │                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ friend-rpc       │ user               │ (none)           │ MongoDB, Redis   │
│ (2 replicas)     │ msg                │                  │                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ conversation-rpc │ user               │ (none)           │ MongoDB, Redis   │
│ (2 replicas)     │ group              │                  │                  │
│                  │ msg                │                  │                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ third-rpc        │ (none)             │ (none)           │ MongoDB, Redis   │
│ (2 replicas)     │                    │                  │ MinIO            │
├─────────────────────────────────────────────────────────────────────────────┤
│ push-rpc         │ (none)             │ (none)           │ Redis, Kafka     │
│ (2 replicas)     │                    │                  │                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Edge Services Layer

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ SERVICE          │ INIT RPC DEPS      │ LAZY RPC DEPS    │ EXTERNAL DEPS    │
├─────────────────────────────────────────────────────────────────────────────┤
│ messagegateway   │ user (at init)     │ msg              │ Redis            │
│ (1 replica)      │                    │ conversation     │                  │
│                  │                    │ group            │                  │
│                  │                    │ friend           │                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ openim-api       │ ALL RPC SERVICES   │ (none)           │ MongoDB, Redis   │
│ (2 replicas)     │ (user, msg, group, │                  │                  │
│                  │  friend, auth,     │                  │                  │
│                  │  conversation,     │                  │                  │
│                  │  third)            │                  │                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ openim-msgtransfer│ (none)             │ (none)           │ Kafka            │
│ (2 replicas)     │                    │                  │ Redis (optional) │
│                  │                    │                  │ MongoDB (opt)    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Circular Dependency Resolution

The following services have inter-dependencies that form cycles:

```
msg-rpc ◄──────┐
  ▲            │
  │ needs      │
  │           friend-rpc ◄─┐
  │            │          │
  ├────────────┼──────────┤
  │            │          │
user-rpc ◄─────┴──group-rpc
  ▲                      ▲
  │                      │
  └──────conversation-rpc┘
```

**How It's Handled:**
- Services use **lazy connection retry** with backoff
- `client.GetConn(ctx, serviceName)` retries until service available
- Kubernetes pod scheduling naturally staggers startup
- After 2-3 minutes, all services connect successfully

**Startup behavior:**
1. All pods start simultaneously
2. Early pods fail GetConn() calls, log warnings
3. As dependencies become available, retries succeed
4. Eventually all services fully connected

---

## Service Port Assignment

### RPC Service Ports

| Service | RPC Port | Metrics Port |
|---------|----------|--------------|
| auth-rpc | 10200 | 12200 |
| user-rpc | 10320 | 12320 |
| msg-rpc | 10280 | 12280 |
| group-rpc | 10260 | 12260 |
| friend-rpc | 10240 | 12240 |
| conversation-rpc | 10220 | 12220 |
| third-rpc | 10300 | 12300 |
| push-rpc | 10170 | 12170 |

### Edge Service Ports

| Service | Port | Type | Purpose |
|---------|------|------|---------|
| messagegateway | 10140 | RPC | gRPC for other gateways |
| messagegateway | 10001 | WebSocket | Client connections |
| messagegateway | 12140 | Metrics | Prometheus metrics |
| openim-api | 10002 | REST | HTTP API |
| openim-api | 12002 | Metrics | Prometheus metrics |
| openim-msgtransfer | 12020 | Metrics | Prometheus metrics (Kafka consumer) |

---

## Service Discovery Names (registerName)

These names are used in gRPC service discovery:

```
"user-rpc-service"              → user-rpc-server deployment
"msg-rpc-service"               → msg-rpc-server deployment
"group-rpc-service"             → group-rpc-server deployment
"friend-rpc-service"            → friend-rpc-server deployment (relation)
"auth-rpc-service"              → auth-rpc-server deployment
"conversation-rpc-service"      → conversation-rpc-server deployment
"third-rpc-service"             → third-rpc-server deployment
"messagegateway-rpc-service"    → messagegateway-rpc-server deployment
```

---

## Deployment Commands

### Check Service Status

```bash
# All RPC services
kubectl get pods -n openim | grep rpc

# All services (RPC + edge)
kubectl get pods -n openim | grep -E "rpc|api|transfer|push|gateway"

# Specific service with more details
kubectl get pods -n openim -l app=msg-rpc-server -o wide

# Check service discovery endpoints
kubectl get endpoints -n openim | grep rpc
```

### Monitor Service Health

```bash
# Check for init errors
kubectl logs -n openim msg-rpc-server-xxx | grep -E "ERROR|FAIL|503|connection"

# Check for 403 permission errors
kubectl logs -n openim msg-rpc-server-xxx | grep "403"

# Check for service discovery issues
kubectl logs -n openim msg-rpc-server-xxx | grep -E "service|discovery|resolve"

# Verify all services listening
kubectl logs -n openim msg-rpc-server-xxx | grep "listening on ports"
```

### Debugging Service Connectivity

```bash
# Test DNS resolution from pod
kubectl exec -it msg-rpc-server-xxx -n openim -- \
  nslookup user-rpc-service.openim.svc.cluster.local

# Test gRPC connectivity
kubectl exec -it msg-rpc-server-xxx -n openim -- \
  grpcurl -plaintext user-rpc-service:10320 list

# Check service has endpoints
kubectl describe svc user-rpc-service -n openim

# View service endpoints directly
kubectl get endpoints user-rpc-service -n openim -o yaml
```

### Restart Services for Dependency Fix

```bash
# Restart single service
kubectl rollout restart deployment/user-rpc-server -n openim

# Restart all RPC services
for svc in user msg group friend auth conversation third; do
  kubectl rollout restart deployment/${svc}-rpc-server -n openim
done

# Restart and wait for Ready
kubectl rollout restart deployment/user-rpc-server -n openim
kubectl rollout status deployment/user-rpc-server -n openim
```

### View Configuration

```bash
# Check discovery config
kubectl get cm openim-config -n openim -o yaml | grep -A5 "discovery.yml"

# Check service start config
kubectl get cm openim-start-config-user -n openim -o yaml

# Check environment variables
kubectl get pod msg-rpc-server-xxx -n openim -o yaml | grep -A50 "env:"
```

---

## Common Issues & Quick Fixes

### Issue: Pod in CrashLoopBackOff

```bash
# Check logs for error
kubectl logs -n openim {pod-name} | tail -20

# If "nil pointer": Update image to v3.8.3-patch.12
# If "component check failed": Check MongoDB/Redis/Kafka connectivity
# If missing ConfigMap: kubectl create configmap openim-start-config-{svc} ...

# After fixing: Delete pod to restart
kubectl delete pod {pod-name} -n openim
```

### Issue: 403 Forbidden Errors

```bash
# Verify ServiceAccount exists
kubectl get sa openim-services -n openim

# Verify RBAC bindings
kubectl get rolebindings -n openim | grep openim
kubectl get clusterrolebindings | grep openim

# Check permissions
kubectl auth can-i get endpoints --as=system:serviceaccount:openim:openim-services
```

### Issue: Services Can't Find Each Other

```bash
# Verify all services running first
kubectl get pods -n openim | grep -c Running

# Check service endpoints exist
kubectl get endpoints -n openim | grep rpc

# Restart failed services
kubectl delete pod -n openim -l app=user-rpc-server
```

---

## Monitoring Checklist

**Every 5 minutes during deployment:**
```bash
kubectl get pods -n openim | grep -E "rpc|api|transfer"
```

**Expected progression:**
```
0-10s:   Pods appear in "Pending" state
10-20s:  Pods transition to "ContainerCreating"
20-30s:  Pods reach "Running" but services still initializing
30-60s:  First services ready, others still connecting
60-120s: All services ready and interconnected
```

**Final state:**
```bash
# All pods in Running state
kubectl get pods -n openim | wc -l  # Should be 20+ pods

# All services with endpoints
kubectl get svc -n openim | wc -l   # Should be 10+ services

# All services resolvable
for svc in user msg group friend auth conversation third; do
  kubectl exec msg-rpc-server-xxx -n openim -- \
    nslookup ${svc}-rpc-service.openim.svc.cluster.local
done
```

---

**Quick Reference Version:** 1.0  
**Last Updated:** April 18, 2026
