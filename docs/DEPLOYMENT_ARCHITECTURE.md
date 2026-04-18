# OpenIM Kubernetes Deployment Architecture

**Last Updated:** April 18, 2026  
**Status:** Production Ready - Source Code Validated  
**Deployment Mode:** Kubernetes with Service Discovery

---

## Table of Contents

1. [Service Dependency Graph](#service-dependency-graph)
2. [Service Discovery Architecture](#service-discovery-architecture)
3. [Configuration Pattern](#configuration-pattern)
4. [Deployment Requirements](#deployment-requirements)
5. [Service Startup Order](#service-startup-order)
6. [Health Checks & Initialization](#health-checks--initialization)
7. [Troubleshooting Guide](#troubleshooting-guide)

---

## Service Dependency Graph

### Complete Dependency Map

```
┌─────────────────────────────────────────────────────────────────────┐
│                    OPENIM KUBERNETES DEPLOYMENT                    │
│                         (Namespace: openim)                         │
└─────────────────────────────────────────────────────────────────────┘

LAYER 1: INFRASTRUCTURE (External to Services)
═══════════════════════════════════════════════
┌─────────────────────────────────────────────────────────────────────┐
│  MongoDB:27017  │  Redis:6379  │  Kafka:9094  │  MinIO:9000        │
└─────────────────────────────────────────────────────────────────────┘
         ▲                ▲               ▲               ▲
         │ IMENV_*        │ IMENV_*       │ IMENV_*       │ IMENV_*
         │ overrides      │ overrides     │ overrides     │ overrides


LAYER 2: FOUNDATION RPC SERVICES (No RPC Dependencies)
═══════════════════════════════════════════════════════
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  auth-rpc-server │  │  third-rpc-server │  │  push-rpc-server │
│     (2/2)        │  │      (2/2)        │  │      (2/2)       │
│                  │  │                  │  │                  │
│ Ports:           │  │ Ports:            │  │ Ports:           │
│ RPC: 10200       │  │ RPC: 10300        │  │ RPC: 10170       │
│ Metrics: 12200   │  │ Metrics: 12300    │  │ Metrics: 12170   │
│                  │  │                  │  │                  │
│ Dependencies:    │  │ Dependencies:     │  │ Dependencies:    │
│ • MongoDB        │  │ • MongoDB         │  │ • Redis          │
│ • Redis          │  │ • Redis           │  │ • Kafka          │
│                  │  │ • MinIO           │  │                  │
└──────────────────┘  └──────────────────┘  └──────────────────┘


LAYER 3: PRIMARY RPC SERVICES (Single Level Dependencies)
══════════════════════════════════════════════════════════

┌──────────────────────┐
│ user-rpc-server      │  ◄─────┐
│    (1/1)             │        │
│                      │        │
│ Ports:               │        │
│ RPC: 10320           │        │ Requires these RPC services
│ Metrics: 12320       │        │ at startup (via GetConn)
│                      │        │
│ Dependencies:        │        │
│ ► msg-rpc-server     │        │
│ ► group-rpc-server   │        │
│ ► friend-rpc-server  │        │
└──────────────────────┘        │
         ▲                       │
         │                       │
         └───────────────────────┘


LAYER 4: INTERCONNECTED RPC SERVICES (Cross Dependencies)
══════════════════════════════════════════════════════════

msg-rpc-server (2/2)               friend-rpc-server (2/2)
Ports: 10280, 12280                Ports: 10240, 12240
RPC Port: 10280                     RPC Port: 10240
Metrics: 12280                      Metrics: 12240
┌────────────────────────────┐     ┌──────────────────────┐
│ Dependencies:              │     │ Dependencies:        │
│ ► user-rpc-server ◄───────┼─────┤ ► user-rpc-server    │
│ ► group-rpc-server (see ▼) │     │ ► msg-rpc-server ◄──┼─ (this service)
│ ► friend-rpc-server ◄──┐   │     └──────────────────────┘
│ ► conversation-rpc ◄───┼───┼─────────────────┐
└────────────────────────────┘                 │
         ▲                                      ▼
         │                           ┌──────────────────────┐
         │                           │group-rpc-server (2/2)│
         └───────────────────────────┤ Ports: 10260, 12260 │
                                     │                      │
                        ┌────────────┤ Dependencies:        │
                        │            │ ► user-rpc-server    │
                        │            │ ► msg-rpc-server ◄───┤ (this service)
                        │            │ ► conversation-rpc ◄─┤
                        │            └──────────────────────┘
                        │                      ▲
                        │                      │
         ┌──────────────┴──────────────────────┘
         │
         ▼
┌──────────────────────────────────┐
│ conversation-rpc-server (2/2)    │
│ Ports: 10220, 12220              │
│                                  │
│ Dependencies:                    │
│ ► user-rpc-server                │
│ ► group-rpc-server               │
│ ► msg-rpc-server                 │
└──────────────────────────────────┘


LAYER 5: GATEWAY & EDGE SERVICES
═════════════════════════════════

openim-api (2/2)                    messagegateway-rpc-server (1/1)
Ports: 10002, 12002                 Ports: 10140, 12140, 10001
┌──────────────────────────────┐   ┌─────────────────────────────┐
│ HTTP REST API Server         │   │ WebSocket/LongConn Gateway  │
│                              │   │                             │
│ Dependencies:                │   │ Init Dependencies:          │
│ ► user-rpc-service           │   │ ► user-rpc-service          │
│ ► msg-rpc-service            │   │                             │
│ ► group-rpc-service          │   │ Lazy Dependencies (via      │
│ ► friend-rpc-service         │   │  rpccache/message handlers):│
│ ► auth-rpc-service           │   │ ► msg-rpc-service          │
│ ► conversation-rpc-service   │   │ ► conversation-rpc-service │
│ ► third-rpc-service          │   │ ► group-rpc-service        │
└──────────────────────────────┘   │ ► friend-rpc-service       │
                                     └─────────────────────────────┘

openim-msgtransfer (2/2)
Metrics Port: 12020
┌────────────────────────────────┐
│ Message Transfer Worker        │
│ (Kafka Consumer)               │
│                                │
│ Dependencies: NONE (RPC)       │
│ • Kafka (consumer only)        │
│ • Redis (optional state)       │
│ • MongoDB (optional logging)   │
└────────────────────────────────┘
```

### Dependency Summary Table

| Service | Type | Replicas | Init Dependencies | Lazy Dependencies | External Dependencies |
|---------|------|----------|-------------------|-------------------|----------------------|
| **auth-rpc** | RPC | 2 | None | None | MongoDB, Redis |
| **user-rpc** | RPC | 1 | msg, group, friend | None | MongoDB, Redis |
| **msg-rpc** | RPC | 2 | user, group, friend, conversation | None | MongoDB, Redis, Kafka |
| **group-rpc** | RPC | 2 | user, msg, conversation | None | MongoDB, Redis |
| **friend-rpc** | RPC | 2 | user, msg | None | MongoDB, Redis |
| **conversation-rpc** | RPC | 2 | user, group, msg | None | MongoDB, Redis |
| **third-rpc** | RPC | 2 | None | None | MongoDB, Redis, MinIO |
| **push-rpc** | RPC | 2 | None | None | Redis, Kafka |
| **messagegateway-rpc** | Gateway | 1 | user | msg, conversation, group, friend | Redis, WebSocket (10001) |
| **openim-api** | REST | 2 | All 7 RPC services | None | MongoDB, Redis |
| **openim-msgtransfer** | Worker | 2 | None | None | Kafka, Redis, MongoDB |

---

## Service Discovery Architecture

### Kubernetes Mode Configuration

**File:** `config/discovery.yml`
```yaml
discovery:
  enable: "k8s"                    # Kubernetes service discovery mode
  kubernetes:
    namespace: "openim"            # Kubernetes namespace
```

### Service Registration Pattern

Each RPC service registers with a unique service name via **gRPC Resolver**:

```
┌─────────────────────────────────────┐
│  Kubernetes Service (headless)      │
│  Name: user-rpc-service             │
│  ClusterIP: None                    │
│  Endpoints: [pod1, pod2, ...]       │
└─────────────────────────────────────┘
         ▲
         │ DNS resolution
         │ user-rpc-service.openim.svc.cluster.local
         │
┌─────────────────────────────────────┐
│  Pod Inside Service                 │
│  app: user-rpc-server               │
│  container: openim-server           │
│  image: openim/openim-server:...    │
│                                     │
│  Config: config/openim-rpc-user.yml │
│  registerName: user-rpc-service     │
└─────────────────────────────────────┘
         ▲
         │ GetConn(ctx, "user-rpc-service")
         │
┌─────────────────────────────────────┐
│  Other Service Needing Connection   │
│  msg-rpc-server pod                 │
│  Uses discovery.SvcDiscoveryRegistry│
└─────────────────────────────────────┘
```

### Service Names (Register Names)

These names must match between:
1. Kubernetes Service metadata (`metadata.name`)
2. Service Config file (`registerName` field)
3. Source code discovery calls (`config.Discovery.RpcService.User`)

**Configured Service Names:**
```
user-rpc-service              # User service
msg-rpc-service               # Message service
group-rpc-service             # Group service
friend-rpc-service            # Friend/relation service
auth-rpc-service              # Authentication service
conversation-rpc-service      # Conversation service
third-rpc-service             # Third-party/storage service
messagegateway-rpc-service    # Message gateway service
openim-api-service            # REST API service
openim-msgtransfer-service    # Message transfer service
openim-push-service           # Push notification service
```

---

## Configuration Pattern

### Multi-Config Loading

Each service loads configuration from multiple YAML files:

```
Service Startup:
  1. Load {service}-rpc-{name}.yml (RPC configuration)
  2. Load share.yml (shared configuration)
  3. Load discovery.yml (service discovery settings)
  4. Load mongodb.yml (MongoDB settings)
  5. Load redis.yml (Redis settings)
  6. Load kafka.yml (Kafka settings) - if needed
  7. Load notification.yml (notification settings)
  8. Load webhooks.yml (webhook settings)
  9. Load local-cache.yml (cache settings)
```

### Environment Variable Overrides

Kubernetes environment variables override YAML configuration:

```yaml
env:
  - name: CONFIG_PATH
    value: "/openim-server/config"
  - name: IMENV_MONGODB_ADDRESS
    value: "mongo:27017"
  - name: IMENV_REDIS_ADDRESS
    value: "redis:6379"
  - name: IMENV_KAFKA_ADDRESS
    value: "kafka:9094"
  - name: IMENV_MINIO_INTERNALADDRESS
    value: "minio:9000"
  - name: IMENV_MINIO_ACCESSKEYID
    value: "root"
  - name: IMENV_MINIO_SECRETACCESSKEY
    value: "openIM123"
  - name: IMENV_DISCOVERY_ENABLE
    value: "k8s"
```

---

## Deployment Requirements

### 1. RBAC Permissions

**ServiceAccount:** `openim-services`

**Required ClusterRole Rules:**
```yaml
rules:
  # Endpoints for service discovery (legacy)
  - apiGroups: [""]
    resources: ["endpoints"]
    verbs: ["get", "list", "watch"]
  
  # EndpointSlices for service discovery (Kubernetes 1.21+)
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["get", "list", "watch"]
  
  # Services for metadata
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["get", "list"]
```

**Why Required:**
- Kubernetes discovery requires permission to list Endpoints/EndpointSlices
- Each pod needs to watch for changes to service endpoints
- Pod restarts are automatically detected via EndpointSlice updates

### 2. ConfigMaps Required

**openim-config** - Main service configuration
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: openim-config
  namespace: openim
data:
  openim-api.yml: |
    # REST API configuration
  openim-rpc-auth.yml: |
    # Auth RPC configuration
  # ... other service configs
```

**openim-start-config-{service}** - Service-specific startup config
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: openim-start-config-user
  namespace: openim
data:
  start-config.yml: |
    serviceBinaries:
      openim-rpc-user: 1
    toolBinaries:
      - check-component
      - check-free-memory
    maxFileDescriptors: 10000
```

### 3. Kubernetes Services (Headless)

**Required for each RPC service:**
```yaml
apiVersion: v1
kind: Service
metadata:
  name: user-rpc-service
  namespace: openim
spec:
  clusterIP: None              # Headless service
  selector:
    app: user-rpc-server
  ports:
    - name: rpc
      port: 10320
      targetPort: 10320
    - name: metrics
      port: 12320
      targetPort: 12320
```

---

## Service Startup Order

### Critical Path Analysis

The deployment uses **lazy initialization** - services retry connections until other services are available. However, optimal startup order is:

#### Phase 1: Foundation Services (Parallel, ~15 seconds)
```
auth-rpc-server    (no RPC deps)
third-rpc-server   (no RPC deps)
push-rpc-server    (no RPC deps)
```

#### Phase 2: Primary Support Services (Parallel, ~20 seconds)
```
friend-rpc-server  (needs user + msg)
conversation-rpc-server (needs user + group + msg)
```

#### Phase 3: Core RPC Services (Parallel, ~20 seconds)
```
user-rpc-server    (needs msg + group + friend)
msg-rpc-server     (needs user + group + friend + conversation)
group-rpc-server   (needs user + msg + conversation)
```

#### Phase 4: Gateway Services (Parallel, ~10 seconds)
```
messagegateway-rpc-server  (needs user only at init)
openim-api                 (needs all RPC services)
openim-msgtransfer         (no RPC deps)
```

**Total Time:** ~60-90 seconds for complete deployment

### Actual Deployment Behavior

Kubernetes scheduling handles most of this automatically:
1. All deployments created simultaneously
2. Pods scheduled as resources available
3. Services retry GetConn() with backoff
4. After 2-3 minutes, all services stable and connected

---

## Health Checks & Initialization

### Component Verification

Each RPC service validates infrastructure on startup:

**check-component tool checks:**
1. **MongoDB** - Connection, authentication, database creation
2. **Redis** - Connection, password authentication, key operations
3. **Kafka** - Producer connection, SASL configuration
4. **MinIO** - Credentials, bucket access (for third-rpc)

**Log Evidence:**
```
[2026-04-18 08:41:16 UTC] RPC server is initializing
[2026-04-18 08:41:16 UTC] All components checks passed
[2026-04-18 08:41:16 UTC] Cmdline: /openim-server/_output/bin/platforms/linux/amd64/openim-rpc-user -i 0 -c /openim-server/config/
[2026-04-18 08:41:17 UTC] PID: 1540 is listening on ports: 10320, 12320
```

### Liveness Indicators

For each service pod:
```bash
# Pod enters Running state
kubectl get pods -n openim {service}-{pod-hash}
# Status.Phase = Running

# Service is listening
kubectl logs -n openim {pod-name} | grep "listening on ports"

# No service discovery errors
kubectl logs -n openim {pod-name} | grep -v "403\|connection refused"

# Metrics endpoint available
curl http://{pod-ip}:12{xxx}/metrics
```

---

## Troubleshooting Guide

### Common Issues & Solutions

#### 1. Pod Status: `CrashLoopBackOff`

**Symptom:** Pod restarts repeatedly
```
NAME                            READY   STATUS             RESTARTS
msg-rpc-server-xxx              0/1     CrashLoopBackOff   5
```

**Root Causes & Fixes:**

a) **Nil Pointer in etcd client**
   - Issue: Image v3.8.3 has nil pointer bug
   - Fix: Upgrade to `openim/openim-server:v3.8.3-patch.12`

b) **Missing ConfigMap**
   ```bash
   # Check ConfigMap exists
   kubectl get cm -n openim | grep openim-start-config-user
   
   # Create if missing
   kubectl create configmap openim-start-config-user \
     --from-file=start-config.yml=/path/to/start-config.yml \
     -n openim
   ```

c) **Component Connectivity Failure**
   ```bash
   # Check logs
   kubectl logs -n openim {pod-name}
   
   # Verify MongoDB connectivity
   kubectl run debug --image=mongo:latest -it -- \
     mongosh mongodb://openIM:openIM123@mongo:27017/openim
   
   # Verify Redis connectivity
   kubectl run debug --image=redis:latest -it -- \
     redis-cli -h redis -a password ping
   ```

#### 2. 403 Forbidden Errors in Logs

**Symptom:**
```
invalid response code 403 for service user in namespace openim
```

**Root Cause:** ServiceAccount lacks permissions

**Fix:**
```bash
# Verify ServiceAccount
kubectl get sa -n openim openim-services

# Verify ClusterRole
kubectl get clusterrole | grep openim

# Verify ClusterRoleBinding
kubectl get clusterrolebinding | grep openim

# Add missing permissions (if needed)
kubectl create clusterrole openim-discovery \
  --verb=get,list,watch \
  --resource=endpoints,endpointslices

kubectl create clusterrolebinding openim-discovery \
  --clusterrole=openim-discovery \
  --serviceaccount=openim:openim-services
```

#### 3. Service Discovery Not Working

**Symptom:** Services cannot connect to each other
```
failed to resolve service user in namespace openim
```

**Diagnostics:**
```bash
# Check service exists
kubectl get svc -n openim user-rpc-service

# Check service has endpoints
kubectl get endpoints -n openim user-rpc-service

# Check DNS resolution from pod
kubectl exec -it {pod-name} -n openim -- \
  nslookup user-rpc-service.openim.svc.cluster.local

# Check gRPC connectivity
kubectl exec -it {pod-name} -n openim -- \
  grpcurl -plaintext user-rpc-service:10320 list
```

**Fix:**
```bash
# Verify discovery.yml has "k8s"
kubectl get cm openim-config -n openim -o yaml | grep -A2 discovery.yml

# Verify services match config registerName
kubectl get svc -n openim | grep -E "rpc|gateway|api|transfer"

# Restart affected pods
kubectl rollout restart deployment/{service}-name -n openim
```

#### 4. Kafka Component Check Failures

**Symptom:** MsgTransfer/Push pods show Kafka errors
```
Kafka check failed: Error: kafka: invalid configuration
(Net.SASL.User must not be empty when SASL is enabled)
```

**Root Cause:** SASL user not configured but password is

**Fix:**
- For non-SASL Kafka: Remove SASL configuration
- For SASL Kafka: Add `IMENV_KAFKA_USER` environment variable
- For now, services continue running despite Kafka check failure

#### 5. Services Cannot Find Other RPC Services

**Symptom:** One service can't connect to another
```
rpc error: code = Unavailable desc = connection error:
desc = "transport: Error while dialing dial unix
/var/run/docker.sock: connect: permission denied"
```

**Diagnostics:**
```bash
# Check service order of startup
kubectl get pods -n openim --sort-by=.metadata.creationTimestamp | head -20

# Check if dependency service is running
kubectl get pods -n openim | grep {dependency-service}

# Check connectivity between services
kubectl exec -it {pod1} -n openim -- \
  telnet {service2}-pod-ip 10320
```

**Fix:**
```bash
# Verify all RPC services are Running
kubectl get pods -n openim | grep rpc | grep Running

# If some aren't running, check their logs
kubectl logs -n openim {service}-{pod-hash} -f

# Wait for dependency service to start
kubectl wait --for=condition=Ready pod \
  -l app=user-rpc-server -n openim --timeout=300s

# Then manually trigger pod restart
kubectl delete pod {failed-pod} -n openim
```

---

## Performance Tuning

### Recommended Resource Limits

```yaml
# For RPC services
resources:
  requests:
    memory: "256Mi"
    cpu: "200m"
  limits:
    memory: "512Mi"
    cpu: "500m"

# For API/Gateway
resources:
  requests:
    memory: "512Mi"
    cpu: "300m"
  limits:
    memory: "1Gi"
    cpu: "1000m"

# For MsgTransfer (higher throughput)
resources:
  requests:
    memory: "512Mi"
    cpu: "400m"
  limits:
    memory: "2Gi"
    cpu: "2000m"
```

### Pod Disruption Budget (Optional HA)

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: openim-rpc-pdb
  namespace: openim
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: msg-rpc-server
```

---

## Production Checklist

- [ ] All Kubernetes services created (headless, clusterIP: None)
- [ ] ServiceAccount `openim-services` created with proper RBAC
- [ ] ConfigMaps created for all services
- [ ] Image version pinned to `v3.8.3-patch.12` or later
- [ ] Environment variables for infrastructure set (IMENV_*)
- [ ] MongoDB, Redis, Kafka, MinIO all accessible
- [ ] Pod logs checked for 403 errors and component check failures
- [ ] Service discovery working (cross-service connectivity verified)
- [ ] All pods in Running state
- [ ] Prometheus metrics endpoints accessible (port 12xxx)
- [ ] Health checks configured (liveness/readiness probes optional)

---

## Related Documentation

- [docs/contrib/environment.md](./contrib/environment.md) - Environment variable reference
- [config/discovery.yml](../config/discovery.yml) - Discovery configuration
- [config/share.yml](../config/share.yml) - Shared configuration including RPC service names
- [deployments/deploy/](../deployments/deploy/) - Kubernetes deployment manifests

---

**Document Version:** 1.0  
**Last Verified:** April 18, 2026  
**Kubernetes Version:** 1.20+ (EndpointSlices support)  
**OpenIM Version:** v3.8.3-patch.12
