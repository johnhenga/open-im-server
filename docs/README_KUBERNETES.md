# OpenIM Kubernetes Deployment Documentation

This folder contains comprehensive documentation for deploying OpenIM on Kubernetes with proper service discovery, configuration management, and dependency handling.

## Documents

### 1. **[DEPLOYMENT_ARCHITECTURE.md](./DEPLOYMENT_ARCHITECTURE.md)** ⭐ START HERE
   - Complete deployment architecture overview
   - **Service Dependency Graph** - Visual representation of all service relationships
   - Kubernetes service discovery configuration
   - RBAC requirements and setup
   - Multi-config pattern explanation
   - Health checks and initialization process
   - Comprehensive troubleshooting guide

### 2. **[SERVICE_DEPENDENCY_MATRIX.md](./SERVICE_DEPENDENCY_MATRIX.md)** ⭐ QUICK REFERENCE
   - Quick lookup table of all service dependencies
   - Service port assignments
   - Service discovery names (registerNames)
   - Common operational commands
   - Circular dependency explanation
   - Quick fixes for common issues
   - Monitoring checklist

### 3. **[../config/](../config/)** - Configuration Files
   - `discovery.yml` - Service discovery settings
   - `share.yml` - Shared configuration with RPC service names
   - `openim-api.yml` - REST API configuration
   - `openim-rpc-*.yml` - Individual RPC service configs

### 4. **[../deployments/deploy/](../deployments/deploy/)** - Kubernetes Manifests
   - Service definitions (headless services for DNS discovery)
   - Deployment manifests (StatefulSets/Deployments for each service)
   - ConfigMap templates
   - ServiceAccount and RBAC definitions

---

## Quick Start

### Understanding the Architecture (5 minutes)

1. Read: [Service Dependency Graph](#service-dependency-graph) section in DEPLOYMENT_ARCHITECTURE.md
2. Understand: RPC services vs. Edge services distinction
3. Note: Circular dependencies are handled by Kubernetes scheduling

### Deploying OpenIM (10 minutes)

1. **Prerequisites:**
   - Kubernetes 1.20+ (for EndpointSlices support)
   - kubectl configured to access your cluster
   - openim namespace created

2. **Create RBAC:**
   ```bash
   kubectl apply -f rbac/
   ```

3. **Create ConfigMaps:**
   ```bash
   kubectl apply -f configmaps/
   ```

4. **Deploy Services:**
   ```bash
   kubectl apply -f services/
   kubectl apply -f deployments/
   ```

5. **Monitor Startup:**
   ```bash
   watch kubectl get pods -n openim
   ```

### Troubleshooting (Refer to DEPLOYMENT_ARCHITECTURE.md)

- Pod in CrashLoopBackOff? → [Issue 1: CrashLoopBackOff](./DEPLOYMENT_ARCHITECTURE.md#1-pod-status-crashloopbackoff)
- 403 Forbidden errors? → [Issue 2: 403 Forbidden](./DEPLOYMENT_ARCHITECTURE.md#2-403-forbidden-errors-in-logs)
- Services can't find each other? → [Issue 3: Service Discovery](./DEPLOYMENT_ARCHITECTURE.md#3-service-discovery-not-working)

---

## Key Concepts

### Service Layers

```
┌────────────────────────────────────────┐
│  EDGE SERVICES                         │
│  • openim-api (REST API)              │
│  • messagegateway (WebSocket)         │
│  • openim-msgtransfer (Worker)        │
└────────────────────────────────────────┘
                    ▲
                    │
        ┌───────────┴────────────┐
        │                        │
┌───────▼─────────────┐ ┌────────▼──────────┐
│ RPC SERVICES        │ │ RPC SERVICES      │
│ (With Deps)         │ │ (No Deps)         │
│ • user-rpc          │ │ • auth-rpc        │
│ • msg-rpc           │ │ • third-rpc       │
│ • group-rpc         │ │ • push-rpc        │
│ • friend-rpc        │ │                   │
│ • conversation-rpc  │ │                   │
└─────────────────────┘ └───────────────────┘
                    ▲
                    │
        ┌───────────┴───────────┐
        │                       │
      MongoDB              Redis, Kafka, MinIO
```

### Service Discovery Mode

All services use **Kubernetes service discovery** (`k8s` mode):
- Services are registered via Kubernetes DNS
- Service names must match Kubernetes Service object names
- Headless services (ClusterIP: None) used for direct pod access
- RBAC ServiceAccount required for Endpoints/EndpointSlices permissions

### Configuration Pattern

Each service loads:
1. **Service-specific config** (e.g., `openim-rpc-user.yml`)
2. **Shared config** (e.g., `share.yml` with RPC service names)
3. **Discovery config** (e.g., `discovery.yml`)
4. **Infrastructure configs** (MongoDB, Redis, Kafka)

Environment variables (IMENV_*) override YAML values.

---

## Dependency Highlights

### Critical Paths

**Longest dependency chain:**
```
openim-api → user-rpc → group-rpc → conversation-rpc → msg-rpc
```

**Circular dependencies:**
- msg-rpc needs user, group, friend, conversation
- group-rpc needs msg
- friend-rpc needs msg
- conversation-rpc needs msg

These are resolved by **lazy initialization** - services retry until dependencies available.

### Service Port Ranges

- **RPC Ports:** 10200-10320 (distributed across services)
- **Metrics Ports:** 12200-12320 (prometheus metrics)
- **WebSocket:** 10001 (messagegateway for clients)
- **REST API:** 10002 (HTTP API)

---

## RBAC Requirements

ServiceAccount `openim-services` needs:

```yaml
rules:
  - apiGroups: [""]
    resources: ["endpoints", "services"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["get", "list", "watch"]
```

**Why:** Kubernetes service discovery requires permission to read Endpoints/EndpointSlices for service resolution.

---

## Deployment Checklist

- [ ] Read DEPLOYMENT_ARCHITECTURE.md service dependency graph
- [ ] Create openim namespace
- [ ] Create ServiceAccount with RBAC permissions
- [ ] Create ConfigMaps from config/ folder
- [ ] Create Kubernetes Services (headless)
- [ ] Deploy RPC services (all can deploy in parallel)
- [ ] Deploy Edge services (after RPC services stable)
- [ ] Verify all pods Running (2-3 minutes)
- [ ] Check logs for no 403 errors
- [ ] Verify service discovery working (cross-pod connectivity)
- [ ] Check Prometheus metrics endpoints

---

## Operational Commands

```bash
# Check all services
kubectl get all -n openim

# Watch pod status
watch kubectl get pods -n openim

# Check service endpoints
kubectl get endpoints -n openim

# View logs
kubectl logs -n openim {service}-{pod-hash}

# Test service discovery
kubectl exec -it {pod} -n openim -- \
  nslookup user-rpc-service.openim.svc.cluster.local

# Scale a deployment
kubectl scale deployment user-rpc-server -n openim --replicas=3

# Restart a service
kubectl rollout restart deployment/user-rpc-server -n openim
```

---

## Performance Considerations

### Startup Time

- **Initialization:** 30-60 seconds per pod
- **Service Discovery:** 30-60 seconds for all connections established
- **Total deployment:** 2-3 minutes for all services Ready

### Resource Usage

- **Each RPC pod:** ~300MB memory, 200m CPU at baseline
- **API pod:** ~500MB memory, 300m CPU
- **MsgTransfer:** ~600MB memory, 400m CPU (throughput dependent)

### Scaling

Each service can be scaled independently:
- Most RPC services: scale to 3+ replicas for HA
- User RPC: typically 1-2 replicas (stateless)
- API: scale 2-4 replicas based on load
- MsgTransfer: scale 2-3 replicas for throughput

---

## Source Code References

Deployment architecture is based on:
- `pkg/common/discoveryregister/discoveryregister.go` - Discovery logic
- `pkg/common/config/config.go` - RpcRegisterName definitions
- `internal/*/init.go` - Service startup patterns
- `pkg/common/startrpc/start.go` - gRPC server startup

See DEPLOYMENT_ARCHITECTURE.md for detailed source code citations.

---

## Troubleshooting Matrix

| Problem | Symptom | Reference |
|---------|---------|-----------|
| Nil pointer crash | CrashLoopBackOff | [Issue 1](./DEPLOYMENT_ARCHITECTURE.md#1-pod-status-crashloopbackoff) |
| No permissions | 403 Forbidden in logs | [Issue 2](./DEPLOYMENT_ARCHITECTURE.md#2-403-forbidden-errors-in-logs) |
| Can't find service | Connection refused | [Issue 3](./DEPLOYMENT_ARCHITECTURE.md#3-service-discovery-not-working) |
| Kafka errors | Component check failed | [Issue 4](./DEPLOYMENT_ARCHITECTURE.md#4-kafka-component-check-failures) |
| Circular deps | Service won't connect | [Issue 5](./DEPLOYMENT_ARCHITECTURE.md#5-services-cannot-find-other-rpc-services) |

---

## Related Documentation

- [Main README](../README.md) - OpenIM project overview
- [Contributing Guide](../CONTRIBUTING.md) - Development guidelines
- [Environment Reference](./contrib/environment.md) - Environment variables
- [Configuration Guide](../config/README.md) - Configuration file reference

---

**Documentation Version:** 1.0  
**Last Updated:** April 18, 2026  
**OpenIM Version:** v3.8.3-patch.12+  
**Kubernetes:** 1.20+

---

## How to Use This Documentation

### For Developers
- Start with [SERVICE_DEPENDENCY_MATRIX.md](./SERVICE_DEPENDENCY_MATRIX.md) for quick reference
- Refer to source code links in [DEPLOYMENT_ARCHITECTURE.md](./DEPLOYMENT_ARCHITECTURE.md)

### For Operations
- Use [SERVICE_DEPENDENCY_MATRIX.md](./SERVICE_DEPENDENCY_MATRIX.md) for day-to-day operations
- Refer to [DEPLOYMENT_ARCHITECTURE.md](./DEPLOYMENT_ARCHITECTURE.md) troubleshooting section for issues

### For New Deployments
1. Read entire [DEPLOYMENT_ARCHITECTURE.md](./DEPLOYMENT_ARCHITECTURE.md)
2. Follow the Deployment Checklist above
3. Keep [SERVICE_DEPENDENCY_MATRIX.md](./SERVICE_DEPENDENCY_MATRIX.md) open as reference

---

**Questions? Refer to the [Troubleshooting Guide](./DEPLOYMENT_ARCHITECTURE.md#troubleshooting-guide) in DEPLOYMENT_ARCHITECTURE.md**
