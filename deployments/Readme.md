# Kubernetes Deployment

The OpenIM server is deployed with Helm. The single, canonical deployment lives in
[`scp-cpall-7app-dev/`](./scp-cpall-7app-dev/) — a reduced, cluster-internal install of the full
stack (mongodb, redis, kafka, minio, openim-server, openim-chat) into the `scp-cpall-7app-dev`
namespace.

See [`scp-cpall-7app-dev/README.md`](./scp-cpall-7app-dev/README.md) for the deltas, install
command, smoke test, and teardown.

> The previous raw-manifest "Origin Deploy" (`deployments/deploy/`) has been removed to avoid
> keeping two parallel deployment definitions. The Helm charts it now relies on live in
> `backend/helm-charts` (`charts/openim-server`, `charts/openim-chat`, `infra/*`); a copy of the
> server umbrella chart also lives here under [`charts/`](./charts/).

## Resource Requests

- CPU: 2 cores
- Memory: 4 GiB
- Disk usage: 20 GiB (on Node)

## Quick start

```bash
cd backend/helm-charts
./deploy/scp-cpall-7app-dev/install.sh   # idempotent (helm upgrade --install)
```
