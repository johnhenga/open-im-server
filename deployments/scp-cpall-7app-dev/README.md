# OpenIM dev clone — namespace `scp-cpall-7app-dev`

A reduced, cluster-internal clone of the `openim` namespace on the `dev-sds-sdo-dev-3az-eks`
cluster. Deployed 2026-06-22.

## What this is

Same charts and chart versions as the `openim` namespace (mongodb 14.0.0, redis 18.1.0,
kafka 25.1.11, minio 12.8.8, openim-api 0.1.17, admin-api 0.1.17). Base values were captured
live with `helm get values <release> -n openim` (the repo `config-*.yaml` had drifted); the
`*.yaml` override files in this dir carry only the deltas for this namespace.

Releases: `im-mongodb`, `im-redis`, `im-kafka`, `im-minio`, `openimserver`, `openim-chat`.

## Deltas vs the `openim` namespace

- **Reduced sizing (dev):** mongo 1-member replicaset (`rs0`, no arbiter), redis master only
  (0 replicas), kafka 1 controller (replication factor 1), minio standalone. OpenIM components
  run at chart-default replicas (most are 1; `openim-msgtransfer` defaults to 3 — not further
  minimized).
- **No ingress:** cluster-internal only. All `ingress.enabled: false`. Reach services via
  ClusterIP / `kubectl port-forward`. (Host + private DNS deferred — `openim1.server.top` is
  taken and path-routed on the shared nginx controller.)
- **Image fix — Bitnami purge:** Bitnami removed legacy images from `public.ecr.aws/bitnami`
  (Aug-2025 "Bitnami Secure Images" migration). Infra images are repointed to the official
  relocation `docker.io/bitnamilegacy/*`, pulled with the `docker-hub` secret (copied from
  `seven-deli-chat-dev`).
- **OpenIM image:** all 12 server components use the same custom combined image
  `971022779850.dkr.ecr.ap-southeast-1.amazonaws.com/sds-scp-tmp:openim-server-push-fix`
  (the live `openim` runtime image; the chart's `ghcr.io/openimsdk/*` values were stale).
  Chat uses upstream `ghcr.io/openimsdk/chat-*:release-v1.6` (matches the source's last-good
  helm manifest). ECR pulls via node IAM (no secret).
- **Chat service-ref fixes:** `config.mongo.address -> im-mongodb`, `config.openIMUrl ->
  http://openimserver-openim-api` (the captured values had stale names).

## Install / update

```bash
cd backend/helm-charts
./deploy/scp-cpall-7app-dev/install.sh   # idempotent (helm upgrade --install)
```

## Smoke test (in-cluster)

OpenIM returns HTTP 200 even on app errors — check the `errCode` in the body, and note every
request needs an `operationID` header.

```bash
kubectl run smoke --rm -i --restart=Never -n scp-cpall-7app-dev \
  --image=public.ecr.aws/docker/library/alpine -- sh -c \
  'apk add -q curl && curl -s -X POST http://openimserver-openim-api/auth/get_admin_token \
   -H "Content-Type: application/json" -H "operationID: smoke1" \
   -d "{\"secret\":\"openIM123\",\"userID\":\"imAdmin\"}"'
# expect: {"errCode":0,...,"data":{"token":"..."}}   (proves api -> rpc-auth -> mongo)
```

## Teardown

```bash
for r in openim-chat openimserver im-kafka im-minio im-redis im-mongodb; do
  helm uninstall $r -n scp-cpall-7app-dev; done
kubectl delete pvc --all -n scp-cpall-7app-dev      # PVCs are retained by the charts
kubectl delete namespace scp-cpall-7app-dev
```

## Not deployed

`app` (DevSpace python dev container — not OpenIM) and the prometheus exporters (monitoring).
Add an ingress + private DNS host when external access is needed.
