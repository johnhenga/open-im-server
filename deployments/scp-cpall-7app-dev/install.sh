#!/bin/bash
# Deploy the OpenIM stack into namespace scp-cpall-7app-dev (dev clone of the `openim` namespace).
# Run from the helm-charts repo root. Idempotent: uses `helm upgrade --install`.
set -euo pipefail

NS=scp-cpall-7app-dev
D="deploy/$NS"

kubectl get namespace "$NS" >/dev/null 2>&1 || kubectl create namespace "$NS"

# Pull secret for docker.io/bitnamilegacy/* (bitnami purged public.ecr.aws/bitnami).
# Copied once from seven-deli-chat-dev; recreate if missing.
kubectl get secret docker-hub -n "$NS" >/dev/null 2>&1 || \
  kubectl get secret docker-hub -n seven-deli-chat-dev -o yaml \
    | sed '/namespace:/d;/resourceVersion:/d;/uid:/d;/creationTimestamp:/d' \
    | sed '/managedFields:/,/^[^ ]/d' \
    | kubectl apply -n "$NS" -f -

# Infra (base values from live `openim` release + dev/image overrides)
helm upgrade --install im-mongodb infra/mongodb -n "$NS" -f "$D/values-im-mongodb.yaml" -f "$D/mongodb.yaml"
helm upgrade --install im-redis   infra/redis   -n "$NS" -f "$D/values-im-redis.yaml"   -f "$D/redis.yaml"
helm upgrade --install im-minio   infra/minio   -n "$NS" -f "$D/values-im-minio.yaml"   -f "$D/minio.yaml"
helm upgrade --install im-kafka   infra/kafka   -n "$NS" -f "$D/values-im-kafka.yaml"   -f "$D/kafka.yaml" --timeout 8m

# OpenIM server + chat
helm upgrade --install openimserver charts/openim-server -n "$NS" -f "$D/values-openimserver.yaml" -f "$D/openimserver.yaml"
helm upgrade --install openim-chat  charts/openim-chat   -n "$NS" -f "$D/values-openim-chat.yaml"  -f "$D/openim-chat.yaml"

kubectl get pods -n "$NS"
