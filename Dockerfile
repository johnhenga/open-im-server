# OpenIM server: one image holds the program of every service. Kubernetes starts each service by
# running its program directly (the chart's command: <global.binaryPath>/openim-<service> -i 0 -c /config/),
# so the image holds only the programs: no Go toolchain, no mage, no shell, no package manager.
# It runs as a non-root user. Config, TLS CA files and secrets are mounted by the chart.
#
# Build (--provenance=false --sbom=false: the tag is then the image itself, so the digest the chart
# pins is the digest Inspector scans):
#   docker buildx build --platform linux/amd64 --provenance=false --sbom=false -t <repo>:openim-server-v-<sha> --push .
FROM golang:1.25-alpine@sha256:1ae0735f00daffa3aaf1363a5184c0d2dc55c78e3db4ec70241cdac97bf84b59 AS builder

WORKDIR /openim-server
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# Static programs (no C library) without debug data: the same flags as `mage build` in release mode.
RUN mkdir /out && for d in cmd/openim-api cmd/openim-cmdutils cmd/openim-crontask cmd/openim-msggateway \
      cmd/openim-msgtransfer cmd/openim-push cmd/openim-rpc/openim-rpc-*; do \
      CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/$(basename $d) ./$d || exit 1; \
    done

# distroless static: CA certificates and time zone data, nothing else.
FROM gcr.io/distroless/static:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3

# The same path as the old image, so the chart's global.binaryPath does not change.
COPY --from=builder /out/ /openim-server/_output/bin/platforms/linux/amd64/

# 65532 is distroless's nonroot user. A number, so Kubernetes can check runAsNonRoot.
USER 65532:65532
