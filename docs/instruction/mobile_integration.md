# Mobile Integration Reminders

## MinIO Configuration (`externalAddress`)

When integrating with mobile SDKs, it is critical to ensure that the `externalAddress` in the MinIO configuration is set to a publicly reachable address.

### Key Points:
- **`internalAddress`**: Used by the OpenIM server for internal operations (e.g., within the Kubernetes cluster). Usually set to internal DNS like `minio-service:10005`.
- **`externalAddress`**: Used to generate **presigned URLs** for file downloads and uploads. These URLs are sent to the mobile client.
- **The Problem**: If `externalAddress` is set to an internal address (like `http://minio-service:10005`), the mobile client will be unable to resolve or connect to it from outside the internal network.
- **The Requirement**: For mobile integration, `externalAddress` must be set to a public IP address or a domain name configured on your Load Balancer / Ingress (e.g., `https://minio.example.com`).

> [!IMPORTANT]
> Failure to set a proper `externalAddress` will result in file upload/download failures on mobile devices, even if everything works correctly in an internal simulation environment.
