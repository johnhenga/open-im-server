# OpenIM Server Architecture

This diagram illustrates the high-level architecture of OpenIM Server, designed for high-concurrency messaging and real-time communication.

```mermaid
graph LR
    %% Theme Configuration for Dark Mode
    classDef client fill:#3a3a3a,stroke:#00ff88,stroke-width:2px,color:#fff;
    classDef gateway fill:#2d3436,stroke:#0984e3,stroke-width:2px,color:#fff;
    classDef logic fill:#2d3436,stroke:#6c5ce7,stroke-width:2px,color:#fff;
    classDef storage fill:#2d3436,stroke:#d63031,stroke-width:2px,color:#fff;
    classDef queue fill:#2d3436,stroke:#fdcb6e,stroke-width:2px,color:#fff;

    subgraph Clients ["Client Layer"]
        SDK[OpenIM SDK / Web / Mobile]:::client
    end

    subgraph Entry ["Entry Layer"]
        API[OpenIM API Gateway]:::gateway
        GW[Message Gateway - WebSocket]:::gateway
    end

    subgraph Discovery ["Discovery & Registry"]
        ZkEtcd[Zookeeper / Etcd]:::logic
    end

    subgraph Logic ["Logic Layer (RPC)"]
        RPC["RPC Services<br/>(User, Msg, Group, etc)"]:::logic
    end

    subgraph Async ["Message Processing"]
        Kafka[Kafka Cluster]:::queue
        Transfer[Msg Transfer]:::queue
    end

    subgraph Storage ["Storage Layer"]
        Redis[(Redis Cache)]:::storage
        Mongo[(MongoDB)]:::storage
        Minio[(Minio OSS)]:::storage
    end

    %% Connections
    SDK -- HTTP/REST --> API
    SDK -- WebSocket --> GW
    
    API -- RPC --> Logic
    GW -- RPC --> Logic
    
    Logic -- Registry --> ZkEtcd
    Logic -- Auth/Data --> Redis
    Logic -- Persistence --> Mongo
    Logic -- Files --> Minio
    
    Logic -- Push --> Kafka
    Kafka -- Consume --> Transfer
    Transfer -- Store --> Mongo
    Transfer -- Sync --> GW
    
    %% Style adjustments
    linkStyle default stroke:#888,stroke-width:1px;
```

## Architectural Components

### 1. Client Layer
- **OpenIM SDK:** Decouples complex messaging logic from the UI, handling local storage, sequence synchronization, and connection management.

### 2. Entry Layer
- **API Gateway:** Handles RESTful requests, user authentication, and administrative operations.
- **Message Gateway:** Maintains persistent WebSocket connections for real-time message delivery.

### 3. Logic Layer (RPC Services)
- Microservices architecture partitioned by domain (User, Group, Msg, etc.).
- Horizontally scalable and discovered via Zookeeper or Etcd.

### 4. Storage & Processing
- **Kafka:** Decouples message sending from persistence, ensuring high throughput and reliability.
- **Redis:** Used for hot data caching (online status, tokens) and sequence IDs.
- **MongoDB:** Primary store for message history and metadata.
- **Minio:** Handles object storage for images, videos, and voice messages.
