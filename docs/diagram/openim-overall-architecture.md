mermaid```
flowchart TB
  %% =========================
  %% Clients / Edge
  %% =========================
  subgraph Clients
    Web["Web / Admin UI"]
    Mobile["Mobile Apps (iOS/Android)"]
    SDK["OpenIM SDK Clients"]
  end

  subgraph Edge["Edge / Entry"]
    API["openim-api\n(HTTP/REST + OpenAPI 3.0)"]
    GW["openim-msggateway\n(WebSocket/LongConn Gateway)"]
  end

  %% =========================
  %% Core RPC microservices
  %% =========================
  subgraph RPC["Core RPC services (gRPC)"]
    RPCAuth["openim-rpc-auth\n(auth/token)"]
    RPCUser["openim-rpc-user\n(user/profile)"]
    RPCRelation["openim-rpc-friend / relation\n(friendship/blacklist)"]
    RPCGroup["openim-rpc-group\n(groups)"]
    RPCConv["openim-rpc-conversation\n(conversation state)"]
    RPCMsg["openim-rpc-msg\n(message persistence/seq)"]
    RPCThird["openim-rpc-third\n(3rd-party + object storage ops)"]
  end

  %% =========================
  %% Async / background
  %% =========================
  subgraph Async["Async + background services"]
    Transfer["openim-msgtransfer\n(Kafka consumers/producers)"]
    Push["openim-push\n(push notifications)"]
    Cron["openim-crontask\n(scheduled tasks)"]
  end

  %% =========================
  %% Infra dependencies
  %% =========================
  subgraph Infra["Infrastructure"]
    Kafka["Kafka\nTopics: latestMsgToRedis, msgToPush, offlineMsgToMongoMysql"]
    Redis["Redis\n(cache, seq/online state, etc.)"]
    Mongo["MongoDB\n(message/offline/conversation data)"]
    MinIO["MinIO / Object Storage\n(files/media)"]
    Disc["Service Discovery\n(etcd/standalone discovery)"]
  end

  %% =========================
  %% Client -> Edge
  %% =========================
  Web --> API
  Mobile --> API
  SDK --> API
  SDK --> GW

  %% =========================
  %% Edge -> RPC
  %% =========================
  API --> RPCAuth
  API --> RPCUser
  API --> RPCRelation
  API --> RPCGroup
  API --> RPCConv
  API --> RPCMsg
  API --> RPCThird

  GW --> RPCAuth
  GW --> RPCMsg
  GW --> RPCConv
  GW --> RPCRelation
  GW --> RPCGroup
  GW --> RPCUser

  %% =========================
  %% RPC -> Infra
  %% =========================
  RPCAuth --> Redis
  RPCUser --> Mongo
  RPCRelation --> Mongo
  RPCGroup --> Mongo
  RPCConv --> Mongo
  RPCMsg --> Mongo
  RPCMsg --> Redis
  RPCThird --> MinIO

  %% =========================
  %% Messaging pipeline (async)
  %% =========================
  GW --> Kafka
  API --> Kafka

  Kafka --> Transfer
  Transfer --> Redis
  Transfer --> Mongo

  Kafka --> Push
  Push --> Kafka
  Push --> Redis

  %% =========================
  %% Service discovery (simplified)
  %% =========================
  API --- Disc
  GW --- Disc
  RPCAuth --- Disc
  RPCUser --- Disc
  RPCRelation --- Disc
  RPCGroup --- Disc
  RPCConv --- Disc
  RPCMsg --- Disc
  RPCThird --- Disc
  Transfer --- Disc
  Push --- Disc
  Cron --- Disc
```