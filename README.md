# Kafka HTTP Gateway

Lightweight HTTP gateway exposing Kafka Producer, Consumer, and AdminClient APIs over HTTP. Built on pure Netty — no
Spring, no heavy frameworks.

## Features

- **Producer** — create, send (JSON & binary), partitions, transactions, lifecycle
- **Consumer** — create, poll, commit, seek, seek-to-beginning, seek-to-end, subscribe, assign, position, offsets,
  partitions, topics, lifecycle
- **Admin** — topics, configs, ACLs, consumer groups, offsets, SCRAM credentials, cluster info
- **Health check** — `GET /health` for liveness probes (no auth required)
- **Version** — `GET /version`
- **Binary protocol** — high-performance binary format for producer send and consumer poll
- **Optional Basic Auth** — HTTP Basic Authentication via `users.json`

## Requirements

- **Java 21** (required)
- Kafka broker accessible from the gateway host

## Build & Run

```bash
# Build
./gradlew build

# Run (default: 0.0.0.0:8086)
./gradlew run

# Create distribution
./gradlew installDist

# Run distribution
./build/install/kafka-gateway/bin/kafka-gateway

# Run distribution with custom host and port
KAFKA_GATEWAY_OPTS="-Dhost=127.0.0.1 -Dport=9090" ./build/install/kafka-gateway/bin/kafka-gateway

# Run tests
./gradlew test
```

## Configuration

| System Property                  | Default    | Description                                                   |
|----------------------------------|------------|---------------------------------------------------------------|
| `-Dhost`                         | `0.0.0.0`  | Bind address                                                  |
| `-Dport`                         | `8086`     | Listen port                                                   |
| `-Dnetty.workerThreads`          | `0`        | Worker threads; `0` uses the Netty default (2 x CPU cores)    |
| `-Dnetty.backlog`                | `1024`     | Maximum pending TCP connections                               |
| `-Dnetty.readTimeoutSeconds`     | `300`      | HTTP connection read timeout                                  |
| `-Dnetty.writeTimeoutSeconds`    | `300`      | HTTP connection write timeout                                 |
| `-Dnetty.maxRequestBytes`        | `33554432` | Maximum request body size                                     |
| `-Dnetty.maxJsonRequestBytes`    | `4194304`  | Maximum JSON request body size                                |
| `-Dnetty.epoll`                  | `true`     | Use native epoll on Linux when available                      |
| `-Dclient.close.parallelism`     | `32`       | Maximum concurrent close operations per client type           |

## Deployment

### systemd

A systemd unit file is provided in `kafka-gateway.service`:

```bash
sudo useradd -r -s /usr/sbin/nologin kafka-gateway
sudo mkdir -p /var/log/kafka-gateway
sudo chown kafka-gateway:kafka-gateway /var/log/kafka-gateway
sudo systemctl link /opt/kafka-gateway/kafka-gateway.service
sudo systemctl enable --now kafka-gateway
```

The service runs as user `kafka-gateway` from `/opt/kafka-gateway/` with:

- Default bind address and port configured via `KAFKA_GATEWAY_OPTS` (`-Dhost=127.0.0.1 -Dport=8086`)
- `LimitNOFILE=65536`
- `TimeoutStopSec=120` (graceful shutdown)

### TLS

TLS is **not** implemented in the application. Use an NGINX reverse proxy for TLS termination (LAN-only deployment).

### Authentication

Optional HTTP Basic Authentication via `users.json` in the working directory:

```json
[
  {
    "username": "admin",
    "password": "secret"
  }
]
```

If `users.json` is missing, authentication is disabled and a warning is logged at startup.

## API Overview

All endpoints accept JSON by default. Producer send and consumer poll also support a binary protocol for high-throughput
scenarios.

The API is documented as an OpenAPI 3.1 specification in [`docs/api/openapi.yaml`](docs/api/openapi.yaml)
(see [`docs/api/README.md`](docs/api/README.md) for Swagger UI setup).

### Producer

| Method | Endpoint                       | Description               |
|--------|--------------------------------|---------------------------|
| POST   | `/producer/create`             | Create producer instance  |
| POST   | `/producer/send`               | Send record (JSON/binary) |
| POST   | `/producer/get-partitions`     | Get topic partitions      |
| POST   | `/producer/begin-transaction`  | Begin transaction         |
| POST   | `/producer/commit-transaction` | Commit transaction        |
| POST   | `/producer/abort-transaction`  | Abort transaction         |
| POST   | `/producer/touch`              | Reset expiration timer    |
| POST   | `/producer/release`            | Destroy producer instance |
| GET    | `/producer`                    | List all producers        |

### Consumer

| Method | Endpoint                          | Description                 |
|--------|-----------------------------------|-----------------------------|
| POST   | `/consumer/create`                | Create consumer instance    |
| POST   | `/consumer/poll`                  | Poll records (JSON/binary)  |
| POST   | `/consumer/commit`                | Commit offsets              |
| POST   | `/consumer/seek`                  | Seek to specific offset     |
| POST   | `/consumer/seek-to-beginning`     | Seek to partition beginning |
| POST   | `/consumer/seek-to-end`           | Seek to partition end       |
| POST   | `/consumer/get-position`          | Get current position        |
| POST   | `/consumer/assign`                | Manual partition assignment |
| POST   | `/consumer/get-assignment`        | Get current assignment      |
| POST   | `/consumer/subscribe`             | Subscribe to topics/pattern |
| POST   | `/consumer/unsubscribe`           | Unsubscribe from all topics |
| POST   | `/consumer/get-subscription`      | Get current subscription    |
| POST   | `/consumer/get-partitions`        | Get partitions for a topic  |
| POST   | ~~`/consumer/list-partitions`~~   | ~~List partitions~~ *(deprecated)* |
| POST   | `/consumer/list-topics`           | List topics                 |
| POST   | `/consumer/get-group-metadata`    | Get group metadata          |
| POST   | `/consumer/get-committed`         | Get committed offsets       |
| POST   | `/consumer/get-beginning-offsets` | Get beginning offsets       |
| POST   | `/consumer/get-end-offsets`       | Get end offsets             |
| POST   | `/consumer/touch`                 | Reset expiration timer      |
| POST   | `/consumer/release`               | Destroy consumer instance   |
| GET    | `/consumer`                       | List all consumers          |

### Admin

**Management**

| Method | Endpoint         | Description              |
|--------|------------------|--------------------------|
| POST   | `/admin/create`  | Create admin instance    |
| POST   | `/admin/release` | Destroy admin instance   |
| POST   | `/admin/touch`   | Reset expiration timer   |
| GET    | `/admin`         | List all admin instances |

**Cluster**

| Method | Endpoint                   | Description       |
|--------|----------------------------|-------------------|
| POST   | `/admin/describe-cluster`  | Describe cluster  |
| POST   | `/admin/describe-log-dirs` | Describe log dirs |

**Topics**

| Method | Endpoint                   | Description            |
|--------|----------------------------|------------------------|
| POST   | `/admin/list-topics`       | List topics            |
| POST   | `/admin/describe-topic`    | Describe topic         |
| POST   | `/admin/create-topic`      | Create topic           |
| POST   | `/admin/create-topics`     | Create multiple topics |
| POST   | `/admin/delete-topic`      | Delete topic           |
| POST   | `/admin/delete-topics`     | Delete multiple topics |
| POST   | `/admin/create-partitions` | Create partitions      |

**Configs**

| Method | Endpoint                         | Description             |
|--------|----------------------------------|-------------------------|
| POST   | `/admin/describe-topic-configs`  | Describe topic configs  |
| POST   | `/admin/describe-broker-configs` | Describe broker configs |
| POST   | `/admin/set-topic-config`        | Set topic config        |
| POST   | `/admin/delete-topic-config`     | Delete topic config     |

**SCRAM**

| Method | Endpoint                                 | Description                |
|--------|------------------------------------------|----------------------------|
| POST   | `/admin/describe-user-scram-credentials` | Describe SCRAM credentials |
| POST   | `/admin/upsert-user-scram-credentials`   | Create/update SCRAM cred   |
| POST   | `/admin/delete-user-scram-credentials`   | Delete SCRAM credential    |

**ACLs**

| Method | Endpoint               | Description   |
|--------|------------------------|---------------|
| POST   | `/admin/describe-acls` | Describe ACLs |
| POST   | `/admin/create-acls`   | Create ACLs   |
| POST   | `/admin/delete-acls`   | Delete ACLs   |

**Producers**

| Method | Endpoint                    | Description        |
|--------|-----------------------------|--------------------|
| POST   | `/admin/describe-producers` | Describe producers |
| POST   | `/admin/abort-transaction`  | Abort transaction  |

**Groups**

| Method | Endpoint                                    | Description                        |
|--------|---------------------------------------------|------------------------------------|
| POST   | `/admin/list-groups`                        | List groups                        |
| POST   | `/admin/describe-classic-group`             | Describe classic group             |
| POST   | `/admin/describe-consumer-group`            | Describe consumer group            |
| POST   | `/admin/describe-share-group`               | Describe share group               |
| POST   | `/admin/describe-streams-group`             | Describe streams group             |
| POST   | `/admin/list-consumer-group-offsets`        | List consumer group offsets        |
| POST   | `/admin/alter-consumer-group-offsets`       | Alter consumer group offsets       |
| POST   | `/admin/delete-consumer-group-offsets`      | Delete consumer group offsets      |
| POST   | `/admin/remove-members-from-consumer-group` | Remove members from consumer group |
| POST   | `/admin/delete-consumer-group`              | Delete consumer group              |
| POST   | `/admin/delete-consumer-groups`             | Delete multiple consumer groups    |
| POST   | `/admin/delete-share-group`                 | Delete share group                 |
| POST   | `/admin/delete-share-groups`                | Delete multiple share groups       |
| POST   | `/admin/delete-streams-group`               | Delete streams group               |
| POST   | `/admin/delete-streams-groups`              | Delete multiple streams groups     |

**Offsets**

| Method | Endpoint                             | Description                 |
|--------|--------------------------------------|-----------------------------|
| POST   | `/admin/list-earliest-offsets`       | List earliest offsets       |
| POST   | `/admin/list-earliest-local-offsets` | List earliest local offsets |
| POST   | `/admin/list-latest-offsets`         | List latest offsets         |
| POST   | `/admin/list-latest-tiered-offsets`  | List latest tiered offsets  |
| POST   | `/admin/list-max-timestamp-offsets`  | List max timestamp offsets  |
| POST   | `/admin/list-timestamp-offsets`      | List offsets by timestamp   |

### System

| Method | Endpoint   | Description              |
|--------|------------|--------------------------|
| GET    | `/health`  | Liveness check (no auth) |
| GET    | `/version` | Application version      |

## Instance Lifecycle

Each `create` request returns a UUID **token** — proof of ownership. All subsequent operations on that instance require
the token.

Instances auto-expire after `expirationTimeout` (configurable per instance, 1 second to 24 hours). The expiration timer
is reset on every request to the instance (poll, send, seek, describe, etc.). Use `/touch` when you
need to keep an instance alive without performing any operation.

## Architecture

```
HTTP Request
  → HttpServerCodec → HttpVersionHandler → ReadTimeoutHandler(300s) → WriteTimeoutHandler(300s)
  → JsonRequestSizeLimitHandler(4MB JSON)
  → HttpObjectAggregator(32MB)
  → HttpRequestFlowControlHandler (one active request per connection)
  → HealthRequestDecoder
  → VersionRequestDecoder
  → BasicAuthenticationHandler (optional)
  → ProducerRequestDecoder → ConsumerRequestDecoder → AdminRequestDecoder
  → DefaultRequestDecoder (404 fallback)
  → ProducerResponseEncoder → ConsumerResponseEncoder → AdminResponseEncoder
  → ProducerRequestProcessor → ConsumerRequestProcessor → AdminRequestProcessor
  → DefaultInboundHandler
```

- **Producer/Consumer** blocking Kafka calls are offloaded to virtual threads via `BlockingTaskExecutor`
- **Producer send** uses Kafka callback (non-blocking)
- **Admin** uses `KafkaFuture.whenComplete()` callbacks (non-blocking)

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
