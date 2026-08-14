# Kafka HTTP Gateway

Lightweight HTTP gateway exposing Kafka Producer, Consumer, and AdminClient APIs over HTTP. Built on pure Netty — no
Spring, no heavy frameworks.

## Features

- **Producer** — create, send (JSON & binary), partitions, transactions, lifecycle
- **Consumer** — create, poll, commit, seek, seek-to-beginning, seek-to-end, subscribe, assign, position, offsets,
  partitions, topics, lifecycle
- **Admin** — topics, configs, ACLs, consumer groups, offsets, SCRAM credentials, cluster info
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

| System Property                  | Default    | Description                                                          |
|----------------------------------|------------|----------------------------------------------------------------------|
| `-Dhost`                         | `0.0.0.0`  | Bind address                                                         |
| `-Dport`                         | `8086`     | Listen port                                                          |
| `-Dnetty.workerThreads`          | `0`        | Netty event loop threads; `0` uses the Netty default (2 x CPU cores) |
| `-Dnetty.backlog`                | `1024`     | Maximum pending TCP connections                                      |
| `-Dnetty.readTimeoutSeconds`     | `300`      | HTTP connection read timeout                                         |
| `-Dnetty.writeTimeoutSeconds`    | `300`      | HTTP connection write timeout                                        |
| `-Dnetty.maxRequestBytes`        | `33554432` | Maximum request body size                                            |
| `-Dnetty.maxJsonRequestBytes`    | `4194304`  | Maximum JSON request body size                                       |
| `-Dnetty.responseChunkBytes`     | `65536`    | Chunk size for streamed consumer poll responses                      |
| `-Dshutdown.timeoutSeconds`      | `60`       | Common deadline for graceful shutdown                                |
| `-Dnetty.epoll`                  | `true`     | Use native epoll on Linux when available                             |
| `-Dclient.close.parallelism`     | `32`       | Maximum concurrent close operations per client type                  |
| `-Dlog.dir`                      | `(none)`   | Log file directory (console-only when unset); files roll daily and are deleted after 30 days |

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

- Default bind address, port, and log directory configured via `KAFKA_GATEWAY_OPTS`
  (`-Dhost=127.0.0.1 -Dport=8086 -Dlog.dir=/var/log/kafka-gateway`)
- Application log files roll daily in `/var/log/kafka-gateway`; console output goes to journald
- `LimitNOFILE=65536`
- `TimeoutStopSec=120` (greater than the default 60-second application shutdown deadline)

### Windows Service (Apache Procrun)

1. Install `prunsrv.exe` from the [Apache Commons Daemon binaries](https://downloads.apache.org/commons/daemon/binaries/windows/).
2. Extract the distribution archive into the installation folder (`C:\kafka-gateway` in the example below).
3. Navigate to the installation folder.

Run the provided script from an elevated prompt:

```powershell
powershell -ExecutionPolicy Bypass -File .\install-windows-service.ps1 -InstallDir C:\kafka-gateway
```

Parameters:

| Parameter           | Default                        | Description                                                                                    |
|---------------------|--------------------------------|------------------------------------------------------------------------------------------------|
| `-InstallDir`       | (required)                     | The distribution directory containing `lib`                                                     |
| `-WorkDir`          | `-InstallDir`                  | Service working directory                                                                       |
| `-LogDir`           | `<WorkDir>\logs`               | Log directory (Procrun service and application log files)                                       |
| `-ServiceName`      | `kafka-gateway`                | Windows service name                                                                            |
| `-Prunsrv`          | `prunsrv.exe`                  | Procrun executable (`<InstallDir>`, then PATH)                                                  |
| `-JvmOpts`          | `-Xms256M;-Xmx2G`              | JVM options (like `JAVA_OPTS` in the systemd unit)                                              |
| `-KafkaGatewayOpts` | `-Dhost=127.0.0.1;-Dport=8086` | Gateway options (like `KAFKA_GATEWAY_OPTS` in the systemd unit)                                 |
| `-StopTimeout`      | `120`                          | Seconds to wait for shutdown; must exceed the `-Dshutdown.timeoutSeconds` deadline (default 60) |

Manage the service:

```powershell
prunsrv //ES//kafka-gateway   # start
prunsrv //SS//kafka-gateway   # stop
prunsrv //DS//kafka-gateway   # uninstall
```

Manual configuration (GUI): the Commons Daemon binaries also include `prunmgr.exe`:

```powershell
prunmgr.exe //ES//kafka-gateway
```

The dialog edits the service parameters (startup, JVM, paths, logging) and can start/stop the service. Other modes: `//MS//` (tray monitor), `//MR//` (monitor and start the service if it is not running), `//MQ//` (quit all monitors). Changes are stored in the registry under `HKLM\SOFTWARE\WOW6432Node\Apache Software Foundation\Procrun 2.0\kafka-gateway`.

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
| POST   | `/admin/describe-features` | Describe features |
| POST   | `/admin/describe-log-dirs` | Describe log dirs |
| POST   | `/admin/update-feature`    | Update feature    |

**Topics**

| Method | Endpoint                   | Description            |
|--------|----------------------------|------------------------|
| POST   | `/admin/list-topics`       | List topics            |
| POST   | `/admin/describe-topic`    | Describe topic         |
| POST   | `/admin/create-topic`      | Create topic           |
| POST   | `/admin/create-topics`     | Create multiple topics |
| POST   | `/admin/delete-topic`      | Delete topic           |
| POST   | `/admin/delete-topics`     | Delete multiple topics |
| POST   | `/admin/delete-records`    | Delete records         |
| POST   | `/admin/create-partitions` | Create partitions      |

**Configs**

| Method | Endpoint                         | Description             |
|--------|----------------------------------|-------------------------|
| POST   | `/admin/describe-topic-configs`  | Describe topic configs  |
| POST   | `/admin/describe-broker-configs` | Describe broker configs |
| POST   | `/admin/describe-group-configs`  | Describe group configs  |
| POST   | `/admin/alter-group-config`      | Alter group config      |
| POST   | `/admin/alter-topic-config`      | Alter topic config      |
| POST   | `/admin/delete-group-config`     | Delete group config     |
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

Each `create` request returns a string **token** — proof of ownership. All subsequent operations on that instance require
the token.

Instances auto-expire after `expirationTimeout` (configurable per instance, 1 second to 24 hours). The expiration timer
is reset on every request to the instance (poll, send, seek, describe, etc.). Use `/touch` when you
need to keep an instance alive without performing any operation.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
