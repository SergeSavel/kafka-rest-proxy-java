# Kafka HTTP Gateway

Lightweight Kafka HTTP Gateway built on Netty — exposes Kafka Producer, Consumer, and AdminClient APIs over HTTP without Spring or heavy frameworks.

## Architecture

```
HTTP Request
  → HttpServerCodec → HttpVersionHandler → ReadTimeoutHandler(300s) → WriteTimeoutHandler(300s)
  → HttpObjectAggregator(32MB)
  → HealthRequestDecoder
  → VersionRequestDecoder
  → BasicAuthenticationHandler (optional, users.json)
  → ProducerRequestDecoder → ConsumerRequestDecoder → AdminRequestDecoder
  → DefaultRequestDecoder (404 fallback)
  → ProducerResponseEncoder → ConsumerResponseEncoder → AdminResponseEncoder
  → ProducerRequestProcessor → ConsumerRequestProcessor → AdminRequestProcessor
  → DefaultInboundHandler
```

**Entry point:** `pro.savel.kafka.Application` — Netty NIO server, default `0.0.0.0:8086` (`-Dhost=`, `-Dport=`).

### Modules

| Package    | Purpose                                                                                                                |
|------------|------------------------------------------------------------------------------------------------------------------------|
| `producer` | Producer lifecycle, send, partitions                                                                                   |
| `consumer` | Consumer lifecycle, poll, commit, seek, seek-to-beginning, seek-to-end, subscribe, assign, position, offsets, topics   |
| `admin`    | Topics, configs, ACLs, groups, offsets, SCRAM, cluster                                                                 |
| `common`   | Shared contracts, exceptions, HTTP utils, client lifecycle (`ClientProvider`, `ClientWrapper`, `BlockingTaskExecutor`) |

### Key patterns

- **Decoders** parse JSON/binary requests into typed Request DTOs, pass via `RequestBearer`
- **Processors** handle business logic; producer/consumer use `BlockingTaskExecutor` (virtual threads) for blocking Kafka calls; producer `send()` uses Kafka callback (non-blocking); admin uses `KafkaFuture.whenComplete()` callbacks (non-blocking)
- **Encoders** serialize Response DTOs to JSON/binary HTTP responses
- **`ClientProvider<T>`** manages instance lifecycle: creation, expiration (1s scheduled timer), removal. Shared by producer, consumer, admin
- **`ClientWrapper`** wraps a Kafka client instance with id, token, owner, expiration timestamp
- **`HttpStatusException`** — base class for gateway-specific exceptions; each subclass defines its own HTTP status code, handled uniformly in `CommonErrors`

## Build & Run

```bash
# Build
./gradlew build

# Run (default port 8086)
./gradlew run

# Run with custom port
./gradlew run -Dport=9090

# Create distribution
./gradlew installDist

# Run tests
./gradlew test
```

**Java 21 required.** Gradle wrapper included.

## Deployment

- **systemd:** `kafka-gateway.service` — runs as `kafka-gateway` user, installed to `/opt/kafka-gateway/`,
  `WorkingDirectory=/opt/kafka-gateway`, `LimitNOFILE=65536`, `TimeoutStopSec=120`
- **TLS:** NOT implemented in the app — handled by NGINX reverse proxy (LAN-only deployment)
- **Auth:** Optional HTTP Basic Auth via `users.json` in working directory; if file is missing, auth is disabled

## Architectural Decisions

These are intentional design choices — do not flag as issues:

- **TLS via NGINX** — app is LAN-only, TLS termination is at the reverse proxy
- **Arbitrary Kafka Properties** — create requests accept any `Properties` map; clients must be able to configure Kafka instances freely. Security boundary is at the network/broker level
- **Token-based ownership** — UUID token returned on create = proof of ownership. Only the creator knows the token. No RBAC or role separation at the gateway level
- **Basic Auth is optional** — rarely used; gateway is a transparent transport layer. Client rights are determined by Kafka broker ACLs/SASL per instance
- **Sequential access guaranteed externally** — each KafkaConsumer instance must be called strictly sequentially by
  clients; server does not enforce per-instance synchronization. KafkaAdmin and KafkaProducer are thread-safe and may be
  called concurrently

## Code Conventions

- **Style:** Braces on new lines (Allman style), 4-space indent, no tabs
- **Lombok:** `@Data`, `@Getter`, `@Builder` on DTOs and request/response classes
- **Jackson:** `ObjectMapper` with `FAIL_ON_UNKNOWN_PROPERTIES=true`, `FAIL_ON_NULL_FOR_PRIMITIVES=true`
- **Validation:** Jakarta Validation (`@NotEmpty`, `@NotNull`, `@Positive`) enforced via Hibernate Validator
- **Logging:** SLF4J + Log4j2; root level WARN, `pro.savel` at INFO
- **License:** Apache 2.0 header on all source files
- **Tests:** JUnit 5 (declared, no tests written yet)
