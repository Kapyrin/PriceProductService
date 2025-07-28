# Price Product Service

## Description
This application is a service for processing product price updates. It receives JSON requests with price data, validates them, stores individual prices in PostgreSQL, calculates and caches average prices in Redis, and uses RabbitMQ for asynchronous message processing. The application supports high load (10,000–100,000 JSON requests) with dynamic scaling of RabbitMQ consumers based on queue size metrics. Prometheus metrics are exposed for monitoring.

## Architecture
The application is built using Java 21, Vert.x for HTTP API, RabbitMQ for messaging, PostgreSQL for persistent storage, and Redis for caching. Key classes:
- **HTTP API**: `PriceApiVerticle.java` - Handles endpoints `/price-updates`, `/average-price/:productId`, `/metrics`, `/health`.
- **Message Processing**: `PriceUpdateConsumerTask.java` - Consumes messages from RabbitMQ, validates, and stores data.
- **Calculation and Caching**: `PriceAverageCalculatorImpl.java` - Calculates and caches average prices in Redis.
- **Configuration**: `RedisConfig.java` - Configures Redis pool and cache settings.
- **Utilities**: `ApplicationShutdownHandler.java` - Graceful shutdown.
  - `ConsumerScaler.java` - Dynamic scaling of RabbitMQ consumers.
  - `DlqProcessor.java` - Processes dead-letter queue.
  - `ConnectionMonitor.java` - Monitors connections to RabbitMQ and Redis.
  - `CumerInitializer.java` - Initializes RabbitMQ consumers.
- **Entry Point**: `Main.java` - Initializes and starts the application.
- **Repository**: `PriceRepositoryImpl.java` - Database operations.
- **Validation**: `PriceUpdateValidatorImpl.java` - Validates price updates.
- **Publishing**: `RawPriceUpdatePublisherImpl.java` - Publishes messages to RabbitMQ.

## Setup
### Requirements
- Java 21
- Gradle
- Docker (for containerized deployment)
- PostgreSQL, RabbitMQ, Redis (run locally or via Docker)

### Local Setup
1. Configure `application.properties` with local settings (e.g., `db.url=jdbc:postgresql://localhost:5433/price_service`).
2. Run PostgreSQL, RabbitMQ, and Redis locally.
3. Open the project in IntelliJ IDEA and run `Main.java`.

## Running Locally
1. Build the application:
   ```bash
   ./gradlew build
   ```
2. Run the application:
   ```bash
   java -jar build/libs/app.jar
   ```
   - Or run `Main.java` directly in IDEA.

## Building and Running in Docker
1. Build the application JAR:
   ```bash
   ./gradlew build
   ```
2. Build the Docker image:
   ```bash
   docker build -t price-service-app .
   ```
3. Update `.env` with Docker-specific settings (e.g., `DB_URL=jdbc:postgresql://db:5432/price_db`).
4. Start the containers:
   ```bash
   docker-compose up -d
   ```
5. Stop the containers:
   ```bash
   docker-compose down
   ```

## Monitoring
- Metrics are exposed at `http://localhost:8080/metrics` (Prometheus format).
- Use `curl http://localhost:8080/metrics` to view metrics like `rabbitmq_queue_size`, `http_post_price_updates_requests_total`.
- For advanced monitoring, add Prometheus and Grafana to `docker-compose.yml` (optional).

## Configuration
- Local: `application.properties`.
- Docker: `.env` overrides settings.
- Ignore `.env` in `.gitignore` for security.

## Notes
- Ensure Redis is configured with `maxmemory` and `maxmemory-policy allkeys-lru` for cache management.
- RabbitMQ DLQ has TTL of 12 hours for invalid messages.
- Dynamic scaling of RabbitMQ consumers based on queue size (min 2, max 10).