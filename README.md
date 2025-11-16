Kafka Processor Project:
This is a comprehensive Kafka-based project featuring producers, consumers, and streams processing. It demonstrates real-time data ingestion, processing, and analytics using different serialization formats (Protobuf and Avro) with Schema Registry integration. The project includes a multi-node Kafka cluster, monitoring with Prometheus and Grafana, and is designed for deployment via Docker Compose.

Overview
The project simulates various data streams:

Stock prices and user events (using Protobuf serialization).
IoT sensor data and weather data (using Avro serialization).

Key features:

Multi-node Kafka cluster (3 brokers) for high availability.
Schema Registry for managing schemas.
Producers generating simulated data at configurable rates.
Consumers processing and logging messages with error handling and metrics.
Streams application for advanced analytics, including moving averages, anomaly detection, joins, and aggregations.
Monitoring via Prometheus (metrics scraping) and Grafana (dashboards).

Modules
Kafka Producer

Located in KafkaProducer directory.
Generates simulated data for stocks, user events, sensors, and weather.
Uses Protobuf for stocks/users and Avro for sensors/weather.
Configurable rates (e.g., 1000ms intervals) via application.yml.
Metrics: Message count, production time.
Exposed on port 8083.

Kafka Consumer:

Located in KafkaConsumer directory.
Consumes from producer topics with dedicated consumer groups.
Handles deserialization, processing (e.g., anomaly checks), and logging.
Metrics: Messages consumed, processing time, consumer lag.
Exposed on port 8082.

Kafka Streams:

Located in KafkaStreams directory.
Processes streams for analytics:

Stock: Moving averages, volume surges.
Users: Journey tracking, segmentation, conversions.
Sensors/Weather: Maintenance alerts, correlations.
Cross-stream joins (e.g., stock-user interest).


Uses state stores for aggregations.
Metrics: Processing time, errors.
Exposed on port 8084; state dir at /data/kafka-streams.

Monitoring:

Prometheus: Scrapes JMX metrics from Kafka brokers and Micrometer metrics from Spring apps. Access at http://localhost:9090.
Grafana: Pre-configured dashboards for Kafka health, throughput, latency, and app-specific metrics. Access at http://localhost:3000 (default credentials: admin/admin).

Setup and Running:

1. Clone the repository
   git clone <your-github-repo-url>
   cd <repo-name>
2. Build and start the services using Docker Compose:
   docker-compose -f docker-compose.multinode.yml up -d --build
   This starts the Kafka cluster, Schema Registry, Producer, Consumer, Streams, Prometheus, and Grafana.
   Wait ~1-2 minutes for health checks to pass and topics to be created (via topic-creator service).

3. Verify services:

Kafka: Use docker exec -it kafka1 kafka-topics.sh --bootstrap-server localhost:9092 --list.
Schema Registry: curl http://localhost:8081/subjects.
Apps: Check logs with docker logs kafka-producer, etc.
Prometheus: Browse targets at http://localhost:9090/targets.
Grafana: Add Prometheus as datasource and import Kafka dashboards.

4. Stop the application:

docker-compose -f docker-compose.multinode.yml down -v