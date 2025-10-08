#!/bin/bash
set -e

MAX_RETRIES=10
COUNT=0

echo "Waiting for Kafka brokers to be ready..."
until /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092,kafka2:9094,kafka3:9096 --list; do
  COUNT=$((COUNT + 1))
  if [ $COUNT -ge $MAX_RETRIES ]; then
    echo "Kafka brokers not ready after $MAX_RETRIES tries, exiting."
    exit 1
  fi
  echo "Kafka brokers not ready, retrying in 5 seconds..."
  sleep 5
done

echo "Creating weather-topic if not exists..."
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092,kafka2:9094,kafka3:9096 \
--create \
--if-not-exists \
--topic stock-prices-protobuf \
--partitions 6 \
--replication-factor 3 \
--config min.insync.replicas=2

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092,kafka2:9094,kafka3:9096 \
--create \
--if-not-exists \
--topic user-events-protobuf \
--partitions 6 \
--replication-factor 3 \
--config min.insync.replicas=2

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092,kafka2:9094,kafka3:9096 \
--create \
--if-not-exists \
--topic iot-sensors-avro \
--partitions 6 \
--replication-factor 3 \
--config min.insync.replicas=2

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092,kafka2:9094,kafka3:9096 \
  --create \
  --if-not-exists \
  --topic weather-data-avro \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092,kafka2:9094,kafka3:9096 \
  --create \
  --if-not-exists \
  --topic stock-moving-average-alerts \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092,kafka2:9094,kafka3:9096 \
  --create \
  --if-not-exists \
  --topic volume-surge-alerts \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092,kafka2:9094,kafka3:9096 \
  --create \
  --if-not-exists \
  --topic user-journey-analytics \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092,kafka2:9094,kafka3:9096 \
  --create \
  --if-not-exists \
  --topic user-segmentation \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092,kafka2:9094,kafka3:9096 \
  --create \
  --if-not-exists \
  --topic conversions \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092,kafka2:9094,kafka3:9096 \
  --create \
  --if-not-exists \
  --topic sensor-maintenance-alerts \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092,kafka2:9094,kafka3:9096 \
  --create \
  --if-not-exists \
  --topic sensor-weather-correlations \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092,kafka2:9094,kafka3:9096 \
  --create \
  --if-not-exists \
  --topic stock-interest-correlations \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092,kafka2:9094,kafka3:9096 \
  --create \
  --if-not-exists \
  --topic market-overview-feed \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2

echo "Topic creation completed."