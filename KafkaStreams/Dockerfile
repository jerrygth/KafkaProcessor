# Stage 1: Build the KafkaStreams module
FROM gradle:8.10.2-jdk17 AS builder
WORKDIR /build
COPY build.gradle .
COPY settings.gradle .
COPY KafkaStreams/build.gradle KafkaStreams/
COPY KafkaStreams/src KafkaStreams/src
RUN gradle :KafkaStreams:clean :KafkaStreams:build -x test

# Stage 2: Create the runtime image
FROM eclipse-temurin:17-jre
# Create the state directory for Kafka Streams with full permissions
RUN mkdir -p /data/kafka-streams && chmod -R 777 /data/kafka-streams
WORKDIR /app
COPY --from=builder /build/KafkaStreams/build/libs/KafkaStreams-0.0.1-SNAPSHOT.jar app.jar

# Create non-root user for security
RUN adduser --disabled-password springuser
USER springuser

# Expose Spring Boot port
EXPOSE 8084

# Run the application
ENTRYPOINT ["java", "-jar", "app.jar"]