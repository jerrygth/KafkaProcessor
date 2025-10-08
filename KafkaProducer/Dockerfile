# Stage 1: Build the KafkaProducer module
FROM gradle:8.10.2-jdk17 AS builder
WORKDIR /build
COPY build.gradle .
COPY settings.gradle .
COPY KafkaProducer/build.gradle KafkaProducer/
COPY KafkaProducer/src KafkaProducer/src
RUN gradle :KafkaProducer:clean :KafkaProducer:build -x test

# Stage 2: Create the runtime image
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=builder /build/KafkaProducer/build/libs/KafkaProducer-0.0.1-SNAPSHOT.jar app.jar

# Create non-root user for security
RUN adduser --disabled-password springuser
USER springuser

# Expose Spring Boot port (optional for Kafka producer)
EXPOSE 8083

# Run the application
ENTRYPOINT ["java", "-jar", "app.jar"]