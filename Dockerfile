# ========================
# Stage 1: Build the App
# ========================
FROM hseeberger/scala-sbt:11.0.14.1_1.6.2_2.12.15 AS builder

WORKDIR /app
COPY . .
RUN sbt clean compile assembly

# ========================
# Stage 2: Runtime
# ========================
FROM openjdk:11-jre-slim

WORKDIR /app
COPY --from=builder /app/target/scala-2.12/*.jar /app/app.jar
COPY src/main/resources/application.conf /app/application.conf
COPY src/main/resources/log4j2.properties /app/log4j2.properties
COPY data /app/data

CMD ["java", "-jar", "/app/app.jar"]