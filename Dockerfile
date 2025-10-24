# Stage 1: Build the App
FROM hseeberger/scala-sbt:11.0.14.1_1.6.2_2.12.15 AS builder
WORKDIR /app
COPY . .
RUN sbt clean compile assembly

# Stage 2: Runtime
FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu
WORKDIR /app
COPY --from=builder /app/target/scala-2.12/spark-csv-to-avro-assembly-0.1.jar /app/app.jar
COPY src/main/resources/application.conf /app/application.conf
COPY src/main/resources/log4j2.properties /app/log4j2.properties
COPY data /app/data
CMD ["/opt/spark/bin/spark-submit", "--class", "com.example.CsvToAvroApp", "--driver-java-options", "-Dlog4j.configurationFile=/app/log4j2.properties -Dlog4j.debug=true", "/app/app.jar"]