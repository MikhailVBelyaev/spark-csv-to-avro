# ========================
# Stage 1: Build the App
# ========================
FROM hseeberger/scala-sbt:11.0.14.1_1.6.2_2.12.15 AS builder

WORKDIR /app
COPY . .
RUN rm -rf /root/.cache/coursier && \
    sbt clean compile assembly

# ========================
# Stage 2: Runtime
# ========================
FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu
# FROM openjdk:11-jdk

# ✅ Switch to root temporarily for package installation
# USER root
# RUN apt-get update && \
#    apt-get install -y wget && \
#    wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql_2.12/3.5.0/spark-sql_2.12-3.5.0.jar -P /opt/spark/jars/ && \
#    wget https://repo1.maven.org/maven2/org/apache/spark/spark-core_2.12/3.5.0/spark-core_2.12-3.5.0.jar -P /opt/spark/jars/ && \
#    rm -rf /var/lib/apt/lists/*

# ✅ Return to the spark user for runtime
# USER 185

WORKDIR /app
COPY --from=builder /app/target/scala-2.12/*.jar /app/app.jar
COPY src/main/resources/application.conf /app/application.conf
COPY src/main/resources/log4j2.properties /app/log4j2.properties
COPY data /app/data

CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql_2.12:3.5.0", "--class", "com.example.CsvToAvroApp", "/app/app.jar"]
# CMD ["spark-submit", "--class", "com.example.CsvToAvroApp", "/app/app.jar"]
# CMD ["java", "-jar", "/app/app.jar"]