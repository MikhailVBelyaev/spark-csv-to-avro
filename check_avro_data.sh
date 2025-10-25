#!/bin/bash
# =====================================================
# Script: check_avro_data.sh
# Description: Run Spark container with custom command
#              to open spark-shell and check Avro data
# =====================================================

# Container name (adjust if different in docker-compose)
CONTAINER_NAME="spark-app"

# Avro data path (relative to container)
AVRO_PATH="data/output/processing_date=2025-10-24"

# Step 1. Stop any running container
echo "🧹 Stopping existing container (if running)..."
docker compose stop $CONTAINER_NAME 2>/dev/null || true

# Step 2. Run the container with spark-shell command
echo "🚀 Starting container with Spark Shell..."
docker compose run --rm $CONTAINER_NAME \
  /opt/spark/bin/spark-shell --jars /opt/spark/jars/spark-avro_2.12-3.5.0.jar \
  -e "
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder.getOrCreate()
    val df = spark.read.format(\"avro\").load(\"${AVRO_PATH}\")
    df.show(false)
    df.printSchema()
  "

# Step 3. Stop all services
echo "🧩 Done! Container exited."