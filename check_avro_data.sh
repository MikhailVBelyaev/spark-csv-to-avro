#!/bin/bash
# =====================================================
# Script: check_avro_data.sh
# Description: Run Spark container with custom command
#              to open spark-shell and check Avro data
# Usage: bash check_avro_data.sh 2025-10-24
# =====================================================

# Container name (adjust if different in docker-compose)
CONTAINER_NAME="spark-app"

# Check argument
if [ -z "$1" ]; then
  echo "❗ Please provide a date argument (e.g., 2025-10-24)"
  exit 1
fi

# Avro data path (relative to container)
AVRO_PATH="data/output/processing_date=$1"

# Step 1. Create temporary Scala script
TMP_SCRIPT="/tmp/check_avro.scala"
echo "📝 Creating temporary Scala script at $TMP_SCRIPT..."
cat <<EOF > $TMP_SCRIPT
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.getOrCreate()
val df = spark.read.format("avro").load("${AVRO_PATH}")
println("\\n===== DATA PREVIEW =====")
df.show(false)
println("\\n===== SCHEMA =====")
df.printSchema()
sys.exit(0)
EOF

# Step 2. Stop any running container
echo "🧹 Stopping existing container (if running)..."
docker compose stop $CONTAINER_NAME 2>/dev/null || true

# Step 3. Run the container with spark-shell command
echo "🚀 Starting container with Spark Shell..."
docker compose run --rm $CONTAINER_NAME \
  /opt/spark/bin/spark-shell --jars /opt/spark/jars/spark-avro_2.12-3.5.0.jar \
  -i $TMP_SCRIPT

# Step 4. Clean up temporary Scala script
rm -f $TMP_SCRIPT

# Step 5. Done
echo "🧩 Done! Container exited."