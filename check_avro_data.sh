#!/bin/bash
# =====================================================
# Script: check_avro_data.sh
# Description: Run Spark container with custom command
#              to open spark-shell and check Avro data
# Usage: bash check_avro_data.sh 2025-10-24
# =====================================================

CONTAINER_NAME="spark-app"

if [ -z "$1" ]; then
  echo "❗ Please provide a date argument (e.g., 2025-10-24)"
  exit 1
fi

DATE="$1"

# Find the output folder matching the date
AVRO_PATH=$(find data/output -type d -name "*${DATE}*")

if [ -z "$AVRO_PATH" ]; then
  echo "❗ No output folder found matching date '${DATE}'"
  exit 1
fi

# Save script in project so it's visible inside container
SCRIPT_DIR="data/scripts"
TMP_SCRIPT="${SCRIPT_DIR}/check_avro.scala"

mkdir -p "$SCRIPT_DIR"

echo "📝 Creating Scala script at $TMP_SCRIPT..."
cat <<EOF > "$TMP_SCRIPT"
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.getOrCreate()
val df = spark.read.format("avro").load("${AVRO_PATH}")
println("\\n===== DATA PREVIEW =====")
df.show(false)
println("\\n===== SCHEMA =====")
df.printSchema()
sys.exit(0)
EOF

echo "🧹 Stopping existing container (if running)..."
docker compose stop $CONTAINER_NAME 2>/dev/null || true

echo "🚀 Starting container with Spark Shell..."
docker compose run --rm $CONTAINER_NAME \
  /opt/spark/bin/spark-shell \
  --jars /opt/spark/jars/spark-avro_2.12-3.5.0.jar \
  -i "/app/${TMP_SCRIPT}"

echo "🧩 Done! Container exited."