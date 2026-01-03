#!/bin/bash
# =====================================================
# Script: check_avro_data.sh
# Description: Run Spark container with custom command
#              to open spark-shell and check Avro data,
#              including corrupted rows and casting errors
# Usage: bash check_avro_data.sh 2026-01-03
# =====================================================

CONTAINER_NAME="spark-app"

if [ -z "$1" ]; then
  echo "❗ Please provide a date argument (e.g., 2026-01-03)"
  exit 1
fi

DATE="$1"

# Find the output folder matching the date
AVRO_PATH=$(find data/output -type d -name "*${DATE}*")

if [ -z "$AVRO_PATH" ]; then
  echo "❗ No output folder found matching date '${DATE}'"
  exit 1
fi

# Script location inside project
SCRIPT_DIR="data/scripts"
TMP_SCRIPT="${SCRIPT_DIR}/check_avro_all.scala"
mkdir -p "$SCRIPT_DIR"

echo "📝 Creating Scala script at $TMP_SCRIPT..."
cat <<EOF > "$TMP_SCRIPT"
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

// ================= VALID AVRO =================
println("\\n===== ✅ VALID AVRO DATA =====")
val validDf = spark.read.format("avro").load("${AVRO_PATH}")
validDf.show(20, false)
println(s"Valid rows count: \${validDf.count()}")

// ================= CASTING ERRORS =================
println("\\n===== ⚠️ CASTING ERRORS =====")
val castPath = "data/corrupted/cast_errors_*"
val castDir = new java.io.File("data/corrupted")
if (castDir.exists && castDir.list.exists(_.startsWith("cast_errors"))) {
    val castDf = spark.read.option("header","true").csv(castPath)
    castDf.show(20, false)
    println(s"Casting errors count: \${castDf.count()}")
} else {
    println("No casting errors found.")
}

// ================= STRUCTURAL ERRORS =================
println("\\n===== ❌ STRUCTURAL ERRORS =====")
val structPath = "data/corrupted/structural_*"
val structDf = spark.read.option("header","false").csv(structPath)
if (structDf.take(1).nonEmpty) {
    structDf.show(20, false)
    println(s"Structural errors count: \${structDf.count()}")
} else {
    println("No structural errors found.")
}

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