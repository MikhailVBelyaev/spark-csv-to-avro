#!/bin/bash
# =====================================================
# Script: generate_doc.sh
# Description: Generate ScalaDoc documentation inside
#              Spark build container and copy output
#              to data/doc/
# Usage: bash generate_doc.sh
# =====================================================

CONTAINER_NAME="spark-test"
DOC_OUTPUT_DIR="data/doc"

echo "🧹 Cleaning previous documentation..."
rm -rf "$DOC_OUTPUT_DIR"
mkdir -p "$DOC_OUTPUT_DIR"

echo "🚀 Building documentation with sbt doc..."
docker compose run --rm $CONTAINER_NAME bash -c "
  sbt doc && \
  mkdir -p /app/data/doc && \
  cp -r /app/target/scala-2.12/api/* /app/data/doc/
"

if [ $? -eq 0 ]; then
  echo "✅ Documentation successfully generated and saved to $DOC_OUTPUT_DIR"
else
  echo "❌ Failed to generate documentation"
  exit 1
fi