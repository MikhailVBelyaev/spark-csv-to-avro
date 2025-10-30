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

echo "🚀 Building documentation with sbt doc inside $CONTAINER_NAME container..."
docker compose run --rm -v "$(pwd)/data:/app/data" $CONTAINER_NAME bash -c "
  echo '🛠 Running sbt doc...'
  sbt doc && \
  echo '📂 Copying generated docs to /app/data/doc...' && \
  mkdir -p /app/data/doc && \
  cp -r /app/target/scala-2.12/api/* /app/data/doc/ || echo '⚠️ No documentation files found in target folder.'
"

echo "🔍 Checking if documentation was generated..."
if [ -n "$(ls -A $DOC_OUTPUT_DIR 2>/dev/null)" ]; then
  echo "✅ Documentation successfully generated and saved to $DOC_OUTPUT_DIR"
  echo "📂 Copying documentation to local project folder (docs/)..."
  mkdir -p docs
  cp -r "$DOC_OUTPUT_DIR"/* docs/
  sudo chown -R $USER:$USER docs
  chmod -R a+rX docs
  echo "✅ Documentation copied to docs/ with correct permissions."
else
  echo "❌ No documentation found in $DOC_OUTPUT_DIR. Check sbt output or volume mapping."
  exit 1
fi