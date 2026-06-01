#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_unshard_job.sh — FD unshard pipeline (fd-tools)
#
# Downloads one stdtime file from Linode S3, applies the C++ `unshard` logic
# via JNI, and uploads the result back to S3.
#
# Configuration via env vars (mirrors the standalone `unshard` binary's
# Hadoop env-var convention — note that IDIR/ODIR are FILE paths, not dirs):
#
#   UNSHARD_IDIR     S3 path to input stdtime file   (required)
#   UNSHARD_ODIR     S3 path to output stdtime file  (required)
#   UNSHARD_FACTOR   Unshard factor (float)          (required)
#
# Usage:
#   UNSHARD_IDIR=results_dir/runId/stdtime.flat \
#   UNSHARD_ODIR=results_dir/runId/stdtime.unshard \
#   UNSHARD_FACTOR=16 \
#   ./run_unshard_job.sh
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"

# ---------------------------------------------------------------------------
# S3 credentials
# ---------------------------------------------------------------------------
export S3_ACCESS_KEY="${S3_ACCESS_KEY:-7O2KSN0FIDL9NY97JEF8}"
export S3_SECRET_KEY="${S3_SECRET_KEY:-NCromtL8w1RvTo8Afa6hN7wHNDeZopNX8emHlpBd}"

# ---------------------------------------------------------------------------
# S3 config
# ---------------------------------------------------------------------------
S3_BUCKET="${S3_BUCKET:-fds-e3testing-bucket-husingh}"
S3_ENDPOINT="${S3_ENDPOINT:-us-ord-10.linodeobjects.com}"

# Key index used to SSE-C encrypt the output upload. If UNSHARD_ODIR already
# ends in .keyN, it is used as-is; otherwise .key<S3_OUTPUT_KEY_INDEX> is appended.
S3_OUTPUT_KEY_INDEX="${S3_OUTPUT_KEY_INDEX:-2}"

# Optional: local directory to save intermediate files for inspection
FD_LOCAL_DEBUG_DIR="${FD_LOCAL_DEBUG_DIR:-}"

# ---------------------------------------------------------------------------
# UNSHARD_* env vars — required
# ---------------------------------------------------------------------------
: "${UNSHARD_IDIR:?UNSHARD_IDIR must be set (S3 path to input stdtime file)}"
: "${UNSHARD_ODIR:?UNSHARD_ODIR must be set (S3 path to output stdtime file)}"
: "${UNSHARD_FACTOR:?UNSHARD_FACTOR must be set (float, e.g. 16)}"

# ---------------------------------------------------------------------------
# Local config
# ---------------------------------------------------------------------------
ENCRYPTION_KEYS_FILE="${ENCRYPTION_KEYS_FILE:-$PROJECT_DIR/conf/encryption_keys.json}"
LIB_DIR="$PROJECT_DIR/lib"

# ---------------------------------------------------------------------------
# Locate spark-submit
# ---------------------------------------------------------------------------
if command -v spark-submit &>/dev/null; then
  SPARK_SUBMIT="spark-submit"
elif [ -f "/opt/homebrew/opt/apache-spark/bin/spark-submit" ]; then
  SPARK_SUBMIT="/opt/homebrew/opt/apache-spark/bin/spark-submit"
elif [ -n "${SPARK_HOME:-}" ] && [ -f "$SPARK_HOME/bin/spark-submit" ]; then
  SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
else
  echo "ERROR: spark-submit not found. Install with: brew install apache-spark"
  exit 1
fi

# ---------------------------------------------------------------------------
# Locate the jar
# ---------------------------------------------------------------------------
JAR=$(find "$PROJECT_DIR/target" -name "fd-spark*.jar" \
        -not -name "*javadoc*" 2>/dev/null | sort | tail -1)
if [ -z "$JAR" ]; then
  echo "ERROR: No jar found. Build with: sbt package"
  exit 1
fi

echo "========================================"
echo "  UnshardJob"
echo "========================================"
echo "  spark-submit         : $SPARK_SUBMIT"
echo "  jar                  : $JAR"
echo "  S3 bucket            : $S3_BUCKET"
echo "  S3 endpoint          : $S3_ENDPOINT"
echo "  UNSHARD_IDIR         : $UNSHARD_IDIR"
echo "  UNSHARD_ODIR         : $UNSHARD_ODIR"
echo "  UNSHARD_FACTOR       : $UNSHARD_FACTOR"
echo "  S3 output key index  : $S3_OUTPUT_KEY_INDEX"
if [ -n "$FD_LOCAL_DEBUG_DIR" ]; then
  echo "  Local debug dir      : $FD_LOCAL_DEBUG_DIR"
else
  echo "  Local debug dir      : (not set)"
fi
echo "  Encryption keys      : $ENCRYPTION_KEYS_FILE"
echo "========================================"
echo ""

"$SPARK_SUBMIT" \
  --master "local[*]" \
  --class com.fd.UnshardJob \
  --packages "org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  --conf "spark.driver.bindAddress=127.0.0.1" \
  --conf "spark.driver.host=127.0.0.1" \
  --conf "spark.driver.memory=4g" \
  --conf "spark.executor.memory=4g" \
  --conf "spark.executorEnv.S3_ACCESS_KEY=$S3_ACCESS_KEY" \
  --conf "spark.executorEnv.S3_SECRET_KEY=$S3_SECRET_KEY" \
  --conf "spark.executorEnv.S3_ENDPOINT=$S3_ENDPOINT" \
  --conf "spark.executorEnv.S3_OUTPUT_KEY_INDEX=$S3_OUTPUT_KEY_INDEX" \
  --conf "spark.executorEnv.FD_LOCAL_DEBUG_DIR=$FD_LOCAL_DEBUG_DIR" \
  --conf "spark.executorEnv.UNSHARD_IDIR=$UNSHARD_IDIR" \
  --conf "spark.executorEnv.UNSHARD_ODIR=$UNSHARD_ODIR" \
  --conf "spark.executorEnv.UNSHARD_FACTOR=$UNSHARD_FACTOR" \
  --conf "spark.driver.extraJavaOptions=-Djava.library.path=$LIB_DIR --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED" \
  --conf "spark.executor.extraJavaOptions=-Djava.library.path=$LIB_DIR --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED" \
  "$JAR" \
  "$S3_BUCKET" \
  "$ENCRYPTION_KEYS_FILE"

echo ""
echo "Done."
