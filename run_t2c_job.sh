#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_t2c_job.sh — FD t2c (stdtime → stdcount) pipeline (fd-tools)
#
# Downloads one stdtime file from Linode S3, applies the C++ `t2c` logic
# via JNI, and uploads the resulting stdcount file back to S3.
#
# Configuration via env vars (mirrors the standalone `t2c` binary's
# Hadoop env-var convention — both T2C_IDIR and T2C_ODIR are FILE paths):
#
#   T2C_IDIR    S3 path to input stdtime file    (required)
#   T2C_ODIR    S3 path for output stdcount file (required)
#
# Usage:
#   T2C_IDIR=results_dir/runId/stdtime.unshard.key2 \
#   T2C_ODIR=results_dir/runId/stdcount.unshard \
#   ./run_t2c_job.sh
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

# Key index used to SSE-C encrypt the output upload. If T2C_ODIR already
# ends in .keyN, it is used as-is; otherwise .key<S3_OUTPUT_KEY_INDEX> is appended.
S3_OUTPUT_KEY_INDEX="${S3_OUTPUT_KEY_INDEX:-2}"

# Optional: local directory to save intermediate files for inspection
FD_LOCAL_DEBUG_DIR="${FD_LOCAL_DEBUG_DIR:-}"

# ---------------------------------------------------------------------------
# T2C_* env vars — required
# ---------------------------------------------------------------------------
: "${T2C_IDIR:?T2C_IDIR must be set (S3 path to input stdtime file)}"
: "${T2C_ODIR:?T2C_ODIR must be set (S3 path to output stdcount file)}"

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
echo "  T2cJob (stdtime → stdcount)"
echo "========================================"
echo "  spark-submit         : $SPARK_SUBMIT"
echo "  jar                  : $JAR"
echo "  S3 bucket            : $S3_BUCKET"
echo "  S3 endpoint          : $S3_ENDPOINT"
echo "  T2C_IDIR             : $T2C_IDIR"
echo "  T2C_ODIR             : $T2C_ODIR"
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
  --class com.fd.T2cJob \
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
  --conf "spark.executorEnv.T2C_IDIR=$T2C_IDIR" \
  --conf "spark.executorEnv.T2C_ODIR=$T2C_ODIR" \
  --conf "spark.driver.extraJavaOptions=-Djava.library.path=$LIB_DIR --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED" \
  --conf "spark.executor.extraJavaOptions=-Djava.library.path=$LIB_DIR --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED" \
  "$JAR" \
  "$S3_BUCKET" \
  "$ENCRYPTION_KEYS_FILE"

echo ""
echo "Done."
