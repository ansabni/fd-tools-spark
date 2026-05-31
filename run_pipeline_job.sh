#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_pipeline_job.sh — FD mix pipeline (fd-tools)
#
# Downloads stdtime.* footprint descriptor files from Linode S3,
# applies mix_maprule C++ logic, and uploads the output back to S3.
#
# S3 paths are configured via MIX_MAPRULE_* env vars (same convention as
# the mix_maprule binary):
#
#   MIX_MAPRULE_IDIR          S3 prefix for stdtime.* inputs  (required)
#   MIX_MAPRULE_ODIR          S3 prefix for output            (required)
#   MIX_MAPRULE_CONFIG        S3 path to config file          (required)
#   MIX_MAPRULE_SUFFIX        Output suffix                   (optional)
#   MIX_MAPRULE_THINNING_PCT_JUMP  Thinning pct              (optional)
#
# Usage:
#   MIX_MAPRULE_IDIR=Anirudh/fds \
#   MIX_MAPRULE_ODIR=Anirudh/mix_output \
#   MIX_MAPRULE_CONFIG=Anirudh/config.key2 \
#   ./run_pipeline_job.sh
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

# Key index used to SSE-C encrypt the output files uploaded to S3.
# The output filename will be <file>.key<id> (e.g. stdtime.config.key2).
S3_OUTPUT_KEY_INDEX="${S3_OUTPUT_KEY_INDEX:-2}"

# Optional: local directory to save intermediate files for inspection.
FD_LOCAL_DEBUG_DIR="${FD_LOCAL_DEBUG_DIR:-}"

# ---------------------------------------------------------------------------
# MIX_MAPRULE_* env vars — S3 paths for the pipeline
# ---------------------------------------------------------------------------
: "${MIX_MAPRULE_IDIR:?MIX_MAPRULE_IDIR must be set (S3 prefix for stdtime.* inputs, e.g. Anirudh/fds)}"
: "${MIX_MAPRULE_ODIR:?MIX_MAPRULE_ODIR must be set (S3 prefix for output, e.g. Anirudh/mix_output)}"
: "${MIX_MAPRULE_CONFIG:?MIX_MAPRULE_CONFIG must be set (S3 path to config file, e.g. Anirudh/config.key2)}"
MIX_MAPRULE_SUFFIX="${MIX_MAPRULE_SUFFIX:-}"
MIX_MAPRULE_THINNING_PCT_JUMP="${MIX_MAPRULE_THINNING_PCT_JUMP:-}"

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
echo "  FDPipelineJob (mix)"
echo "========================================"
echo "  spark-submit             : $SPARK_SUBMIT"
echo "  jar                      : $JAR"
echo "  S3 bucket                : $S3_BUCKET"
echo "  S3 endpoint              : $S3_ENDPOINT"
echo "  MIX_MAPRULE_IDIR         : $MIX_MAPRULE_IDIR"
echo "  MIX_MAPRULE_ODIR         : $MIX_MAPRULE_ODIR"
echo "  MIX_MAPRULE_CONFIG       : $MIX_MAPRULE_CONFIG"
echo "  MIX_MAPRULE_SUFFIX       : ${MIX_MAPRULE_SUFFIX:-(derived from config filename)}"
echo "  MIX_MAPRULE_THINNING_PCT : ${MIX_MAPRULE_THINNING_PCT_JUMP:-(not set, default 0.0)}"
echo "  S3 output key index      : $S3_OUTPUT_KEY_INDEX  (output files named <file>.key${S3_OUTPUT_KEY_INDEX})"
if [ -n "$FD_LOCAL_DEBUG_DIR" ]; then
  echo "  Local debug dir          : $FD_LOCAL_DEBUG_DIR"
else
  echo "  Local debug dir          : (not set)"
fi
echo "  Encryption keys          : $ENCRYPTION_KEYS_FILE"
echo "========================================"
echo ""

"$SPARK_SUBMIT" \
  --master "local[*]" \
  --class com.fd.FDPipelineJob \
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
  --conf "spark.executorEnv.MIX_MAPRULE_IDIR=$MIX_MAPRULE_IDIR" \
  --conf "spark.executorEnv.MIX_MAPRULE_ODIR=$MIX_MAPRULE_ODIR" \
  --conf "spark.executorEnv.MIX_MAPRULE_CONFIG=$MIX_MAPRULE_CONFIG" \
  --conf "spark.executorEnv.MIX_MAPRULE_SUFFIX=$MIX_MAPRULE_SUFFIX" \
  --conf "spark.executorEnv.MIX_MAPRULE_THINNING_PCT_JUMP=$MIX_MAPRULE_THINNING_PCT_JUMP" \
  --conf "spark.driver.extraJavaOptions=-Djava.library.path=$LIB_DIR --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED" \
  --conf "spark.executor.extraJavaOptions=-Djava.library.path=$LIB_DIR --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED" \
  "$JAR" \
  "$S3_BUCKET" \
  "$ENCRYPTION_KEYS_FILE"

echo ""
echo "Done. Results uploaded to: s3://$S3_BUCKET/$MIX_MAPRULE_ODIR/"
