#!/usr/bin/env bash
# =============================================================================
# FDS Tools — FDPipelineJob (mix_maprule) Deploy Script
#
# USAGE:
#   1. cp deploy/run-pipeline.sh deploy/run-pipeline.local.sh   (gitignored)
#   2. Fill in the credential and path sections below
#   3. chmod +x deploy/run-pipeline.local.sh
#   4. ./deploy/run-pipeline.local.sh
#
# This script:
#   a) Substitutes all <PLACEHOLDER> values in the YAML template
#   b) Writes the filled YAML to /tmp/fds-tools-pipeline-filled.yaml
#   c) Deletes any previous fds-tools-pipeline SparkApplication
#   d) Applies the new SparkApplication
# =============================================================================
set -euo pipefail

KUBECONFIG_PATH="${KUBECONFIG:-/Users/husingh/Desktop/fd-compute-spark-June/kubeconfig.yaml}"
YAML_TEMPLATE="$(dirname "$0")/fds-tools-pipeline-sparkapplication.yaml"
FILLED_YAML="/tmp/fds-tools-pipeline-filled.yaml"

# ── S3 credentials (fill in your local copy — NEVER commit real values) ───────
S3_ACCESS_KEY="5FVWSISC706ERZROGCNF"
S3_SECRET_KEY="PfU06XNC4oVKwIOC7DroaFy6VirbINDYU9LNGRkx"

# ── Encryption keys JSON — must contain all key indices used in input files ───
# Example: '{"1":"base64key1==","2":"base64key2=="}'
ENCRYPTION_KEYS_JSON='{"1":"fpyc32mCUpIaSE0aZgxmiC3Q+HPFGpxqyIDClfhR9lE=","2":"Og0extQ2dVxp4r9ZzxzDdzpDMSwReZd2rwlDpuZdLgE="}'

# ── S3 config ─────────────────────────────────────────────────────────────────
S3_BUCKET="fds-e3testing-bucket-husingh"

# ── MIX_MAPRULE_* paths (fill in your local copy) ────────────────────────────
#
# Pipeline step context (from the Hadoop pipeline):
#
#   AddFD step (config = g-type maprule):
#     MIX_MAPRULE_IDIR   = S3 prefix where fd-compute-spark-June wrote stdtime.* files
#                          e.g. "fd_compute_output/sqa"
#     MIX_MAPRULE_ODIR   = output prefix for the mix result
#                          e.g. "fd_tools_output/<run_id>"
#     MIX_MAPRULE_CONFIG = S3 path to the maprule config file (with .keyN suffix)
#                          e.g. "configs/g_mg1.config.key2"
#
#   ComputeStdTime step (config = flat):
#     MIX_MAPRULE_IDIR   = output prefix of AddFD step
#     MIX_MAPRULE_ODIR   = same prefix (mix_maprule writes stdtime.<suffix> there)
#     MIX_MAPRULE_CONFIG = S3 path to the "flat" config file
#                          e.g. "configs/flat.key2"
#
MIX_MAPRULE_IDIR="fd_compute_output/sqa"
MIX_MAPRULE_ODIR="fd_tools_output/runid_1"
MIX_MAPRULE_CONFIG="fd_tools_config/g*.mg1.key2"
MIX_MAPRULE_SUFFIX=""
MIX_MAPRULE_THINNING_PCT_JUMP="${MIX_MAPRULE_THINNING_PCT_JUMP:-}"

# ─────────────────────────────────────────────────────────────────────────────

echo "==> Substituting placeholders → $FILLED_YAML"
sed \
  -e "s|<S3_ACCESS_KEY>|${S3_ACCESS_KEY}|g" \
  -e "s|<S3_SECRET_KEY>|${S3_SECRET_KEY}|g" \
  -e "s|\$ENCRYPTION_KEYS_JSON|${ENCRYPTION_KEYS_JSON}|g" \
  -e "s|<S3_BUCKET>|${S3_BUCKET}|g" \
  -e "s|<MIX_MAPRULE_IDIR>|${MIX_MAPRULE_IDIR}|g" \
  -e "s|<MIX_MAPRULE_ODIR>|${MIX_MAPRULE_ODIR}|g" \
  -e "s|<MIX_MAPRULE_CONFIG>|${MIX_MAPRULE_CONFIG}|g" \
  -e "s|<MIX_MAPRULE_SUFFIX>|${MIX_MAPRULE_SUFFIX}|g" \
  -e "s|<MIX_MAPRULE_THINNING_PCT_JUMP>|${MIX_MAPRULE_THINNING_PCT_JUMP}|g" \
  "$YAML_TEMPLATE" > "$FILLED_YAML"

echo "==> Deleting previous SparkApplication (if any)"
kubectl --kubeconfig "$KUBECONFIG_PATH" delete sparkapplication fds-tools-pipeline \
  -n spark-operator --ignore-not-found

echo "==> Applying $FILLED_YAML"
kubectl --kubeconfig "$KUBECONFIG_PATH" apply -f "$FILLED_YAML"

echo ""
echo "==> Submitted. Watch with:"
echo "    kubectl --kubeconfig $KUBECONFIG_PATH get sparkapplication fds-tools-pipeline -n spark-operator -w"
echo "    kubectl --kubeconfig $KUBECONFIG_PATH logs -n spark-operator -l spark-role=driver -f"
