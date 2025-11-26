#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# Kafka Dump Script (Bash)
#   - Dumps messages from Kafka topic
#   - Tars and encrypts using one-time password
#   - Uploads to Artifactory
#
# Expected environment variables:
#   ARTIFACTORY_BASE_URL  (e.g. https://artifactory.company.com/artifactory)
#   ARTIFACTORY_USER
#   ARTIFACTORY_PASSWORD
#
# Kafka console consumer location:
#   KAFKA_BIN_DIR (default: /opt/confluent/latest/bin)
# ------------------------------------------------------------------------------

KAFKA_BIN_DIR="${KAFKA_BIN_DIR:-/opt/confluent/latest/bin}"

usage() {
  cat <<EOF
Usage: $0 \\
  --inc-chg INC1234567 \\
  --request-id REQ-ABC1234567890 \\
  --cluster CLUSTER_NAME \\
  --env ENV \\
  --topic TOPIC_NAME \\
  --otp ONE_TIME_PASSWORD \\
  --requestor USER_ID

Required env vars:
  ARTIFACTORY_BASE_URL, ARTIFACTORY_USER, ARTIFACTORY_PASSWORD
EOF
  exit 1
}

# ------------------------------------------------------------------------------
# Parse arguments
# ------------------------------------------------------------------------------
INC_CHG=""
REQUEST_ID=""
CLUSTER=""
ENV_NAME=""
TOPIC=""
OTP=""
REQUESTOR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --inc-chg)
      INC_CHG="$2"
      shift 2
      ;;
    --request-id)
      REQUEST_ID="$2"
      shift 2
      ;;
    --cluster)
      CLUSTER="$2"
      shift 2
      ;;
    --env)
      ENV_NAME="$2"
      shift 2
      ;;
    --topic)
      TOPIC="$2"
      shift 2
      ;;
    --otp)
      OTP="$2"
      shift 2
      ;;
    --requestor)
      REQUESTOR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo "Unknown argument: $1"
      usage
      ;;
  esac
done

# Basic validation
if [[ -z "${INC_CHG}" || -z "${REQUEST_ID}" || -z "${CLUSTER}" || -z "${ENV_NAME}" || -z "${TOPIC}" || -z "${OTP}" || -z "${REQUESTOR}" ]]; then
  echo "ERROR: Missing required arguments."
  usage
fi

if [[ -z "${ARTIFACTORY_BASE_URL:-}" || -z "${ARTIFACTORY_USER:-}" || -z "${ARTIFACTORY_PASSWORD:-}" ]]; then
  echo "ERROR: ARTIFACTORY_BASE_URL, ARTIFACTORY_USER, ARTIFACTORY_PASSWORD must be set."
  exit 1
fi

# ------------------------------------------------------------------------------
# Resolve Kafka bootstrap + security config based on cluster/env
# ------------------------------------------------------------------------------

resolve_kafka_config() {
  case "${CLUSTER}:${ENV_NAME}" in
    "PHY-PROD-CL1:PROD")
      KAFKA_BOOTSTRAP="phy-prod-kafka:9093"
      # Example SSL config (adjust to your environment)
      KAFKA_SECURITY_OPTS="--consumer.config /opt/kafka/conf/phy-prod-consumer.properties"
      ;;
    "VM-UAT-CL2:UAT")
      KAFKA_BOOTSTRAP="vm-uat-kafka:9093"
      KAFKA_SECURITY_OPTS="--consumer.config /opt/kafka/conf/vm-uat-consumer.properties"
      ;;
    *)
      echo "ERROR: Unknown cluster/env combination: ${CLUSTER}/${ENV_NAME}" >&2
      exit 1
      ;;
  esac
}

resolve_kafka_config

# ------------------------------------------------------------------------------
# Work directory
# ------------------------------------------------------------------------------
WORKDIR="$(mktemp -d "/tmp/kafka-dump-${REQUEST_ID}-XXXX")"
trap 'rm -rf "${WORKDIR}"' EXIT

DUMP_FILE="${WORKDIR}/${TOPIC}.jsonl"
META_FILE="${WORKDIR}/metadata.json"
TAR_FILE="${REQUEST_ID}.tar"
ENC_FILE="${REQUEST_ID}.tar.enc"

# ------------------------------------------------------------------------------
# Dump messages from Kafka using kafka-console-consumer
#   - Adjust to kafka-avro-console-consumer / protobuf / json-schema if needed
# ------------------------------------------------------------------------------
echo "Dumping messages from topic '${TOPIC}' on cluster '${CLUSTER}' (${KAFKA_BOOTSTRAP})..."

"${KAFKA_BIN_DIR}/kafka-console-consumer" \
  --bootstrap-server "${KAFKA_BOOTSTRAP}" \
  --topic "${TOPIC}" \
  --from-beginning \
  --timeout-ms 60000 \
  ${KAFKA_SECURITY_OPTS} \
  > "${DUMP_FILE}"

MSG_COUNT="$(wc -l < "${DUMP_FILE}" || echo 0)"

# ------------------------------------------------------------------------------
# Create metadata.json
# ------------------------------------------------------------------------------
echo "Creating metadata.json..."

cat > "${META_FILE}" <<EOF
{
  "request_id": "${REQUEST_ID}",
  "inc_chg": "${INC_CHG}",
  "cluster": "${CLUSTER}",
  "env": "${ENV_NAME}",
  "topic": "${TOPIC}",
  "created_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "created_by": "${REQUESTOR}",
  "message_count": ${MSG_COUNT}
}
EOF

# ------------------------------------------------------------------------------
# Tar and encrypt using one-time password
# ------------------------------------------------------------------------------
echo "Creating tarball and encrypting with one-time password..."

tar -cf "${TAR_FILE}" -C "${WORKDIR}" .

# AES-256-CBC encryption using openssl
openssl enc -aes-256-cbc -salt \
  -in "${TAR_FILE}" \
  -out "${ENC_FILE}" \
  -k "${OTP}"

# ------------------------------------------------------------------------------
# Upload to Artifactory
#   Path: kafka-dump/<env>/<topic>/<request_id>/<request_id>.tar.enc
# ------------------------------------------------------------------------------
REPO_PATH="kafka-dump/${ENV_NAME,,}/${TOPIC}/${REQUEST_ID}/${ENC_FILE}"
UPLOAD_URL="${ARTIFACTORY_BASE_URL%/}/${REPO_PATH}"

echo "Uploading encrypted tarball to Artifactory: ${UPLOAD_URL}"

curl -sS -u "${ARTIFACTORY_USER}:${ARTIFACTORY_PASSWORD}" \
  -X PUT \
  -T "${ENC_FILE}" \
  "${UPLOAD_URL}"

# If curl fails, exit non-zero
if [[ $? -ne 0 ]]; then
  echo "ERROR: Upload to Artifactory failed." >&2
  exit 1
fi

# ------------------------------------------------------------------------------
# Output JSON for the portal/backend
# ------------------------------------------------------------------------------
cat <<EOF
{"status": "OK", "artifactory_url": "${UPLOAD_URL}", "request_id": "${REQUEST_ID}", "message_count": ${MSG_COUNT}}
EOF
