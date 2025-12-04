#!/bin/bash

export INC="${p:INC_NUMBER}"
export REQ="${p:REQ_NUMBER}"
export KAFKA_BOOTSTRAP="${p:BOOTSTRAP_SERVERS}"
export TOPIC="${p:KAFKA_TOPIC}"
export OTP="${p:OTP_PASSWORD}"
export REQUESTOR="${p:REQUESTOR}"

export CLUSTER="${p:CLUSTER_NAME}"
export ENV_NAME="${p:ENVIRONMENT_NAME}"

export ARTIFACTORY_BASE_URL="${p:ARTIFACTORY_BASE_URL}"
export ARTIFACTORY_USER="${p:ARTIFACTORY_USER}"
export ARTIFACTORY_PASSWORD="${p:ARTIFACTORY_PASSWORD}"

# For traceability in logs
export UCD_RUN_ID="${p:componentProcess.run.id}"

# Debug (mask sensitive values)
echo "INC=$INC"
echo "REQ=$REQ"
echo "TOPIC=$TOPIC"
echo "KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP"
echo "REQUESTOR=$REQUESTOR"
echo "Cluster=$CLUSTER Env=$ENV_NAME"
echo "Artifactory User=$ARTIFACTORY_USER"
echo "UCD Run ID=$UCD_RUN_ID"

chmod +x ${p:component.workspace}/scripts/kafka_dump.sh

${p:component.workspace}/scripts/kafka_dump.sh

#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Kafka Dump Utility (UCD-Compatible Version)
# ============================================================================

BASE_DIR="/var/log/confluent/kafka_dump"
KAFKA_BIN_DIR_DEFAULT="/opt/confluent/latest/bin"
DISK_THRESHOLD=85
AUDIT_LOG="${BASE_DIR}/audit.log"
CLUSTER_JSON="/etc/kafka_dump/cluster_map.json"

# UCD RUN INFO
UCD_RUN_ID="${UCD_RUN_ID:-"$(date +%s)"}"

# ============================================================================
# Resolve arguments and allow UCD environment overrides
# ============================================================================

ARG_INC="${1:-""}"
ARG_REQ="${2:-""}"

# UCD overrides
INC="${INC:-${ARG_INC}}"
REQ="${REQ:-${ARG_REQ}}"

# Pull properties from UCD environment variables if set
INC="${INC:-${UCD_INC_NUMBER:-""}}"
REQ="${REQ:-${UCD_REQ_NUMBER:-""}}"
TOPIC="${TOPIC:-${UCD_KAFKA_TOPIC:-""}}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-${UCD_BOOTSTRAP_SERVERS:-""}}"
OTP="${OTP:-${UCD_OTP_PASSWORD:-""}}"
REQUESTOR="${REQUESTOR:-${UCD_REQUESTOR:-"ucd-user"}}"
CLUSTER="${CLUSTER:-${UCD_CLUSTER_NAME:-""}}"
ENV_NAME="${ENV_NAME:-${UCD_ENVIRONMENT_NAME:-""}}"

# Artifactory credentials from UCD fields
ARTIFACTORY_BASE_URL="${ARTIFACTORY_BASE_URL:-${UCD_ARTIFACTORY_BASE_URL:-""}}"
ARTIFACTORY_USER="${ARTIFACTORY_USER:-${UCD_ARTIFACTORY_USER:-""}}"
ARTIFACTORY_PASSWORD="${ARTIFACTORY_PASSWORD:-${UCD_ARTIFACTORY_PASSWORD:-""}}"

# ============================================================================
# Validation Section
# ============================================================================

fail() {
    printf '{"status":"ERROR","message":"%s"}\n' "$1"
    exit 1
}

[[ -z "$INC" ]] && fail "INC number missing"
[[ -z "$REQ" ]] && fail "REQ number missing"
[[ -z "$TOPIC" ]] && fail "Kafka topic missing"
[[ -z "$KAFKA_BOOTSTRAP" ]] && fail "Bootstrap servers missing"
[[ -z "$OTP" ]] && fail "OTP password missing"
[[ -z "$ARTIFACTORY_BASE_URL" ]] && fail "Artifactory URL missing"
[[ -z "$ARTIFACTORY_USER" ]] && fail "Artifactory user missing"
[[ -z "$ARTIFACTORY_PASSWORD" ]] && fail "Artifactory password missing"

# ============================================================================
# Logging and Folder Setup
# ============================================================================

mkdir -p "$BASE_DIR"
touch "$AUDIT_LOG"

WORKDIR="${BASE_DIR}/${INC}/${REQ}/${TOPIC}"
mkdir -p "$WORKDIR"

LOG_FILE="${WORKDIR}/kafka_dump.log"

log() {
    local level="$1"
    shift
    local msg="$*"

    printf '{"ts":"%s","level":"%s","ucdRunId":"%s","inc":"%s","req":"%s","topic":"%s","msg":"%s"}\n' \
        "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$level" "$UCD_RUN_ID" "$INC" "$REQ" "$TOPIC" "$msg" >> "$LOG_FILE"
}

log INFO "---- UCD Kafka Dump Execution Started ----"
log INFO "Bootstrap: $KAFKA_BOOTSTRAP"
log INFO "Topic: $TOPIC"

# ============================================================================
# Disk Check
# ============================================================================
usage_pct=$(df --output=pcent "$BASE_DIR" | tail -1 | tr -dc '0-9')
if (( usage_pct >= DISK_THRESHOLD )); then
    fail "Disk usage too high (${usage_pct}%). Aborting."
fi

# ============================================================================
# Kafka Binary Resolution
# ============================================================================
KAFKA_BIN_DIR="${KAFKA_BIN_DIR:-$KAFKA_BIN_DIR_DEFAULT}"

if [[ ! -x "$KAFKA_BIN_DIR/kafka-console-consumer" ]]; then
    if command -v kafka-console-consumer >/dev/null 2>&1; then
        KAFKA_BIN_DIR="$(dirname "$(command -v kafka-console-consumer)")"
    else
        fail "kafka-console-consumer not found"
    fi
fi

# ============================================================================
# Optional JSON-based security config
# ============================================================================
KAFKA_SECURITY_OPTS=""
if [[ -f "$CLUSTER_JSON" ]]; then
    cfg=$(jq -r ".\"${CLUSTER}\".\"${ENV_NAME}\".config" "$CLUSTER_JSON" 2>/dev/null || echo "null")
    if [[ "$cfg" != "null" ]]; then
        KAFKA_SECURITY_OPTS="--consumer.config $cfg"
        log INFO "Using security config: $cfg"
    fi
fi

# ============================================================================
# Kafka Dump
# ============================================================================
DUMP_FILE="${WORKDIR}/${TOPIC}.jsonl"

dump_kafka() {
    local attempt=1
    while (( attempt <= 3 )); do
        log INFO "Kafka dump attempt $attempt"
        if "$KAFKA_BIN_DIR/kafka-console-consumer" \
            --bootstrap-server "$KAFKA_BOOTSTRAP" \
            --topic "$TOPIC" \
            --from-beginning \
            --timeout-ms 60000 \
            --property print.timestamp=true \
            --property print.offset=true \
            --property print.partition=true \
            --property print.headers=true \
            --property print.key=true \
            $KAFKA_SECURITY_OPTS > "$DUMP_FILE"; then
                return 0
        fi
        sleep $((attempt * 2))
        (( attempt++ ))
    done
    return 1
}

if ! dump_kafka; then
    fail "Kafka dump failed after retries"
fi

MSG_COUNT=$(wc -l < "$DUMP_FILE")
log INFO "Dump completed: $MSG_COUNT messages"

# ============================================================================
# Metadata + Encrypt + Upload
# ============================================================================

META_FILE="${WORKDIR}/metadata.json"
cat > "$META_FILE" <<EOF
{
  "inc": "${INC}",
  "req": "${REQ}",
  "topic": "${TOPIC}",
  "bootstrap": "${KAFKA_BOOTSTRAP}",
  "message_count": ${MSG_COUNT},
  "ucd_run_id": "${UCD_RUN_ID}",
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF

TAR_FILE="${WORKDIR}/${REQ}.tar"
GZ_FILE="${TAR_FILE}.gz"
ENC_FILE="${WORKDIR}/${REQ}.tar.gz.gpg"

tar -cf "$TAR_FILE" -C "$WORKDIR" .
gzip -9 "$TAR_FILE"

printf "%s" "$OTP" | gpg --batch --yes --passphrase-fd 0 \
    --symmetric --cipher-algo AES256 "$GZ_FILE"

# ============================================================================
# Artifactory Upload (strict HTTP 200/201)
# ============================================================================

UPLOAD_URL="${ARTIFACTORY_BASE_URL%/}/kafka-dump/INC/${INC}/${REQ}/${TOPIC}/$(basename "$ENC_FILE")"

upload() {
    local attempt=1
    while (( attempt <= 3 )); do
        HTTP_CODE=$(curl -s -w "%{http_code}" -o /dev/null \
            -u "${ARTIFACTORY_USER}:${ARTIFACTORY_PASSWORD}" \
            -X PUT -T "$ENC_FILE" "$UPLOAD_URL")

        if [[ "$HTTP_CODE" == "200" || "$HTTP_CODE" == "201" ]]; then
            return 0
        fi

        sleep $((attempt * 2))
        (( attempt++ ))
    done
    return 1
}

if ! upload; then
    fail "Upload to Artifactory failed"
fi

# ============================================================================
# Final JSON Output for UCD
# ============================================================================
printf '{"status":"OK","artifactory_url":"%s","messages":%s}\n' \
    "$UPLOAD_URL" "$MSG_COUNT"

exit 0

