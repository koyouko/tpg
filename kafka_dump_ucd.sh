#!/bin/bash

export INC="${p:INC_NUMBER}"
export REQ="${p:REQ_NUMBER}"
export KAFKA_BOOTSTRAP="${p:BOOTSTRAP_SERVERS}"
export TOPIC="${p:KAFKA_TOPIC}"
export OTP="${p:OTP_PASSWORD}"
export REQUESTOR="${p:REQUESTOR}"

export ARTIFACTORY_BASE_URL="${p:ARTIFACTORY_BASE_URL}"
export ARTIFACTORY_USER="${p:ARTIFACTORY_USER}"
export ARTIFACTORY_PASSWORD="${p:ARTIFACTORY_PASSWORD}"

export UCD_RUN_ID="${p:componentProcess.run.id}"

chmod +x ${p:component.workspace}/scripts/kafka_dump.sh
${p:component.workspace}/scripts/kafka_dump.sh



#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Kafka Dump Utility (UCD-Compatible)
# ============================================================================

BASE_DIR="/var/log/confluent/kafka_dump"
KAFKA_BIN_DIR_DEFAULT="/opt/confluent/latest/bin"
DISK_THRESHOLD=85
AUDIT_LOG="${BASE_DIR}/audit.log"

# Fixed Kafka consumer config (required)
DEFAULT_CFG="/home/stekafka/config/stekafka_client.properties"

# UCD run ID (optional)
UCD_RUN_ID="${UCD_RUN_ID:-"$(date +%s)"}"

# ----------------------------------------------------------------------------
# Cleanup on exit (success or failure) - removes WORKDIR only
# ----------------------------------------------------------------------------
cleanup() {
    if [[ -n "${WORKDIR:-}" && -d "$WORKDIR" ]]; then
        rm -rf "$WORKDIR" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ============================================================================
# Read environment variables exported by UCD
# ============================================================================
INC="${INC:-}"
REQ="${REQ:-}"
TOPIC="${TOPIC:-}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-}"
OTP="${OTP:-}"
REQUESTOR="${REQUESTOR:-"ucd-user"}"

ARTIFACTORY_BASE_URL="${ARTIFACTORY_BASE_URL:-}"
ARTIFACTORY_USER="${ARTIFACTORY_USER:-}"
ARTIFACTORY_PASSWORD="${ARTIFACTORY_PASSWORD:-}"

# ============================================================================
# Validation
# ============================================================================
fail() {
    printf '{"status":"ERROR","message":"%s"}\n' "$1"
    exit 1
}

[[ -z "$INC" ]] && fail "INC number missing"
[[ -z "$REQ" ]] && fail "REQ number missing"
[[ -z "$TOPIC" ]] && fail "Kafka topic missing"
[[ -z "$KAFKA_BOOTSTRAP" ]] && fail "Bootstrap server(s) missing"
[[ -z "$OTP" ]] && fail "OTP password missing"
[[ -z "$ARTIFACTORY_BASE_URL" ]] && fail "Artifactory URL missing"
[[ -z "$ARTIFACTORY_USER" ]] && fail "Artifactory user missing"
[[ -z "$ARTIFACTORY_PASSWORD" ]] && fail "Artifactory password missing"

# ============================================================================
# Normalize bootstrap servers; append :9094 if no port provided
# ============================================================================
normalize_bootstrap() {
    local input="$1"
    local output=""
    IFS=',' read -ra brokers <<< "$input"

    for broker in "${brokers[@]}"; do
        if [[ "$broker" =~ :[0-9]+$ ]]; then
            output+="$broker,"
        else
            output+="${broker}:9094,"
        fi
    done

    echo "${output%,}"
}

KAFKA_BOOTSTRAP="$(normalize_bootstrap "$KAFKA_BOOTSTRAP")"

# ============================================================================
# Setup directories and logging
# ============================================================================
mkdir -p "$BASE_DIR"
touch "$AUDIT_LOG"

WORKDIR="${BASE_DIR}/${INC}/${REQ}/${TOPIC}"
mkdir -p "$WORKDIR"

LOG_FILE="${WORKDIR}/kafka_dump.log"

log() {
    local level="$1"; shift
    local msg="$*"
    printf '{"ts":"%s","level":"%s","runId":"%s","inc":"%s","req":"%s","topic":"%s","msg":"%s"}\n' \
        "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
        "$level" "$UCD_RUN_ID" "$INC" "$REQ" "$TOPIC" "$msg" >> "$LOG_FILE"
}

log INFO "---- Kafka Dump Execution Started ----"
log INFO "Bootstrap servers: $KAFKA_BOOTSTRAP"
log INFO "Topic: $TOPIC"
log INFO "Requestor: $REQUESTOR"

# ============================================================================
# Disk usage check
# ============================================================================
usage_pct=$(df --output=pcent "$BASE_DIR" | tail -1 | tr -dc '0-9')
if (( usage_pct >= DISK_THRESHOLD )); then
    fail "Disk usage ${usage_pct}% exceeds limit (${DISK_THRESHOLD}%)"
fi

# ============================================================================
# Kafka binary lookup
# ============================================================================
KAFKA_BIN_DIR="${KAFKA_BIN_DIR:-$KAFKA_BIN_DIR_DEFAULT}"

if [[ ! -x "$KAFKA_BIN_DIR/kafka-console-consumer" ]]; then
    if command -v kafka-console-consumer >/dev/null 2>&1; then
        KAFKA_BIN_DIR="$(dirname "$(command -v kafka-console-consumer)")"
    else
        fail "kafka-console-consumer not found in PATH or $KAFKA_BIN_DIR_DEFAULT"
    fi
fi

# ============================================================================
# Verify Kafka consumer config file exists
# ============================================================================
if [[ ! -f "$DEFAULT_CFG" ]]; then
    fail "Kafka client config file missing: $DEFAULT_CFG"
fi

KAFKA_SECURITY_OPTS="--consumer.config $DEFAULT_CFG"
log INFO "Using Kafka client config: $DEFAULT_CFG"

# ============================================================================
# Kafka dump
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

        log ERROR "Kafka dump failed on attempt $attempt"
        sleep $((attempt * 2))
        (( attempt++ ))
    done

    return 1
}

if ! dump_kafka; then
    fail "Kafka dump failed after retries"
fi

if [[ ! -s "$DUMP_FILE" ]]; then
    fail "Kafka dump produced an empty file"
fi

MSG_COUNT=$(wc -l < "$DUMP_FILE")
log INFO "Dump complete — ${MSG_COUNT} messages"

# ============================================================================
# Metadata + Encryption
# ============================================================================
META_FILE="${WORKDIR}/metadata.json"
cat > "$META_FILE" <<EOF
{
  "inc": "${INC}",
  "req": "${REQ}",
  "topic": "${TOPIC}",
  "bootstrap": "${KAFKA_BOOTSTRAP}",
  "message_count": ${MSG_COUNT},
  "run_id": "${UCD_RUN_ID}",
  "requestor": "${REQUESTOR}",
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
# Artifactory upload (STRICT: only 200/201 are success)
# ============================================================================
UPLOAD_URL="${ARTIFACTORY_BASE_URL%/}/kafka-dump/INC/${INC}/${REQ}/${TOPIC}/$(basename "$ENC_FILE")"

upload() {
    local attempt=1

    while (( attempt <= 3 )); do
        log INFO "Artifactory upload attempt $attempt to $UPLOAD_URL"

        HTTP_CODE=$(curl -sS -w "%{http_code}" -o /dev/null \
            -u "${ARTIFACTORY_USER}:${ARTIFACTORY_PASSWORD}" \
            -X PUT -T "$ENC_FILE" "$UPLOAD_URL")

        log INFO "Artifactory HTTP response: $HTTP_CODE"

        if [[ "$HTTP_CODE" == "200" || "$HTTP_CODE" == "201" ]]; then
            return 0
        fi

        log ERROR "Upload failed with HTTP $HTTP_CODE on attempt $attempt"
        sleep $((attempt * 2))
        (( attempt++ ))
    done

    return 1
}

if ! upload; then
    fail "Artifactory upload failed – did not receive HTTP 200/201"
fi

log INFO "Upload successful → $UPLOAD_URL"

# ============================================================================
# Final JSON Output
# ============================================================================
# (WORKDIR is cleaned by trap after this)
printf '{"status":"OK","artifactory_url":"%s","messages":%s}\n' \
    "$UPLOAD_URL" "$MSG_COUNT"

exit 0
