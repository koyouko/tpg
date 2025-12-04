#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Kafka Dump Utility (Secure, Encrypted, Audited)
# ============================================================================

BASE_DIR="/var/log/confluent/kafka_dump"
KAFKA_BIN_DIR="${KAFKA_BIN_DIR:-/opt/confluent/latest/bin}"
DISK_THRESHOLD=85
AUDIT_LOG="${BASE_DIR}/audit.log"
CLUSTER_JSON="/etc/kafka_dump/cluster_map.json"

# -------------------------
# Usage
# -------------------------
usage() {
cat <<EOF
Usage: $0 \\
  --inc INC1234567 \\
  --req REQ-1234 \\
  --bootstrap "host1:9093,host2:9093" \\
  --cluster CLUSTER_NAME (optional for JSON security config) \\
  --env ENV_NAME (optional for JSON security config) \\
  --topic TOPIC_NAME \\
  --otp ONE_TIME_PASSWORD \\
  --requestor USERNAME

Bootstrap must be provided either via:
  --bootstrap or environment variable KAFKA_BOOTSTRAP_ENV

Required ENV variables:
  ARTIFACTORY_BASE_URL
  ARTIFACTORY_USER
  ARTIFACTORY_PASSWORD
EOF
exit 1
}

# -------------------------
# Parse Arguments
# -------------------------
INC=""
REQ=""
CLUSTER=""
ENV_NAME=""
TOPIC=""
OTP=""
REQUESTOR=""
KAFKA_BOOTSTRAP=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --inc)       INC="$2"; shift 2 ;;
        --req)       REQ="$2"; shift 2 ;;
        --bootstrap) KAFKA_BOOTSTRAP="$2"; shift 2 ;;
        --cluster)   CLUSTER="$2"; shift 2 ;;
        --env)       ENV_NAME="$2"; shift 2 ;;
        --topic)     TOPIC="$2"; shift 2 ;;
        --otp)       OTP="$2"; shift 2 ;;
        --requestor) REQUESTOR="$2"; shift 2 ;;
        *) usage ;;
    esac
done

# -------------------------
# Validate Required Args
# -------------------------
if [[ -z "$INC" || -z "$REQ" || -z "$TOPIC" || -z "$OTP" ]]; then
    usage
fi

# Bootstrap resolution logic
if [[ -z "$KAFKA_BOOTSTRAP" ]]; then
    if [[ -n "${KAFKA_BOOTSTRAP_ENV:-}" ]]; then
        KAFKA_BOOTSTRAP="$KAFKA_BOOTSTRAP_ENV"
    else
        printf '{"status":"ERROR","message":"Kafka bootstrap not provided"}\n'
        exit 1
    fi
fi

# Validate env variables
: "${ARTIFACTORY_BASE_URL:?Missing ARTIFACTORY_BASE_URL}"
: "${ARTIFACTORY_USER:?Missing ARTIFACTORY_USER}"
: "${ARTIFACTORY_PASSWORD:?Missing ARTIFACTORY_PASSWORD}"

# -------------------------
# Ensure Working Directory Exists
# -------------------------
mkdir -p "$BASE_DIR"
touch "$AUDIT_LOG"

# -------------------------
# Audit Logging
# -------------------------
audit_log() {
    local status="$1"
    local reason="$2"
    local url="${3:-}"

    printf '{"ts":"%s","inc":"%s","req":"%s","topic":"%s","user":"%s","status":"%s","reason":"%s","url":"%s"}\n' \
        "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
        "$INC" "$REQ" "$TOPIC" "$REQUESTOR" \
        "$status" "$reason" "$url" >> "$AUDIT_LOG"
}

# -------------------------
# Structured Logger
# -------------------------
log() {
    local level="$1"
    shift
    local msg="$*"
    printf '{"ts":"%s","level":"%s","inc":"%s","req":"%s","topic":"%s","msg":"%s"}\n' \
        "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
        "$level" "$INC" "$REQ" "$TOPIC" "$msg" >> "$LOG_FILE"
}

# -------------------------
# Disk Usage Check
# -------------------------
usage_pct=$(df --output=pcent "$BASE_DIR" | tail -1 | tr -dc '0-9')
if (( usage_pct >= DISK_THRESHOLD )); then
    msg="Disk usage ${usage_pct}% >= ${DISK_THRESHOLD}%"
    audit_log "FAILED" "$msg"
    printf '{"status":"ERROR","message":"%s"}\n' "$msg"
    exit 1
fi

# -------------------------
# Working Directory Layout
# -------------------------
WORKDIR="${BASE_DIR}/${INC}/${REQ}/${TOPIC}"
mkdir -p "$WORKDIR"
LOG_FILE="${WORKDIR}/kafka_dump.log"

log INFO "Starting Kafka dump for topic: $TOPIC"
log INFO "Using bootstrap: $KAFKA_BOOTSTRAP"

# -------------------------
# Optional JSON Security Config Loader
# -------------------------
resolve_security_config() {

    if [[ ! -f "$CLUSTER_JSON" ]]; then
        log INFO "cluster_map.json not found — skipping security config resolution"
        KAFKA_SECURITY_OPTS=""
        return
    fi

    # Security config path lookup in JSON
    local cfg=""
    cfg=$(jq -r ".\"${CLUSTER}\".\"${ENV_NAME}\".config" "$CLUSTER_JSON" 2>/dev/null || echo "null")

    if [[ "$cfg" == "null" ]]; then
        log INFO "No security config found for ${CLUSTER}/${ENV_NAME}"
        KAFKA_SECURITY_OPTS=""
    else
        KAFKA_SECURITY_OPTS="--consumer.config ${cfg}"
        log INFO "Using security config: $cfg"
    fi
}

resolve_security_config

# -------------------------
# Kafka Dump
# -------------------------
DUMP_FILE="${WORKDIR}/${TOPIC}.jsonl"
META_FILE="${WORKDIR}/metadata.json"
TAR_FILE="${WORKDIR}/${REQ}.tar"
GZ_FILE="${TAR_FILE}.gz"
ENC_FILE="${WORKDIR}/${REQ}.tar.gz.gpg"
CHECKSUM_FILE="${ENC_FILE}.sha256"

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
    msg="Kafka dump failed after 3 retries"
    audit_log "FAILED" "$msg"
    log ERROR "$msg"
    printf '{"status":"ERROR","message":"%s"}\n' "$msg"
    exit 1
fi

MSG_COUNT=$(wc -l < "$DUMP_FILE")
log INFO "Dump complete: $MSG_COUNT messages"

# -------------------------
# Metadata
# -------------------------
cat > "$META_FILE" <<EOF
{
  "inc": "${INC}",
  "req": "${REQ}",
  "topic": "${TOPIC}",
  "bootstrap": "${KAFKA_BOOTSTRAP}",
  "cluster": "${CLUSTER}",
  "env": "${ENV_NAME}",
  "created_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "created_by": "${REQUESTOR}",
  "message_count": ${MSG_COUNT}
}
EOF

log INFO "metadata.json created"

# -------------------------
# Tar + Gzip
# -------------------------
tar -cf "$TAR_FILE" -C "$WORKDIR" .
gzip -9 "$TAR_FILE"
log INFO "Tarball created"

# -------------------------
# Encrypt (AES-256)
# -------------------------
printf "%s" "$OTP" | gpg --batch --yes --passphrase-fd 0 \
    --symmetric --cipher-algo AES256 "$GZ_FILE"

sha256sum "$ENC_FILE" > "$CHECKSUM_FILE"

log INFO "File encrypted + checksum created"

# -------------------------
# Upload to Artifactory (strict: only 200/201 accepted)
# -------------------------
REPO_PATH="kafka-dump/INC/${INC}/${REQ}/${TOPIC}/$(basename "$ENC_FILE")"
UPLOAD_URL="${ARTIFACTORY_BASE_URL%/}/${REPO_PATH}"

upload() {
    local attempt=1
    while (( attempt <= 3 )); do
        log INFO "Upload attempt $attempt → $UPLOAD_URL"

        HTTP_CODE=$(curl -s -w "%{http_code}" -o /dev/null \
            -u "${ARTIFACTORY_USER}:${ARTIFACTORY_PASSWORD}" \
            -X PUT -T "$ENC_FILE" "$UPLOAD_URL")

        if [[ "$HTTP_CODE" == "200" || "$HTTP_CODE" == "201" ]]; then
            log INFO "Upload succeeded (HTTP $HTTP_CODE)"
            return 0
        else
            log ERROR "Upload failed (HTTP $HTTP_CODE)"
        fi

        sleep $((attempt * 2))
        (( attempt++ ))
    done
    return 1
}

if ! upload; then
    msg="Artifactory upload failed after 3 retries"
    audit_log "FAILED" "$msg"
    log ERROR "$msg"
    printf '{"status":"ERROR","message":"%s"}\n' "$msg"
    exit 1
fi

audit_log "SUCCESS" "" "$UPLOAD_URL"
log INFO "Upload successful: $UPLOAD_URL"

# -------------------------
# Final Output
# -------------------------
printf '{"status":"OK","artifactory_url":"%s","message_count":%s}\n' \
    "$UPLOAD_URL" "$MSG_COUNT"

log INFO "Process completed successfully"
exit 0
