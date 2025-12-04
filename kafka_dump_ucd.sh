#!/usr/bin/bash
set -euo pipefail

# ============================================================================
# Kafka Dump Utility (UCD-Compatible, Hardened Version)
# ============================================================================

BASE_DIR="/var/log/confluent/kafka_dump"
KAFKA_BIN_DIR_DEFAULT="/opt/confluent/latest/bin"
DISK_THRESHOLD=85
AUDIT_LOG="${BASE_DIR}/audit.log"
DEFAULT_CFG="/home/stekafka/config/stekafka_client.properties"

# Generate unique run ID if not provided
UCD_RUN_ID="${UCD_RUN_ID:-"$(date +%s)"}"

# ============================================================================
# Ensure cleanup on ANY failure
# ============================================================================
cleanup() {
    if [[ -d "${WORKDIR:-}" ]]; then
        rm -rf "$WORKDIR" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ============================================================================
# Read Required Environment Variables (NO UCD_ prefix)
# ============================================================================

INC="${INC:-${INC_NUMBER:-""}}"
REQ="${REQ:-${REQ_NUMBER:-""}}"
TOPIC="${TOPIC:-${KAFKA_TOPIC:-""}}"

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-${BOOTSTRAP_SERVERS:-""}}"
OTP="${OTP:-${OTP_PASSWORD:-""}}"
REQUESTOR="${REQUESTOR:-${REQUESTOR:-"ucd-user"}}"

ARTIFACTORY_BASE_URL="${ARTIFACTORY_BASE_URL:-${ARTIFACTORY_BASE_URL:-""}}"
ARTIFACTORY_USER="${ARTIFACTORY_USER:-${ARTIFACTORY_USER:-""}}"
ARTIFACTORY_PASSWORD="${ARTIFACTORY_PASSWORD:-${ARTIFACTORY_PASSWORD:-""}}"

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
# Normalize bootstrap servers – append :9094 if missing
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
# Setup directory structure & logging
# ============================================================================

mkdir -p "$BASE_DIR"
touch "$AUDIT_LOG"

WORKDIR="${BASE_DIR}/${INC}/${REQ}/${TOPIC}"
mkdir -p "$WORKDIR"
LOG_FILE="${WORKDIR}/kafka_dump.log"

log() {
    printf '{"ts":"%s","level":"%s","inc":"%s","req":"%s","topic":"%s","msg":"%s"}\n' \
        "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$1" "$INC" "$REQ" "$TOPIC" "$2" >> "$LOG_FILE"
}

log INFO "---- Kafka Dump Execution Started ----"
log INFO "Bootstrap servers normalized: ${KAFKA_BOOTSTRAP}"
log INFO "Dump directory: ${WORKDIR}"

# ============================================================================
# Disk usage check
# ============================================================================

usage_pct=$(df --output=pcent "$BASE_DIR" | tail -1 | tr -dc '0-9')
(( usage_pct >= DISK_THRESHOLD )) && fail "Disk usage ${usage_pct}% exceeds limit ${DISK_THRESHOLD}%"

# ============================================================================
# Verify Kafka binary
# ============================================================================

if ! command -v kafka-console-consumer >/dev/null 2>&1; then
    fail "kafka-console-consumer not found"
fi
KAFKA_BIN_DIR="$(dirname "$(command -v kafka-console-consumer)")"

# ============================================================================
# Validate Kafka config file
# ============================================================================

[[ ! -f "$DEFAULT_CFG" ]] && fail "Kafka client config missing: $DEFAULT_CFG"
KAFKA_SECURITY_OPTS="--consumer.config $DEFAULT_CFG"

# ============================================================================
# Kafka Dump Logic
# ============================================================================

DUMP_FILE="${WORKDIR}/${TOPIC}.jsonl"

dump_kafka() {
    local attempt=1
    while (( attempt <= 3 )); do
        log INFO "Kafka dump attempt $attempt"

        if kafka-console-consumer \
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
    fail "Kafka dump failed after all retries"
fi

# Validate output is not empty
if [[ ! -s "$DUMP_FILE" ]]; then
    fail "Kafka dump produced an empty file – aborting"
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
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF

tar -cf "${WORKDIR}/${REQ}.tar" -C "$WORKDIR" .
gzip -9 "${WORKDIR}/${REQ}.tar"

printf "%s" "$OTP" | \
gpg --batch --yes --passphrase-fd 0 --symmetric --cipher-algo AES256 \
    "${WORKDIR}/${REQ}.tar.gz"

ENC_FILE="${WORKDIR}/${REQ}.tar.gz.gpg"

# ============================================================================
# Artifactory Upload (STRICT 200/201 ONLY)
# ============================================================================

UPLOAD_URL="${ARTIFACTORY_BASE_URL%/}/kafka-dump/INC/${INC}/${REQ}/${TOPIC}/$(basename "$ENC_FILE")"

upload() {
    local attempt=1

    while (( attempt <= 3 )); do
        HTTP_CODE=$(curl -sS -w "%{http_code}" -o /dev/null \
            -u "${ARTIFACTORY_USER}:${ARTIFACTORY_PASSWORD}" \
            -X PUT -T "$ENC_FILE" "$UPLOAD_URL")

        log INFO "Upload returned HTTP code: $HTTP_CODE"

        if [[ "$HTTP_CODE" == "200" || "$HTTP_CODE" == "201" ]]; then
            return 0
        fi

        sleep $((attempt * 2))
        (( attempt++ ))
    done

    return 1
}

if ! upload; then
    fail "Upload failed – Artifactory did not return HTTP 200/201"
fi

log INFO "Upload successful to ${UPLOAD_URL}"

# ============================================================================
# Cleanup
# ============================================================================

cleanup
trap - EXIT

# ============================================================================
# Final JSON Output
# ============================================================================

printf '{"status":"OK","messages":%s,"url":"%s"}\n' "$MSG_COUNT" "$UPLOAD_URL"
exit 0
