#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
BASE_DIR="/var/log/confluent/kafka_dump"
KAFKA_BIN_DIR="${KAFKA_BIN_DIR:-/opt/confluent/latest/bin}"
DISK_THRESHOLD=85   # percent

AUDIT_LOG="${BASE_DIR}/audit.log"

# ---------------------------------------------------------------------------
# ARGUMENT PARSING
# ---------------------------------------------------------------------------
usage() {
cat <<EOF
Usage: $0 \\
  --inc INC1234567 \\
  --req REQ-1234 \\
  --cluster CLUSTER1 \\
  --env ENV \\
  --topic TOPICNAME \\
  --otp ONE_TIME_PASSWORD \\
  --requestor USERNAME

Environment variables required:
  ARTIFACTORY_BASE_URL
  ARTIFACTORY_USER
  ARTIFACTORY_PASSWORD
EOF
exit 1
}

INC=""
REQ=""
CLUSTER=""
ENV_NAME=""
TOPIC=""
OTP=""
REQUESTOR=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --inc)      INC="$2"; shift 2 ;;
        --req)      REQ="$2"; shift 2 ;;
        --cluster)  CLUSTER="$2"; shift 2 ;;
        --env)      ENV_NAME="$2"; shift 2 ;;
        --topic)    TOPIC="$2"; shift 2 ;;
        --otp)      OTP="$2"; shift 2 ;;
        --requestor) REQUESTOR="$2"; shift 2 ;;
        *) usage ;;
    esac
done

: "${ARTIFACTORY_BASE_URL:?Missing ARTIFACTORY_BASE_URL}"
: "${ARTIFACTORY_USER:?Missing ARTIFACTORY_USER}"
: "${ARTIFACTORY_PASSWORD:?Missing ARTIFACTORY_PASSWORD}"

if [[ -z "$INC" || -z "$REQ" || -z "$CLUSTER" || -z "$ENV_NAME" || -z "$TOPIC" || -z "$OTP" ]]; then
    usage
fi

# ---------------------------------------------------------------------------
# ENSURE BASE DIR + AUDIT LOG EXIST
# ---------------------------------------------------------------------------
mkdir -p "${BASE_DIR}" || {
    echo "ERROR: Failed to create ${BASE_DIR}" >&2
    exit 1
}
touch "${AUDIT_LOG}" || {
    echo "ERROR: Failed to create audit log at ${AUDIT_LOG}" >&2
    exit 1
}

# ---------------------------------------------------------------------------
# AUDIT LOG (PERSISTENT, NEVER DELETED)
# ---------------------------------------------------------------------------
audit_log() {
    local status="$1"; shift
    local reason="$1"; shift || true
    local url="${1:-}"

    printf '{"ts":"%s","inc":"%s","req":"%s","topic":"%s","user":"%s","status":"%s","reason":"%s","url":"%s"}\n' \
        "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
        "$INC" "$REQ" "$TOPIC" "${REQUESTOR:-unknown}" \
        "$status" "$reason" "$url" >> "${AUDIT_LOG}"
}

# ---------------------------------------------------------------------------
# DISK USAGE CHECK ON BASE_DIR MOUNT
# ---------------------------------------------------------------------------
usage_pct="$(df --output=pcent "${BASE_DIR}" 2>/dev/null | tail -1 | tr -dc '0-9' || echo 100)"

if (( usage_pct >= DISK_THRESHOLD )); then
    msg="Disk usage ${usage_pct}% >= ${DISK_THRESHOLD}% threshold for ${BASE_DIR}"
    audit_log "FAILED" "${msg}" ""
    printf '{"status":"ERROR","message":"%s"}\n' "${msg}"
    exit 1
fi

# ---------------------------------------------------------------------------
# REQUEST WORKDIR: /var/log/confluent/kafka_dump/INC/REQ/TOPIC
# ---------------------------------------------------------------------------
WORKDIR="${BASE_DIR}/${INC}/${REQ}/${TOPIC}"
mkdir -p "${WORKDIR}"

LOG_FILE="${WORKDIR}/kafka_dump.log"

# ---------------------------------------------------------------------------
# REQUEST-SCOPE JSON LOG
# ---------------------------------------------------------------------------
log() {
    local level="$1"; shift
    local msg="$*"
    printf '{"ts":"%s","level":"%s","inc":"%s","req":"%s","topic":"%s","msg":"%s"}\n' \
        "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
        "$level" "$INC" "$REQ" "$TOPIC" "$msg" >> "${LOG_FILE}"
}

# ---------------------------------------------------------------------------
# CLEANUP: REMOVE /var/log/confluent/kafka_dump/INC/REQ ON EXIT
# ---------------------------------------------------------------------------
cleanup() {
    # Remove only the INC/REQ subtree; audit.log stays at BASE_DIR
    rm -rf "${BASE_DIR}/${INC}/${REQ}" || true
}
trap cleanup EXIT

log info "Starting Kafka dump"

# ---------------------------------------------------------------------------
# FILE PATHS INSIDE WORKDIR
# ---------------------------------------------------------------------------
DUMP_FILE="${WORKDIR}/${TOPIC}.jsonl"
META_FILE="${WORKDIR}/metadata.json"
TAR_FILE="${WORKDIR}/${REQ}.tar"
GZ_FILE="${TAR_FILE}.gz"
ENC_FILE="${WORKDIR}/${REQ}.tar.gz.gpg"
CHECKSUM_FILE="${ENC_FILE}.sha256"

# ---------------------------------------------------------------------------
# RESOLVE KAFKA CONFIG
# ---------------------------------------------------------------------------
resolve_kafka_config() {
  case "${CLUSTER}:${ENV_NAME}" in
    "PHY-PROD-CL1:PROD")
      KAFKA_BOOTSTRAP="phy-prod-kafka:9093"
      KAFKA_SECURITY_OPTS="--consumer.config /opt/kafka/conf/phy-prod-consumer.properties"
      ;;
    "VM-UAT-CL2:UAT")
      KAFKA_BOOTSTRAP="vm-uat-kafka:9093"
      KAFKA_SECURITY_OPTS="--consumer.config /opt/kafka/conf/vm-uat-consumer.properties"
      ;;
    *)
      log error "Unknown cluster/env: ${CLUSTER}/${ENV_NAME}"
      audit_log "FAILED" "Unknown cluster/env: ${CLUSTER}/${ENV_NAME}" ""
      printf '{"status":"ERROR","message":"Unknown cluster/env: %s/%s"}\n' "${CLUSTER}" "${ENV_NAME}"
      exit 1
      ;;
  esac
}
resolve_kafka_config

# ---------------------------------------------------------------------------
# KAFKA DUMP WITH RETRIES
# ---------------------------------------------------------------------------
dump_kafka() {
    local attempt=1
    while (( attempt <= 3 )); do
        log info "Kafka dump attempt ${attempt}"
        if "${KAFKA_BIN_DIR}/kafka-console-consumer" \
              --bootstrap-server "${KAFKA_BOOTSTRAP}" \
              --topic "${TOPIC}" \
              --from-beginning \
              --timeout-ms 60000 \
              --property print.timestamp=true \
              --property print.offset=true \
              --property print.partition=true \
              --property print.headers=true \
              --property print.key=true \
              ${KAFKA_SECURITY_OPTS} \
              > "${DUMP_FILE}"; then
            return 0
        fi
        log error "Kafka dump attempt ${attempt} failed"
        sleep $((attempt * 2))
        ((attempt++))
    done
    return 1
}

if ! dump_kafka; then
    msg="Kafka dump failed after retries"
    log error "${msg}"
    audit_log "FAILED" "${msg}" ""
    printf '{"status":"ERROR","message":"%s"}\n' "${msg}"
    exit 1
fi

MSG_COUNT=$(wc -l < "${DUMP_FILE}" || echo 0)
log info "Kafka dump completed: ${MSG_COUNT} messages"

# ---------------------------------------------------------------------------
# CREATE metadata.json
# ---------------------------------------------------------------------------
cat > "${META_FILE}" <<EOF
{
  "inc": "${INC}",
  "req": "${REQ}",
  "cluster": "${CLUSTER}",
  "env": "${ENV_NAME}",
  "topic": "${TOPIC}",
  "created_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "created_by": "${REQUESTOR}",
  "message_count": ${MSG_COUNT}
}
EOF

# ---------------------------------------------------------------------------
# TAR + GZIP
# ---------------------------------------------------------------------------
tar -cf "${TAR_FILE}" -C "${WORKDIR}" .
gzip -9 "${TAR_FILE}"
log info "Tarball compressed"

# ---------------------------------------------------------------------------
# ENCRYPT (GPG AES256) + CHECKSUM
# ---------------------------------------------------------------------------
printf "%s" "$OTP" | gpg --batch --yes --passphrase-fd 0 --symmetric --cipher-algo AES256 "${GZ_FILE}"
sha256sum "${ENC_FILE}" > "${CHECKSUM_FILE}"
log info "Encrypted with GPG AES256 and checksum created"

# ---------------------------------------------------------------------------
# ARTIFACTORY PATH
#   kafka-dump/INC/<INC>/<REQ>/<TOPIC>/<file>
# ---------------------------------------------------------------------------
REPO_PATH="kafka-dump/INC/${INC}/${REQ}/${TOPIC}/$(basename "${ENC_FILE}")"
UPLOAD_URL="${ARTIFACTORY_BASE_URL%/}/${REPO_PATH}"

# ---------------------------------------------------------------------------
# ARTIFACTORY UPLOAD WITH RETRIES
# ---------------------------------------------------------------------------
upload() {
    local attempt=1
    while (( attempt <= 3 )); do
        log info "Artifactory upload attempt ${attempt}"
        if curl -sS -u "${ARTIFACTORY_USER}:${ARTIFACTORY_PASSWORD}" \
                -X PUT -T "${ENC_FILE}" "${UPLOAD_URL}"; then
            return 0
        fi
        log error "Artifactory upload attempt ${attempt} failed"
        sleep $((attempt * 2))
        ((attempt++))
    done
    return 1
}

if ! upload; then
    msg="Artifactory upload failed after retries"
    log error "${msg}"
    audit_log "FAILED" "${msg}" ""
    printf '{"status":"ERROR","message":"%s"}\n' "${msg}"
    exit 1
fi

log info "Upload successful: ${UPLOAD_URL}"
audit_log "SUCCESS" "" "${UPLOAD_URL}"

# ---------------------------------------------------------------------------
# FINAL OUTPUT FOR PORTAL
# (cleanup trap will remove per-request directory)
# ---------------------------------------------------------------------------
printf '{"status":"OK","artifactory_url":"%s","message_count":%s}\n' \
    "${UPLOAD_URL}" "${MSG_COUNT}"
