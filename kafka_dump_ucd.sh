#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/var/log/confluent/kafka_dump"
KAFKA_BIN_DIR_DEFAULT="/opt/confluent/latest/bin"
DISK_THRESHOLD=85
MAX_DUMP_SIZE_MB=5000          # 5GB max dump size
KAFKA_TIMEOUT_MS=120000        # 2 minutes default
CURL_TIMEOUT=300               # 5 minutes for upload
AUDIT_LOG="${BASE_DIR}/audit.log"

# Fixed Kafka consumer config (required)
DEFAULT_CFG="/home/stekafka/config/stekafka_client.properties"

# UCD run ID (optional)
UCD_RUN_ID="${UCD_RUN_ID:-"$(date +%s)"}"

# Track sensitive files for secure cleanup
declare -a SENSITIVE_FILES=()

# ----------------------------------------------------------------------------
# Cleanup on exit (success or failure)
# ----------------------------------------------------------------------------
cleanup() {
    local exit_code=$?
    
    # Securely remove sensitive intermediate files
    for f in "${SENSITIVE_FILES[@]:-}"; do
        if [[ -f "$f" ]]; then
            # Overwrite before deletion for sensitive data
            shred -u "$f" 2>/dev/null || rm -f "$f" 2>/dev/null || true
        fi
    done
    
    # Remove workdir
    if [[ -n "${WORKDIR:-}" && -d "$WORKDIR" ]]; then
        rm -rf "$WORKDIR" 2>/dev/null || true
    fi
    
    # Remove temporary credential files
    if [[ -n "${CURL_HEADER_FILE:-}" && -f "$CURL_HEADER_FILE" ]]; then
        shred -u "$CURL_HEADER_FILE" 2>/dev/null || rm -f "$CURL_HEADER_FILE" 2>/dev/null || true
    fi
    
    exit "$exit_code"
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
KAFKA_TIMEOUT_MS="${KAFKA_TIMEOUT_MS:-$KAFKA_TIMEOUT_MS}"

ARTIFACTORY_BASE_URL="${ARTIFACTORY_BASE_URL:-}"
ARTIFACTORY_USER="${ARTIFACTORY_USER:-}"
ARTIFACTORY_PASSWORD="${ARTIFACTORY_PASSWORD:-}"

# ============================================================================
# JSON-safe string escaping
# ============================================================================
json_escape() {
    local str="$1"
    str="${str//\\/\\\\}"      # Escape backslashes first
    str="${str//\"/\\\"}"      # Escape double quotes
    str="${str//$'\n'/\\n}"    # Escape newlines
    str="${str//$'\r'/\\r}"    # Escape carriage returns
    str="${str//$'\t'/\\t}"    # Escape tabs
    printf '%s' "$str"
}

# ============================================================================
# Validation
# ============================================================================
fail() {
    local msg="$1"
    
    # Log to file if available
    if [[ -n "${LOG_FILE:-}" ]]; then
        log ERROR "$msg"
    fi
    
    # Log to audit log
    if [[ -f "$AUDIT_LOG" ]]; then
        printf '{"ts":"%s","event":"FAILURE","inc":"%s","req":"%s","topic":"%s","msg":"%s"}\n' \
            "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
            "$(json_escape "${INC:-}")" \
            "$(json_escape "${REQ:-}")" \
            "$(json_escape "${TOPIC:-}")" \
            "$(json_escape "$msg")" >> "$AUDIT_LOG"
    fi
    
    printf '{"status":"ERROR","message":"%s"}\n' "$(json_escape "$msg")"
    exit 1
}

# Validate required parameters
[[ -z "$INC" ]] && fail "INC number missing"
[[ -z "$REQ" ]] && fail "REQ number missing"
[[ -z "$TOPIC" ]] && fail "Kafka topic missing"
[[ -z "$KAFKA_BOOTSTRAP" ]] && fail "Bootstrap server(s) missing"
[[ -z "$OTP" ]] && fail "OTP password missing"
[[ -z "$ARTIFACTORY_BASE_URL" ]] && fail "Artifactory URL missing"
[[ -z "$ARTIFACTORY_USER" ]] && fail "Artifactory user missing"
[[ -z "$ARTIFACTORY_PASSWORD" ]] && fail "Artifactory password missing"

# Validate INC/REQ format (prevent path traversal)
if [[ ! "$INC" =~ ^[A-Za-z0-9_-]+$ ]]; then
    fail "INC contains invalid characters"
fi
if [[ ! "$REQ" =~ ^[A-Za-z0-9_-]+$ ]]; then
    fail "REQ contains invalid characters"
fi
if [[ ! "$TOPIC" =~ ^[A-Za-z0-9._-]+$ ]]; then
    fail "TOPIC contains invalid characters"
fi

# ============================================================================
# Normalize bootstrap servers; append :9094 if no port provided
# ============================================================================
normalize_bootstrap() {
    local input="$1"
    local output=""
    local broker
    
    IFS=',' read -ra brokers <<< "$input"

    for broker in "${brokers[@]}"; do
        # Trim whitespace
        broker="${broker#"${broker%%[![:space:]]*}"}"
        broker="${broker%"${broker##*[![:space:]]}"}"
        
        if [[ -z "$broker" ]]; then
            continue
        fi
        
        if [[ "$broker" =~ :[0-9]+$ ]]; then
            output+="$broker,"
        else
            output+="${broker}:9094,"
        fi
    done

    echo "${output%,}"
}

KAFKA_BOOTSTRAP="$(normalize_bootstrap "$KAFKA_BOOTSTRAP")"

if [[ -z "$KAFKA_BOOTSTRAP" ]]; then
    fail "No valid bootstrap servers after normalization"
fi

# ============================================================================
# Setup directories and logging
# ============================================================================
mkdir -p "$BASE_DIR"
touch "$AUDIT_LOG"
chmod 600 "$AUDIT_LOG"

WORKDIR="${BASE_DIR}/${INC}/${REQ}/${TOPIC}"
mkdir -p "$WORKDIR"
chmod 700 "$WORKDIR"

LOG_FILE="${WORKDIR}/kafka_dump.log"

log() {
    local level="$1"; shift
    local msg="$*"
    printf '{"ts":"%s","level":"%s","runId":"%s","inc":"%s","req":"%s","topic":"%s","msg":"%s"}\n' \
        "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
        "$level" \
        "$(json_escape "$UCD_RUN_ID")" \
        "$(json_escape "$INC")" \
        "$(json_escape "$REQ")" \
        "$(json_escape "$TOPIC")" \
        "$(json_escape "$msg")" >> "$LOG_FILE"
}

audit() {
    local event="$1"; shift
    local details="$*"
    printf '{"ts":"%s","event":"%s","inc":"%s","req":"%s","topic":"%s","requestor":"%s","runId":"%s","details":"%s"}\n' \
        "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
        "$(json_escape "$event")" \
        "$(json_escape "$INC")" \
        "$(json_escape "$REQ")" \
        "$(json_escape "$TOPIC")" \
        "$(json_escape "$REQUESTOR")" \
        "$(json_escape "$UCD_RUN_ID")" \
        "$(json_escape "$details")" >> "$AUDIT_LOG"
}

log INFO "---- Kafka Dump Execution Started ----"
log INFO "Bootstrap servers: $KAFKA_BOOTSTRAP"
log INFO "Topic: $TOPIC"
log INFO "Requestor: $REQUESTOR"
audit "START" "Kafka dump initiated"

# ============================================================================
# Disk usage check
# ============================================================================
usage_pct=$(df --output=pcent "$BASE_DIR" | tail -1 | tr -dc '0-9')
if (( usage_pct >= DISK_THRESHOLD )); then
    fail "Disk usage ${usage_pct}% exceeds limit (${DISK_THRESHOLD}%)"
fi
log INFO "Disk usage: ${usage_pct}%"

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

log INFO "Using Kafka binaries from: $KAFKA_BIN_DIR"

# ============================================================================
# Verify Kafka consumer config file exists
# ============================================================================
if [[ ! -f "$DEFAULT_CFG" ]]; then
    fail "Kafka client config file missing: $DEFAULT_CFG"
fi

# Use array for proper handling of arguments with spaces
KAFKA_SECURITY_OPTS=("--consumer.config" "$DEFAULT_CFG")
log INFO "Using Kafka client config: $DEFAULT_CFG"

# ============================================================================
# Kafka dump with size monitoring
# ============================================================================
DUMP_FILE="${WORKDIR}/${TOPIC}.jsonl"
SENSITIVE_FILES+=("$DUMP_FILE")

dump_kafka() {
    local attempt=1
    local max_attempts=3
    local max_size_bytes=$((MAX_DUMP_SIZE_MB * 1024 * 1024))
    
    while (( attempt <= max_attempts )); do
        log INFO "Kafka dump attempt $attempt of $max_attempts"

        # Run consumer with timeout
        if "$KAFKA_BIN_DIR/kafka-console-consumer" \
            --bootstrap-server "$KAFKA_BOOTSTRAP" \
            --topic "$TOPIC" \
            --from-beginning \
            --timeout-ms "$KAFKA_TIMEOUT_MS" \
            --property print.timestamp=true \
            --property print.offset=true \
            --property print.partition=true \
            --property print.headers=true \
            --property print.key=true \
            "${KAFKA_SECURITY_OPTS[@]}" > "$DUMP_FILE" 2>> "$LOG_FILE"; then
            
            # Check file size
            local file_size
            file_size=$(stat -c%s "$DUMP_FILE" 2>/dev/null || echo "0")
            
            if (( file_size > max_size_bytes )); then
                log ERROR "Dump file exceeds max size (${file_size} > ${max_size_bytes} bytes)"
                rm -f "$DUMP_FILE"
                fail "Dump file exceeds maximum allowed size of ${MAX_DUMP_SIZE_MB}MB"
            fi
            
            log INFO "Dump file size: ${file_size} bytes"
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

# Use awk to avoid whitespace issues with wc
MSG_COUNT=$(awk 'END {print NR}' "$DUMP_FILE")
DUMP_SIZE=$(stat -c%s "$DUMP_FILE")
log INFO "Dump complete — ${MSG_COUNT} messages, ${DUMP_SIZE} bytes"
audit "DUMP_COMPLETE" "messages=${MSG_COUNT}, size=${DUMP_SIZE}"

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
  "dump_size_bytes": ${DUMP_SIZE},
  "run_id": "${UCD_RUN_ID}",
  "requestor": "${REQUESTOR}",
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "script_version": "2.0.0"
}
EOF

TAR_FILE="${WORKDIR}/${REQ}.tar"
GZ_FILE="${TAR_FILE}.gz"
ENC_FILE="${WORKDIR}/${REQ}.tar.gz.gpg"
CHECKSUM_FILE="${WORKDIR}/${REQ}.tar.gz.gpg.sha256"

# Track intermediate sensitive files for cleanup
SENSITIVE_FILES+=("$TAR_FILE" "$GZ_FILE")

log INFO "Creating archive..."
tar -cf "$TAR_FILE" -C "$WORKDIR" "$(basename "$DUMP_FILE")" "$(basename "$META_FILE")"

log INFO "Compressing archive..."
gzip -9 "$TAR_FILE"

log INFO "Encrypting archive..."
if ! printf "%s" "$OTP" | gpg --batch --yes --passphrase-fd 0 \
    --symmetric --cipher-algo AES256 \
    --output "$ENC_FILE" "$GZ_FILE" 2>> "$LOG_FILE"; then
    fail "GPG encryption failed"
fi

# Verify encrypted file exists and has content
if [[ ! -s "$ENC_FILE" ]]; then
    fail "Encryption produced empty file"
fi

# Generate SHA256 checksum
sha256sum "$ENC_FILE" | awk '{print $1}' > "$CHECKSUM_FILE"
CHECKSUM=$(cat "$CHECKSUM_FILE")
log INFO "SHA256 checksum: $CHECKSUM"

# Remove unencrypted files immediately after encryption
rm -f "$GZ_FILE" 2>/dev/null || true

ENC_SIZE=$(stat -c%s "$ENC_FILE")
log INFO "Encrypted file size: ${ENC_SIZE} bytes"

# ============================================================================
# Artifactory upload (STRICT: only 200/201 are success)
# ============================================================================
UPLOAD_PATH="kafka-dump/INC/${INC}/${REQ}/${TOPIC}"
UPLOAD_URL="${ARTIFACTORY_BASE_URL%/}/${UPLOAD_PATH}/$(basename "$ENC_FILE")"
CHECKSUM_URL="${ARTIFACTORY_BASE_URL%/}/${UPLOAD_PATH}/$(basename "$CHECKSUM_FILE")"

# Create secure header file for curl (avoids exposing credentials in process list)
CURL_HEADER_FILE=$(mktemp)
chmod 600 "$CURL_HEADER_FILE"
printf "Authorization: Basic %s\n" "$(printf '%s:%s' "$ARTIFACTORY_USER" "$ARTIFACTORY_PASSWORD" | base64)" > "$CURL_HEADER_FILE"

upload_file() {
    local src_file="$1"
    local dest_url="$2"
    local attempt=1
    local max_attempts=3
    local http_code
    local response_body
    local temp_response
    
    temp_response=$(mktemp)
    
    while (( attempt <= max_attempts )); do
        log INFO "Upload attempt $attempt of $max_attempts to $dest_url"

        http_code=$(curl -sS \
            --connect-timeout 30 \
            --max-time "$CURL_TIMEOUT" \
            -w "%{http_code}" \
            -o "$temp_response" \
            -H @"$CURL_HEADER_FILE" \
            -H "X-Checksum-Sha256: $CHECKSUM" \
            -X PUT \
            -T "$src_file" \
            "$dest_url" 2>> "$LOG_FILE")

        log INFO "HTTP response code: $http_code"

        if [[ "$http_code" == "200" || "$http_code" == "201" ]]; then
            rm -f "$temp_response"
            return 0
        fi

        # Log response body for debugging (truncated)
        if [[ -s "$temp_response" ]]; then
            response_body=$(head -c 500 "$temp_response")
            log ERROR "Response body: $response_body"
        fi

        log ERROR "Upload failed with HTTP $http_code on attempt $attempt"
        sleep $((attempt * 2))
        (( attempt++ ))
    done

    rm -f "$temp_response"
    return 1
}

# Upload encrypted file
if ! upload_file "$ENC_FILE" "$UPLOAD_URL"; then
    fail "Artifactory upload failed for encrypted file – did not receive HTTP 200/201"
fi
log INFO "Encrypted file uploaded successfully"
audit "UPLOAD_COMPLETE" "url=$UPLOAD_URL"

# Upload checksum file
if ! upload_file "$CHECKSUM_FILE" "$CHECKSUM_URL"; then
    log WARN "Checksum file upload failed (non-fatal)"
else
    log INFO "Checksum file uploaded successfully"
fi

# ============================================================================
# Final JSON Output and Audit
# ============================================================================
audit "SUCCESS" "Kafka dump completed successfully"

printf '{"status":"OK","artifactory_url":"%s","messages":%s,"size_bytes":%s,"sha256":"%s"}\n' \
    "$(json_escape "$UPLOAD_URL")" \
    "$MSG_COUNT" \
    "$ENC_SIZE" \
    "$CHECKSUM"

exit 0
