#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="/var/log/confluent/kafka_dump"
KAFKA_BIN_DIR_DEFAULT="/opt/confluent/latest/bin"
DISK_THRESHOLD=85
MAX_DUMP_SIZE_MB=5000          # 5GB max dump size
KAFKA_TIMEOUT_MS_DEFAULT=120000 # 2 minutes default
CURL_TIMEOUT=300               # 5 minutes for upload
AUDIT_LOG="${BASE_DIR}/audit.log"

# Fixed Kafka consumer config (required)
DEFAULT_CFG="/home/stekafka/config/stekafka_client.properties"

# Track sensitive files for secure cleanup
declare -a SENSITIVE_FILES=()

# Track temp files for cleanup on exit (e.g. upload response bodies)
declare -a TEMP_FILES=()

# ============================================================================
# Parse named parameters from CLI
# ============================================================================
usage() {
    cat <<USAGE
Usage: $(basename "$0") [OPTIONS]

Required:
  --inc               <string>   Incident number
  --req               <string>   Request number
  --topic             <string>   Kafka topic
  --bootstrap         <string>   Bootstrap server(s), comma-separated
  --otp               <string>   OTP password for GPG encryption
  --artifactory-url   <string>   Artifactory base URL
  --artifactory-user  <string>   Artifactory username
  --artifactory-pass  <string>   Artifactory password

Optional:
  --requestor         <string>   Requestor name (default: ucd-user)
  --run-id            <string>   UCD run ID (default: epoch seconds)
  --soeid             <string>   SOE ID (used for email TO: SOEID@citi.com)
  --email-cc          <string>   CC address for notification email
  --timeout-ms        <int>      Kafka consumer timeout in ms (default: 120000)
  --max-messages      <int>      Max messages to consume (default: unlimited)
  --wall-timeout      <int>      Hard wall-clock timeout in seconds (default: 600)
  -h, --help                     Show this help

USAGE
    exit 1
}

# Defaults
INC=""
REQ=""
TOPIC=""
KAFKA_BOOTSTRAP=""
OTP=""
REQUESTOR="ucd-user"
KAFKA_TIMEOUT_MS="$KAFKA_TIMEOUT_MS_DEFAULT"
ARTIFACTORY_BASE_URL=""
ARTIFACTORY_USER=""
ARTIFACTORY_PASSWORD=""
UCD_RUN_ID="$(date +%s)"
SOEID=""
EMAIL_CC="dl.icg.global.kafka.ste.admin@imcnam.ssmb.com"
MAX_MESSAGES=""
WALL_TIMEOUT=600

while [[ $# -gt 0 ]]; do
    case "$1" in
        --inc)               INC="$2";                shift 2 ;;
        --req)               REQ="$2";                shift 2 ;;
        --topic)             TOPIC="$2";              shift 2 ;;
        --bootstrap)         KAFKA_BOOTSTRAP="$2";    shift 2 ;;
        --otp)               OTP="$2";                shift 2 ;;
        --requestor)         REQUESTOR="$2";          shift 2 ;;
        --artifactory-url)   ARTIFACTORY_BASE_URL="$2"; shift 2 ;;
        --artifactory-user)  ARTIFACTORY_USER="$2";   shift 2 ;;
        --artifactory-pass)  ARTIFACTORY_PASSWORD="$2"; shift 2 ;;
        --run-id)            UCD_RUN_ID="$2";         shift 2 ;;
        --soeid)             SOEID="$2";              shift 2 ;;
        --email-cc)          EMAIL_CC="$2";           shift 2 ;;
        --timeout-ms)        KAFKA_TIMEOUT_MS="$2";   shift 2 ;;
        --max-messages)      MAX_MESSAGES="$2";       shift 2 ;;
        --wall-timeout)      WALL_TIMEOUT="$2";       shift 2 ;;
        -h|--help)           usage ;;
        *)                   echo "Unknown option: $1" >&2; usage ;;
    esac
done

# ----------------------------------------------------------------------------
# Cleanup on exit (success or failure)
# ----------------------------------------------------------------------------
cleanup() {
    local exit_code=$?
    
    # Securely remove sensitive intermediate files
    for f in "${SENSITIVE_FILES[@]:-}"; do
        if [[ -f "$f" ]]; then
            shred -u "$f" 2>/dev/null || rm -f "$f" 2>/dev/null || true
        fi
    done
    
    # Remove temp files (e.g. upload_file response bodies)
    for f in "${TEMP_FILES[@]:-}"; do
        if [[ -f "$f" ]]; then
            rm -f "$f" 2>/dev/null || true
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
# JSON-safe string escaping
# ============================================================================
json_escape() {
    local str="$1"
    str="${str//\\/\\\\}"
    str="${str//\"/\\\"}"
    str="${str//$'\n'/\\n}"
    str="${str//$'\r'/\\r}"
    str="${str//$'\t'/\\t}"
    printf '%s' "$str"
}

# ============================================================================
# Validation
# ============================================================================
fail() {
    local msg="$1"
    
    if [[ -n "${LOG_FILE:-}" ]]; then
        log ERROR "$msg"
    fi
    
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
[[ -z "$INC" ]]                && fail "INC number missing (use --inc)"
[[ -z "$REQ" ]]                && fail "REQ number missing (use --req)"
[[ -z "$TOPIC" ]]              && fail "Kafka topic missing (use --topic)"
[[ -z "$KAFKA_BOOTSTRAP" ]]    && fail "Bootstrap server(s) missing (use --bootstrap)"
[[ -z "$OTP" ]]                && fail "OTP password missing (use --otp)"
[[ -z "$ARTIFACTORY_BASE_URL" ]] && fail "Artifactory URL missing (use --artifactory-url)"
[[ -z "$ARTIFACTORY_USER" ]]   && fail "Artifactory user missing (use --artifactory-user)"
[[ -z "$ARTIFACTORY_PASSWORD" ]] && fail "Artifactory password missing (use --artifactory-pass)"

# Validate format (prevent path traversal)
if [[ ! "$INC" =~ ^[A-Za-z0-9_-]+$ ]]; then
    fail "INC contains invalid characters"
fi
if [[ ! "$REQ" =~ ^[A-Za-z0-9_-]+$ ]]; then
    fail "REQ contains invalid characters"
fi
if [[ ! "$TOPIC" =~ ^[A-Za-z0-9._-]+$ ]]; then
    fail "TOPIC contains invalid characters"
fi
if [[ ! "$REQUESTOR" =~ ^[A-Za-z0-9._@-]+$ ]]; then
    fail "REQUESTOR contains invalid characters"
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

WORKDIR="${BASE_DIR}/${INC}/${REQ}/${TOPIC}/${UCD_RUN_ID}"
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
[[ -n "$SOEID" ]] && log INFO "SOEID: $SOEID"
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

KAFKA_SECURITY_OPTS=("--consumer.config" "$DEFAULT_CFG")
log INFO "Using Kafka client config: $DEFAULT_CFG"

# ============================================================================
# Kafka dump with size monitoring + wall-clock timeout
# ============================================================================
DUMP_FILE="${WORKDIR}/${TOPIC}.jsonl"
SENSITIVE_FILES+=("$DUMP_FILE")

dump_kafka() {
    local attempt=1
    local max_attempts=3
    local max_size_bytes=$((MAX_DUMP_SIZE_MB * 1024 * 1024))
    
    # Build consumer args
    local -a consumer_args=(
        --bootstrap-server "$KAFKA_BOOTSTRAP"
        --topic "$TOPIC"
        --from-beginning
        --timeout-ms "$KAFKA_TIMEOUT_MS"
        --property print.timestamp=true
        --property print.offset=true
        --property print.partition=true
        --property print.headers=true
        --property print.key=true
        "${KAFKA_SECURITY_OPTS[@]}"
    )
    
    # Add --max-messages if specified
    if [[ -n "$MAX_MESSAGES" ]]; then
        consumer_args+=(--max-messages "$MAX_MESSAGES")
        log INFO "Max messages cap: $MAX_MESSAGES"
    fi

    while (( attempt <= max_attempts )); do
        log INFO "Kafka dump attempt $attempt of $max_attempts (wall timeout: ${WALL_TIMEOUT}s)"

        local exit_status=0
        timeout "$WALL_TIMEOUT" \
            "$KAFKA_BIN_DIR/kafka-console-consumer" \
            "${consumer_args[@]}" > "$DUMP_FILE" 2>> "$LOG_FILE" || exit_status=$?

        if (( exit_status == 124 )); then
            log WARN "Consumer hit wall-clock timeout (${WALL_TIMEOUT}s) — using partial dump"
            # Treat as partial success; continue with whatever was captured
        elif (( exit_status != 0 )); then
            log ERROR "Kafka dump failed on attempt $attempt (exit code: $exit_status)"
            sleep $((attempt * 2))
            (( attempt++ ))
            continue
        fi

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
    done

    return 1
}

if ! dump_kafka; then
    fail "Kafka dump failed after retries"
fi

if [[ ! -s "$DUMP_FILE" ]]; then
    fail "Kafka dump produced an empty file"
fi

MSG_COUNT=$(awk 'END {print NR}' "$DUMP_FILE")
DUMP_SIZE=$(stat -c%s "$DUMP_FILE")
log INFO "Dump complete - ${MSG_COUNT} messages, ${DUMP_SIZE} bytes"
audit "DUMP_COMPLETE" "messages=${MSG_COUNT}, size=${DUMP_SIZE}"

# ============================================================================
# Metadata + Encryption
# ============================================================================
META_FILE="${WORKDIR}/metadata.json"
meta_inc=$(json_escape "$INC")
meta_req=$(json_escape "$REQ")
meta_topic=$(json_escape "$TOPIC")
meta_bootstrap=$(json_escape "$KAFKA_BOOTSTRAP")
meta_run_id=$(json_escape "$UCD_RUN_ID")
meta_requestor=$(json_escape "$REQUESTOR")
meta_timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
cat > "$META_FILE" <<EOF
{
  "inc": "${meta_inc}",
  "req": "${meta_req}",
  "topic": "${meta_topic}",
  "bootstrap": "${meta_bootstrap}",
  "message_count": ${MSG_COUNT},
  "dump_size_bytes": ${DUMP_SIZE},
  "run_id": "${meta_run_id}",
  "requestor": "${meta_requestor}",
  "timestamp": "${meta_timestamp}",
  "script_version": "3.0.0"
}
EOF

TAR_FILE="${WORKDIR}/${REQ}.tar"
GZ_FILE="${TAR_FILE}.gz"
ENC_FILE="${WORKDIR}/${REQ}.tar.gz.gpg"
CHECKSUM_FILE="${WORKDIR}/${REQ}.tar.gz.gpg.sha256"

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

if [[ ! -s "$ENC_FILE" ]]; then
    fail "Encryption produced empty file"
fi

sha256sum "$ENC_FILE" | awk '{print $1}' > "$CHECKSUM_FILE"
CHECKSUM=$(cat "$CHECKSUM_FILE")
log INFO "SHA256 checksum: $CHECKSUM"

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
    local artifact_type="${3:-artifact}"
    local attempt=1
    local max_attempts=3
    local http_code
    local response_body
    local temp_response

    temp_response=$(mktemp)
    TEMP_FILES+=("$temp_response")

    local -a curl_headers=(-H @"$CURL_HEADER_FILE")
    if [[ "$artifact_type" != "checksum" ]]; then
        curl_headers+=(-H "X-Checksum-Sha256: $CHECKSUM")
    fi

    while (( attempt <= max_attempts )); do
        log INFO "Upload attempt $attempt of $max_attempts to $dest_url"

        http_code=$(curl -sS \
            --connect-timeout 30 \
            --max-time "$CURL_TIMEOUT" \
            -w "%{http_code}" \
            -o "$temp_response" \
            "${curl_headers[@]}" \
            -X PUT \
            -T "$src_file" \
            "$dest_url" 2>> "$LOG_FILE")

        log INFO "HTTP response code: $http_code"

        if [[ "$http_code" == "200" || "$http_code" == "201" ]]; then
            return 0
        fi

        if [[ -s "$temp_response" ]]; then
            response_body=$(head -c 500 "$temp_response")
            log ERROR "Response body: $response_body"
        fi

        log ERROR "Upload failed with HTTP $http_code on attempt $attempt"
        sleep $((attempt * 2))
        (( attempt++ ))
    done

    return 1
}

if ! upload_file "$ENC_FILE" "$UPLOAD_URL"; then
    fail "Artifactory upload failed for encrypted file – did not receive HTTP 200/201"
fi
log INFO "Encrypted file uploaded successfully"
audit "UPLOAD_COMPLETE" "url=$UPLOAD_URL"

if ! upload_file "$CHECKSUM_FILE" "$CHECKSUM_URL" "checksum"; then
    log WARN "Checksum file upload failed (non-fatal)"
else
    log INFO "Checksum file uploaded successfully"
fi

# ============================================================================
# Email Notification (HTML for Outlook)
# ============================================================================
send_notification() {
    local to_addr="$1"
    local cc_addr="$2"
    local subject="$3"
    local status_label="$4"
    local status_color="$5"

    local timestamp
    timestamp=$(date -u +"%Y-%m-%d %H:%M:%S UTC")

    local human_size
    if (( ENC_SIZE >= 1073741824 )); then
        human_size="$(awk "BEGIN {printf \"%.2f GB\", ${ENC_SIZE}/1073741824}")"
    elif (( ENC_SIZE >= 1048576 )); then
        human_size="$(awk "BEGIN {printf \"%.2f MB\", ${ENC_SIZE}/1048576}")"
    elif (( ENC_SIZE >= 1024 )); then
        human_size="$(awk "BEGIN {printf \"%.1f KB\", ${ENC_SIZE}/1024}")"
    else
        human_size="${ENC_SIZE} bytes"
    fi

    local email_body
    read -r -d '' email_body <<'HTMLEOF' || true
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<!--[if mso]>
<style>table{border-collapse:collapse;}td{padding:0;}</style>
<![endif]-->
</head>
<body style="margin:0;padding:0;background-color:#f4f5f7;font-family:Segoe UI,Calibri,Arial,sans-serif;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f5f7;padding:24px 0;">
<tr><td align="center">
<table role="presentation" width="620" cellpadding="0" cellspacing="0" style="background-color:#ffffff;border-radius:8px;border:1px solid #dfe1e6;max-width:620px;width:100%;">

  <!-- Header banner -->
  <tr>
    <td style="background-color:#003A70;padding:20px 32px;border-radius:8px 8px 0 0;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td style="color:#ffffff;font-size:20px;font-weight:600;letter-spacing:0.3px;">
            &#9670; Kafka STP &mdash; Topic Dump
          </td>
          <td align="right" style="color:__STATUS_COLOR__;font-size:13px;font-weight:700;background-color:#ffffff;padding:4px 14px;border-radius:4px;">
            __STATUS_LABEL__
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- Summary line -->
  <tr>
    <td style="padding:20px 32px 8px 32px;font-size:14px;color:#505F79;line-height:1.6;">
      The Kafka topic dump for <strong>__TOPIC__</strong> completed at <strong>__TIMESTAMP__</strong>.
    </td>
  </tr>

  <!-- Details table -->
  <tr>
    <td style="padding:12px 32px;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
             style="border:1px solid #dfe1e6;border-radius:6px;border-collapse:separate;">
        <tr style="background-color:#f4f5f7;">
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;width:160px;border-bottom:1px solid #dfe1e6;">INCIDENT</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;border-bottom:1px solid #dfe1e6;">__INC__</td>
        </tr>
        <tr>
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;border-bottom:1px solid #dfe1e6;">REQUEST</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;border-bottom:1px solid #dfe1e6;">__REQ__</td>
        </tr>
        <tr style="background-color:#f4f5f7;">
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;border-bottom:1px solid #dfe1e6;">TOPIC</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;border-bottom:1px solid #dfe1e6;font-family:Consolas,Courier New,monospace;">__TOPIC__</td>
        </tr>
        <tr>
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;border-bottom:1px solid #dfe1e6;">BOOTSTRAP</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;border-bottom:1px solid #dfe1e6;font-family:Consolas,Courier New,monospace;">__BOOTSTRAP__</td>
        </tr>
        <tr style="background-color:#f4f5f7;">
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;border-bottom:1px solid #dfe1e6;">MESSAGES</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;border-bottom:1px solid #dfe1e6;font-weight:600;">__MSG_COUNT__</td>
        </tr>
        <tr>
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;border-bottom:1px solid #dfe1e6;">ENCRYPTED SIZE</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;border-bottom:1px solid #dfe1e6;">__HUMAN_SIZE__</td>
        </tr>
        <tr style="background-color:#f4f5f7;">
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;border-bottom:1px solid #dfe1e6;">SHA-256</td>
          <td style="padding:10px 16px;font-size:12px;color:#172B4D;border-bottom:1px solid #dfe1e6;font-family:Consolas,Courier New,monospace;word-break:break-all;">__CHECKSUM__</td>
        </tr>
        <tr>
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;border-bottom:1px solid #dfe1e6;">REQUESTOR</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;border-bottom:1px solid #dfe1e6;">__REQUESTOR__</td>
        </tr>
        <tr style="background-color:#f4f5f7;">
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;">RUN ID</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;">__RUN_ID__</td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- Download button -->
  <tr>
    <td align="center" style="padding:20px 32px 8px 32px;">
      <table role="presentation" cellpadding="0" cellspacing="0">
        <tr>
          <td style="background-color:#003A70;border-radius:6px;">
            <a href="__UPLOAD_URL__"
               style="display:inline-block;padding:12px 32px;color:#ffffff;font-size:14px;font-weight:600;text-decoration:none;letter-spacing:0.3px;">
              &#8681; Download Encrypted Dump from Artifactory
            </a>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- Artifactory path (plain text fallback) -->
  <tr>
    <td style="padding:8px 32px 4px 32px;font-size:11px;color:#97A0AF;text-align:center;word-break:break-all;">
      __UPLOAD_URL__
    </td>
  </tr>

  <!-- Decryption hint -->
  <tr>
    <td style="padding:16px 32px 4px 32px;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
             style="background-color:#FFFAE6;border-left:4px solid #FF8B00;border-radius:4px;">
        <tr>
          <td style="padding:12px 16px;font-size:12px;color:#172B4D;line-height:1.5;">
            <strong>&#9888; Decryption:</strong>&nbsp;
            <code style="background-color:#f4f5f7;padding:2px 6px;border-radius:3px;font-size:12px;">gpg --decrypt __REQ__.tar.gz.gpg | tar xzf -</code><br>
            Use the OTP password provided in the original request.
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- Footer -->
  <tr>
    <td style="padding:24px 32px;font-size:11px;color:#97A0AF;border-top:1px solid #dfe1e6;margin-top:16px;text-align:center;line-height:1.6;">
      Kafka STP Dump Service v3.0.0 &bull; This is an automated notification.<br>
      Questions? Contact the Kafka STE Admin team.
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>
HTMLEOF

    # Replace placeholders with actual values
    email_body="${email_body//__STATUS_LABEL__/$status_label}"
    email_body="${email_body//__STATUS_COLOR__/$status_color}"
    email_body="${email_body//__INC__/$INC}"
    email_body="${email_body//__REQ__/$REQ}"
    email_body="${email_body//__TOPIC__/$TOPIC}"
    email_body="${email_body//__BOOTSTRAP__/$KAFKA_BOOTSTRAP}"
    email_body="${email_body//__MSG_COUNT__/$MSG_COUNT}"
    email_body="${email_body//__HUMAN_SIZE__/$human_size}"
    email_body="${email_body//__CHECKSUM__/$CHECKSUM}"
    email_body="${email_body//__REQUESTOR__/$REQUESTOR}"
    email_body="${email_body//__RUN_ID__/$UCD_RUN_ID}"
    email_body="${email_body//__UPLOAD_URL__/$UPLOAD_URL}"
    email_body="${email_body//__TIMESTAMP__/$timestamp}"

    # Build mailx arguments
    local -a mail_args=()
    mail_args+=(-s "$subject")
    mail_args+=(-S "content-type=text/html; charset=utf-8")

    if [[ -n "$cc_addr" ]]; then
        mail_args+=(-c "$cc_addr")
    fi

    if printf '%s' "$email_body" | mailx "${mail_args[@]}" "$to_addr" 2>> "$LOG_FILE"; then
        log INFO "Notification email sent to $to_addr (cc: ${cc_addr:-none})"
    else
        log WARN "Failed to send notification email (non-fatal)"
    fi
}

# Send notification if SOEID is available
if [[ -n "$SOEID" ]]; then
    send_notification \
        "${SOEID}@citi.com" \
        "$EMAIL_CC" \
        "Kafka STP : ${TOPIC} — Dump Complete [${INC}]" \
        "&#10004; COMPLETED" \
        "#006644"
else
    log WARN "SOEID not provided — skipping email notification"
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
