#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Kafka STP — Topic Retention Update (1..7 days only)
# - Uses kafka-configs to set retention.ms
# - Uses kafka-topics to capture before/after describe
# - mTLS via --command-config (AdminClient properties)
# - Audit JSONL log under /var/log/confluent/kafka-topic-retention/audit.jsonl
# - Outlook-styled HTML email via sendmail (same styling as your reference)
#
# Binaries:
#   /opt/confluent/latest/bin/kafka-configs(.sh)
#   /opt/confluent/latest/bin/kafka-topics(.sh)
#
# AdminClient properties (mTLS):
#   /home/stekafka/config/stekafka_client.properties
# =============================================================================

BIN_DIR="/opt/confluent/latest/bin"

KAFKA_CONFIGS="${BIN_DIR}/kafka-configs"
[[ -x "${KAFKA_CONFIGS}" ]] || KAFKA_CONFIGS="${BIN_DIR}/kafka-configs.sh"

KAFKA_TOPICS="${BIN_DIR}/kafka-topics"
[[ -x "${KAFKA_TOPICS}" ]] || KAFKA_TOPICS="${BIN_DIR}/kafka-topics.sh"

DEFAULT_CMD_CONFIG="/home/stekafka/config/stekafka_client.properties"

LOG_BASE="/var/log/confluent/kafka-topic-retention"
AUDIT_FILE="${LOG_BASE}/audit.jsonl"

# Email defaults (override via args)
DEFAULT_MAIL_DOMAIN="example.com"
DEFAULT_MAIL_FROM="kafka-stp@$(hostname -d 2>/dev/null || echo example.com)"
SENDMAIL_BIN="/usr/sbin/sendmail"

MIN_DAYS=1
MAX_DAYS=7
DAY_MS=86400000
MIN_MS=$((MIN_DAYS * DAY_MS))        # 86,400,000
MAX_MS=$((MAX_DAYS * DAY_MS))        # 604,800,000

usage() {
  cat <<EOF
Usage:
  $(basename "$0") --inc INC12345 --topic <topic> --bootstrap <host:9093[,host:9093...]> --soeid <SoEID> \\
    (--days <1..7> | --ms <86400000..604800000>) \\
    [--command-config /path/to/adminclient.properties] \\
    [--mail-domain example.com] [--mail-from sender@domain] [--cc addr@domain] \\
    [--dry-run]

Defaults:
  --command-config ${DEFAULT_CMD_CONFIG}
  Audit log:        ${AUDIT_FILE}

Notes:
  - This script enforces retention range: 1..7 days (or equivalent ms).
  - Email is sent to: <soeid>@<mail-domain> using sendmail in HTML for Outlook.
EOF
}

now_iso() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

ensure_log_dir() {
  if [[ ! -d "$LOG_BASE" ]]; then
    mkdir -p "$LOG_BASE" 2>/dev/null || {
      echo "ERROR: Cannot create $LOG_BASE. Run as root or ensure permissions." >&2
      exit 2
    }
  fi
  touch "$AUDIT_FILE" 2>/dev/null || {
    echo "ERROR: Cannot write to $AUDIT_FILE. Fix permissions." >&2
    exit 2
  }
}

require_bins() {
  if [[ ! -x "$KAFKA_CONFIGS" ]]; then
    echo "ERROR: kafka-configs not found/executable at ${BIN_DIR}/kafka-configs(.sh)" >&2
    exit 2
  fi
  if [[ ! -x "$KAFKA_TOPICS" ]]; then
    echo "ERROR: kafka-topics not found/executable at ${BIN_DIR}/kafka-topics(.sh)" >&2
    exit 2
  fi
}

json_escape() {
  local s="${1:-}"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\n'/ }"
  s="${s//$'\r'/ }"
  printf '%s' "$s"
}

html_escape() {
  local s="${1:-}"
  s="${s//&/&amp;}"
  s="${s//</&lt;}"
  s="${s//>/&gt;}"
  s="${s//\"/&quot;}"
  s="${s//$'\r'/}"
  printf '%s' "$s"
}

get_current_retention_ms() {
  local topic="$1"
  local bootstrap="$2"
  local cmdcfg="$3"

  set +e
  local out
  out="$("$KAFKA_CONFIGS" --bootstrap-server "$bootstrap" --command-config "$cmdcfg" \
    --describe --entity-type topics --entity-name "$topic" 2>&1)"
  local rc=$?
  set -e

  if [[ $rc -ne 0 ]]; then
    echo "ERROR_DESCRIBE::$out"
    return 0
  fi

  local val
  val="$(printf '%s\n' "$out" | grep -Eo 'retention\.ms=[0-9]+' | head -n1 | cut -d= -f2 || true)"
  [[ -z "$val" ]] && echo "NOT_SET" || echo "$val"
}

describe_topic() {
  local topic="$1"
  local bootstrap="$2"
  local cmdcfg="$3"

  set +e
  local out
  out="$("$KAFKA_TOPICS" --bootstrap-server "$bootstrap" --command-config "$cmdcfg" \
    --describe --topic "$topic" 2>&1)"
  local rc=$?
  set -e

  if [[ $rc -ne 0 ]]; then
    echo "ERROR_TOPIC_DESCRIBE::$out"
    return 0
  fi
  echo "$out"
}

# ============================================================================
# Email Notification (HTML for Outlook) - same styling as your reference
# ============================================================================
send_notification() {
    local to_addr="$1"
    local cc_addr="$2"
    local subject="$3"
    local status_label="$4"
    local status_color="$5"

    local timestamp
    timestamp=$(date -u +"%Y-%m-%d %H:%M:%S UTC")

    local desc_before_html desc_after_html err_html
    desc_before_html="$(printf '%s' "${TOPIC_DESC_BEFORE:-}" | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g')"
    desc_after_html="$(printf '%s' "${TOPIC_DESC_AFTER:-}"  | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g')"
    err_html="$(printf '%s' "${ERROR_MSG:-}"               | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g')"
    [[ -z "$err_html" ]] && err_html="N/A"

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
            &#9670; Kafka STP &mdash; Topic Retention Update
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
      The Kafka topic retention update for <strong>__TOPIC__</strong> completed at <strong>__TIMESTAMP__</strong>.
    </td>
  </tr>

  <!-- Details table -->
  <tr>
    <td style="padding:12px 32px;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
             style="border:1px solid #dfe1e6;border-radius:6px;border-collapse:separate;">
        <tr style="background-color:#f4f5f7;">
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;width:180px;border-bottom:1px solid #dfe1e6;">INCIDENT</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;border-bottom:1px solid #dfe1e6;">__INC__</td>
        </tr>
        <tr>
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;border-bottom:1px solid #dfe1e6;">TOPIC</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;border-bottom:1px solid #dfe1e6;font-family:Consolas,Courier New,monospace;">__TOPIC__</td>
        </tr>
        <tr style="background-color:#f4f5f7;">
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;border-bottom:1px solid #dfe1e6;">BOOTSTRAP</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;border-bottom:1px solid #dfe1e6;font-family:Consolas,Courier New,monospace;">__BOOTSTRAP__</td>
        </tr>
        <tr>
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;border-bottom:1px solid #dfe1e6;">REQUESTOR (SoEID)</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;border-bottom:1px solid #dfe1e6;">__SOEID__</td>
        </tr>
        <tr style="background-color:#f4f5f7;">
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;border-bottom:1px solid #dfe1e6;">HOST</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;border-bottom:1px solid #dfe1e6;">__HOST__</td>
        </tr>
        <tr>
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;border-bottom:1px solid #dfe1e6;">WINDOW (UTC)</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;border-bottom:1px solid #dfe1e6;">__START_TS__ &rarr; __END_TS__</td>
        </tr>
        <tr style="background-color:#f4f5f7;">
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;border-bottom:1px solid #dfe1e6;">REQUEST MODE</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;border-bottom:1px solid #dfe1e6;">__MODE__ (days=__REQ_DAYS__ / ms=__REQ_MS__)</td>
        </tr>
        <tr>
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;border-bottom:1px solid #dfe1e6;">RETENTION.APPLIED (ms)</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;border-bottom:1px solid #dfe1e6;font-family:Consolas,Courier New,monospace;">__RET_APPLIED__</td>
        </tr>
        <tr style="background-color:#f4f5f7;">
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;border-bottom:1px solid #dfe1e6;">RETENTION.BEFORE (ms)</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;border-bottom:1px solid #dfe1e6;font-family:Consolas,Courier New,monospace;">__RET_BEFORE__</td>
        </tr>
        <tr>
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;border-bottom:1px solid #dfe1e6;">RETENTION.AFTER (ms)</td>
          <td style="padding:10px 16px;font-size:14px;color:#172B4D;border-bottom:1px solid #dfe1e6;font-family:Consolas,Courier New,monospace;">__RET_AFTER__</td>
        </tr>
        <tr style="background-color:#f4f5f7;">
          <td style="padding:10px 16px;font-size:12px;color:#6B778C;font-weight:600;">AUDIT LOG</td>
          <td style="padding:10px 16px;font-size:12px;color:#172B4D;font-family:Consolas,Courier New,monospace;word-break:break-all;">__AUDIT_FILE__</td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- Topic describe BEFORE -->
  <tr>
    <td style="padding:8px 32px 0 32px;font-size:12px;color:#6B778C;font-weight:600;">
      TOPIC DESCRIBE (BEFORE)
    </td>
  </tr>
  <tr>
    <td style="padding:8px 32px 0 32px;">
      <pre style="background-color:#f4f5f7;border:1px solid #dfe1e6;padding:12px 14px;border-radius:6px;white-space:pre-wrap;font-size:12px;color:#172B4D;font-family:Consolas,Courier New,monospace;line-height:1.45;">__TOPIC_DESC_BEFORE__</pre>
    </td>
  </tr>

  <!-- Topic describe AFTER -->
  <tr>
    <td style="padding:12px 32px 0 32px;font-size:12px;color:#6B778C;font-weight:600;">
      TOPIC DESCRIBE (AFTER)
    </td>
  </tr>
  <tr>
    <td style="padding:8px 32px 0 32px;">
      <pre style="background-color:#f4f5f7;border:1px solid #dfe1e6;padding:12px 14px;border-radius:6px;white-space:pre-wrap;font-size:12px;color:#172B4D;font-family:Consolas,Courier New,monospace;line-height:1.45;">__TOPIC_DESC_AFTER__</pre>
    </td>
  </tr>

  <!-- Error block -->
  <tr>
    <td style="padding:12px 32px 0 32px;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
             style="background-color:#FFEBE6;border-left:4px solid #DE350B;border-radius:4px;">
        <tr>
          <td style="padding:12px 16px;font-size:12px;color:#172B4D;line-height:1.5;">
            <strong>&#9888; Error:</strong><br>
            <span style="font-family:Consolas,Courier New,monospace;white-space:pre-wrap;">__ERROR__</span>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- Footer -->
  <tr>
    <td style="padding:24px 32px;font-size:11px;color:#97A0AF;border-top:1px solid #dfe1e6;margin-top:16px;text-align:center;line-height:1.6;">
      Kafka STP Retention Service &bull; This is an automated notification.<br>
      Questions? Contact the Kafka STE Admin team.
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>
HTMLEOF

    email_body="${email_body//__STATUS_LABEL__/$(printf '%s' "$status_label")}"
    email_body="${email_body//__STATUS_COLOR__/$(printf '%s' "$status_color")}"
    email_body="${email_body//__TIMESTAMP__/$(printf '%s' "$timestamp")}"

    email_body="${email_body//__INC__/$(printf '%s' "$(html_escape "${INC:-}")")}"
    email_body="${email_body//__TOPIC__/$(printf '%s' "$(html_escape "${TOPIC:-}")")}"
    email_body="${email_body//__BOOTSTRAP__/$(printf '%s' "$(html_escape "${BOOTSTRAP:-}")")}"
    email_body="${email_body//__SOEID__/$(printf '%s' "$(html_escape "${SOEID:-}")")}"
    email_body="${email_body//__HOST__/$(printf '%s' "$(html_escape "${HOST:-}")")}"
    email_body="${email_body//__START_TS__/$(printf '%s' "$(html_escape "${START_TS:-}")")}"
    email_body="${email_body//__END_TS__/$(printf '%s' "$(html_escape "${END_TS:-}")")}"
    email_body="${email_body//__MODE__/$(printf '%s' "$(html_escape "${MODE:-}")")}"
    email_body="${email_body//__REQ_DAYS__/$(printf '%s' "$(html_escape "${REQUESTED_DAYS:-}")")}"
    email_body="${email_body//__REQ_MS__/$(printf '%s' "$(html_escape "${REQUESTED_MS:-}")")}"

    email_body="${email_body//__RET_APPLIED__/$(printf '%s' "$(html_escape "${RETENTION_APPLIED:-}")")}"
    email_body="${email_body//__RET_BEFORE__/$(printf '%s' "$(html_escape "${RETENTION_BEFORE:-}")")}"
    email_body="${email_body//__RET_AFTER__/$(printf '%s' "$(html_escape "${RETENTION_AFTER:-}")")}"

    email_body="${email_body//__AUDIT_FILE__/$(printf '%s' "$(html_escape "${AUDIT_FILE:-}")")}"
    email_body="${email_body//__TOPIC_DESC_BEFORE__/${desc_before_html}}"
    email_body="${email_body//__TOPIC_DESC_AFTER__/${desc_after_html}}"
    email_body="${email_body//__ERROR__/${err_html}}"

    # Send via sendmail
    if [[ ! -x "$SENDMAIL_BIN" ]]; then
      echo "WARN: sendmail not found at $SENDMAIL_BIN; skipping email." >&2
      return 0
    fi

    {
        echo "From: ${MAIL_FROM}"
        echo "To: ${to_addr}"
        [[ -n "${cc_addr}" ]] && echo "Cc: ${cc_addr}"
        echo "Subject: ${subject}"
        echo "MIME-Version: 1.0"
        echo "Content-Type: text/html; charset=UTF-8"
        echo "Content-Transfer-Encoding: 8bit"
        echo
        printf '%s\n' "$email_body"
    } | "$SENDMAIL_BIN" -t
}

main() {
  local inc="" topic="" bootstrap="" soeid=""
  local days="" ms=""
  local dry_run=0
  local cmdcfg="${DEFAULT_CMD_CONFIG}"
  local mail_domain="${DEFAULT_MAIL_DOMAIN}"
  local mail_from="${DEFAULT_MAIL_FROM}"
  local cc_addr=""

  if [[ $# -eq 0 ]]; then usage; exit 1; fi

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --inc) inc="${2:-}"; shift 2;;
      --topic) topic="${2:-}"; shift 2;;
      --bootstrap) bootstrap="${2:-}"; shift 2;;
      --soeid) soeid="${2:-}"; shift 2;;
      --days) days="${2:-}"; shift 2;;
      --ms) ms="${2:-}"; shift 2;;
      --command-config) cmdcfg="${2:-}"; shift 2;;
      --mail-domain) mail_domain="${2:-}"; shift 2;;
      --mail-from) mail_from="${2:-}"; shift 2;;
      --cc) cc_addr="${2:-}"; shift 2;;
      --dry-run) dry_run=1; shift;;
      -h|--help) usage; exit 0;;
      *) echo "Unknown argument: $1" >&2; usage; exit 1;;
    esac
  done

  ensure_log_dir
  require_bins

  local start_ts; start_ts="$(now_iso)"
  local host; host="$(hostname -f 2>/dev/null || hostname)"
  local status="FAIL"
  local error_msg=""
  local requested_mode=""
  local retention_ms=""
  local before_retention=""
  local after_retention=""
  local before_topic_desc=""
  local after_topic_desc=""
  local end_ts=""

  # Identify OS user without calling whoami (per your request)
  local os_user="${SUDO_USER:-${LOGNAME:-unknown}}"

  finalize() {
    local exit_code="$1"
    end_ts="$(now_iso)"

    # --- audit JSONL ---
    printf '{' >> "$AUDIT_FILE"
    printf '"timestamp_start":"%s",' "$(json_escape "$start_ts")" >> "$AUDIT_FILE"
    printf '"timestamp_end":"%s",' "$(json_escape "$end_ts")" >> "$AUDIT_FILE"
    printf '"inc":"%s",' "$(json_escape "$inc")" >> "$AUDIT_FILE"
    printf '"topic":"%s",' "$(json_escape "$topic")" >> "$AUDIT_FILE"
    printf '"bootstrap":"%s",' "$(json_escape "$bootstrap")" >> "$AUDIT_FILE"
    printf '"command_config":"%s",' "$(json_escape "$cmdcfg")" >> "$AUDIT_FILE"
    printf '"soe_id":"%s",' "$(json_escape "$soeid")" >> "$AUDIT_FILE"
    printf '"os_user":"%s",' "$(json_escape "$os_user")" >> "$AUDIT_FILE"
    printf '"host":"%s",' "$(json_escape "$host")" >> "$AUDIT_FILE"
    printf '"mode":"%s",' "$(json_escape "$requested_mode")" >> "$AUDIT_FILE"
    printf '"requested_days":"%s",' "$(json_escape "${days:-}")" >> "$AUDIT_FILE"
    printf '"requested_ms":"%s",' "$(json_escape "${ms:-}")" >> "$AUDIT_FILE"
    printf '"retention_ms_applied":"%s",' "$(json_escape "${retention_ms:-}")" >> "$AUDIT_FILE"
    printf '"retention_ms_before":"%s",' "$(json_escape "${before_retention:-}")" >> "$AUDIT_FILE"
    printf '"retention_ms_after":"%s",' "$(json_escape "${after_retention:-}")" >> "$AUDIT_FILE"
    printf '"dry_run":%s,' "$dry_run" >> "$AUDIT_FILE"
    printf '"status":"%s",' "$(json_escape "$status")" >> "$AUDIT_FILE"
    printf '"error":"%s"' "$(json_escape "$error_msg")" >> "$AUDIT_FILE"
    printf '}\n' >> "$AUDIT_FILE"

    # --- email (always attempt) ---
    INC="$inc"
    TOPIC="$topic"
    BOOTSTRAP="$bootstrap"
    SOEID="$soeid"
    HOST="$host"
    START_TS="$start_ts"
    END_TS="$end_ts"
    MODE="$requested_mode"
    REQUESTED_DAYS="${days:-}"
    REQUESTED_MS="${ms:-}"
    RETENTION_APPLIED="${retention_ms:-}"
    RETENTION_BEFORE="${before_retention:-}"
    RETENTION_AFTER="${after_retention:-}"
    TOPIC_DESC_BEFORE="${before_topic_desc:-}"
    TOPIC_DESC_AFTER="${after_topic_desc:-}"
    ERROR_MSG="${error_msg:-}"
    MAIL_FROM="$mail_from"

    local status_label status_color
    if [[ "$status" == "SUCCESS" ]]; then
      status_label="SUCCESS"
      status_color="#36B37E"
    else
      status_label="FAILED"
      status_color="#DE350B"
    fi

    local to_addr="${soeid}@${mail_domain}"
    local subject="[${status_label}] Kafka Topic Retention Update - ${topic} - ${inc}"

    send_notification "$to_addr" "$cc_addr" "$subject" "$status_label" "$status_color" || true

    # --- console ---
    if [[ "$exit_code" -eq 0 ]]; then
      echo "OK: topic=$topic retention.ms=$retention_ms (before=$before_retention after=$after_retention)"
      echo "Audit: $AUDIT_FILE"
      echo "Email attempted to: $to_addr"
    else
      echo "ERROR: $error_msg" >&2
      echo "Audit: $AUDIT_FILE" >&2
      echo "Email attempted to: $to_addr" >&2
    fi

    exit "$exit_code"
  }

  # Required args
  if [[ -z "$inc" || -z "$topic" || -z "$bootstrap" || -z "$soeid" ]]; then
    error_msg="Missing required arguments (need --inc, --topic, --bootstrap, --soeid)"
    finalize 1
  fi

  # command-config must exist/readable
  if [[ ! -f "$cmdcfg" ]]; then
    error_msg="command-config not found: $cmdcfg"
    finalize 1
  fi
  if [[ ! -r "$cmdcfg" ]]; then
    error_msg="command-config not readable: $cmdcfg"
    finalize 1
  fi

  # Exactly one of --days or --ms
  if [[ -n "$days" && -n "$ms" ]] || [[ -z "$days" && -z "$ms" ]]; then
    error_msg="Specify exactly one of --days or --ms"
    finalize 1
  fi

  # Range enforcement + conversion
  if [[ -n "$days" ]]; then
    requested_mode="days"
    if ! [[ "$days" =~ ^[0-9]+$ ]]; then
      error_msg="--days must be an integer"
      finalize 1
    fi
    if (( days < MIN_DAYS || days > MAX_DAYS )); then
      error_msg="--days out of range (min ${MIN_DAYS}, max ${MAX_DAYS})"
      finalize 1
    fi
    retention_ms=$((days * DAY_MS))
  else
    requested_mode="ms"
    if ! [[ "$ms" =~ ^[0-9]+$ ]]; then
      error_msg="--ms must be an integer"
      finalize 1
    fi
    if (( ms < MIN_MS || ms > MAX_MS )); then
      error_msg="--ms out of range (min ${MIN_MS}, max ${MAX_MS})"
      finalize 1
    fi
    retention_ms="$ms"
  fi

  # BEFORE snapshots
  before_retention="$(get_current_retention_ms "$topic" "$bootstrap" "$cmdcfg")"
  before_topic_desc="$(describe_topic "$topic" "$bootstrap" "$cmdcfg")"

  if [[ "$before_retention" == ERROR_DESCRIBE::* ]]; then
    error_msg="${before_retention#ERROR_DESCRIBE::}"
    # attempt describe again after failure (as requested)
    after_topic_desc="$(describe_topic "$topic" "$bootstrap" "$cmdcfg")"
    finalize 1
  fi

  # DRY RUN
  if (( dry_run == 1 )); then
    status="SUCCESS"
    after_retention="$before_retention"
    after_topic_desc="$before_topic_desc"
    finalize 0
  fi

  # Apply change
  set +e
  local alter_out
  alter_out="$("$KAFKA_CONFIGS" --bootstrap-server "$bootstrap" --command-config "$cmdcfg" \
    --alter --entity-type topics --entity-name "$topic" --add-config "retention.ms=${retention_ms}" 2>&1)"
  local alter_rc=$?
  set -e

  if [[ $alter_rc -ne 0 ]]; then
    error_msg="$alter_out"
    # attempt describe again after failure
    after_retention="$(get_current_retention_ms "$topic" "$bootstrap" "$cmdcfg")"
    after_topic_desc="$(describe_topic "$topic" "$bootstrap" "$cmdcfg")"
    finalize 1
  fi

  # AFTER snapshots
  after_retention="$(get_current_retention_ms "$topic" "$bootstrap" "$cmdcfg")"
  after_topic_desc="$(describe_topic "$topic" "$bootstrap" "$cmdcfg")"

  if [[ "$after_retention" == ERROR_DESCRIBE::* ]]; then
    error_msg="${after_retention#ERROR_DESCRIBE::}"
    # attempt describe again after failure
    after_topic_desc="$(describe_topic "$topic" "$bootstrap" "$cmdcfg")"
    finalize 1
  fi

  status="SUCCESS"
  finalize 0
}

main "$@"
