#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${1:-}"
if [[ -z "${BOOTSTRAP}" ]]; then
  echo "Usage: $0 <bootstrap-server> --command-config /path/client.properties [--broker-id N] [--default-bytes BYTES] [--log-dirs /p1,/p2]"
  exit 1
fi

BROKER_ID="0"
DEFAULT_BYTES=""
LOG_DIRS=""
COMMAND_CONFIG=""

shift || true
while [[ $# -gt 0 ]]; do
  case "$1" in
    --broker-id) BROKER_ID="${2:-}"; shift 2;;
    --default-bytes) DEFAULT_BYTES="${2:-}"; shift 2;;
    --log-dirs) LOG_DIRS="${2:-}"; shift 2;;
    --command-config) COMMAND_CONFIG="${2:-}"; shift 2;;
    *) echo "Unknown arg: $1"; exit 1;;
  esac
done

if [[ -z "$COMMAND_CONFIG" ]]; then
  echo "ERROR: --command-config is required for mTLS clusters."
  exit 2
fi

if [[ ! -f "$COMMAND_CONFIG" ]]; then
  echo "ERROR: command-config file not found: $COMMAND_CONFIG"
  exit 2
fi

KAFKA_BIN_DIR="${KAFKA_BIN_DIR:-""}"

pick_cmd() {
  local base="$1"
  if [[ -n "$KAFKA_BIN_DIR" ]] && [[ -x "$KAFKA_BIN_DIR/$base" ]]; then
    echo "$KAFKA_BIN_DIR/$base"; return 0
  fi
  if [[ -n "$KAFKA_BIN_DIR" ]] && [[ -x "$KAFKA_BIN_DIR/$base.sh" ]]; then
    echo "$KAFKA_BIN_DIR/$base.sh"; return 0
  fi
  if command -v "$base" >/dev/null 2>&1; then
    command -v "$base"; return 0
  fi
  if command -v "$base.sh" >/dev/null 2>&1; then
    command -v "$base.sh"; return 0
  fi
  return 1
}

TOPICS_CMD="$(pick_cmd kafka-topics)" || { echo "ERROR: kafka-topics(.sh) not found. Set KAFKA_BIN_DIR or fix PATH."; exit 2; }
CONFIGS_CMD="$(pick_cmd kafka-configs)" || { echo "ERROR: kafka-configs(.sh) not found. Set KAFKA_BIN_DIR or fix PATH."; exit 2; }

# --- Get broker default log.segment.bytes unless user provided it
if [[ -z "$DEFAULT_BYTES" ]]; then
  DEFAULT_BYTES="$(
    "$CONFIGS_CMD" --bootstrap-server "$BOOTSTRAP" --command-config "$COMMAND_CONFIG" \
      --entity-type brokers --entity-name "$BROKER_ID" --describe --all 2>/dev/null \
    | awk '
        match($0, /log\.segment\.bytes=([0-9]+)/, m) { print m[1]; found=1; exit }
        END { if (!found) exit 1 }
      ' || true
  )"

  if [[ -z "$DEFAULT_BYTES" ]]; then
    DEFAULT_BYTES="1073741824"  # Kafka default = 1 GiB (fallback)
  fi
fi

echo -e "BROKER_DEFAULT_LOG_SEGMENT_BYTES\t${DEFAULT_BYTES}"
echo -e "TOPIC\tSEGMENT_BYTES\tDEFAULT\tDELTA_BYTES"

# --- List topics and compare topic-level segment.bytes to broker default
"$TOPICS_CMD" --bootstrap-server "$BOOTSTRAP" --command-config "$COMMAND_CONFIG" --list 2>/dev/null \
| while IFS= read -r topic; do
    [[ -z "$topic" ]] && continue

    seg_bytes="$(
      "$CONFIGS_CMD" --bootstrap-server "$BOOTSTRAP" --command-config "$COMMAND_CONFIG" \
        --entity-type topics --entity-name "$topic" --describe --all 2>/dev/null \
      | awk '
          match($0, /segment\.bytes=([0-9]+)/, m) { print m[1]; exit }
        ' || true
    )"

    # Only report topics where segment.bytes is explicitly set AND greater than the default
    if [[ -n "$seg_bytes" ]] && [[ "$seg_bytes" =~ ^[0-9]+$ ]] && (( seg_bytes > DEFAULT_BYTES )); then
      delta=$((seg_bytes - DEFAULT_BYTES))
      echo -e "${topic}\t${seg_bytes}\t${DEFAULT_BYTES}\t${delta}"
    fi
  done

# --- Optional: scan broker log dirs for actual segment files bigger than DEFAULT_BYTES
if [[ -n "$LOG_DIRS" ]]; then
  echo
  echo -e "SEGMENT_FILES_LARGER_THAN_DEFAULT (bytes > ${DEFAULT_BYTES})"
  echo -e "TOPIC\tPARTITION_DIR\tFILE\tSIZE_BYTES"

  IFS=',' read -r -a dirs <<< "$LOG_DIRS"
  for d in "${dirs[@]}"; do
    [[ -z "$d" ]] && continue
    if [[ ! -d "$d" ]]; then
      echo "WARN: log dir not found: $d" >&2
      continue
    fi

    find "$d" -type f -name "*.log" -size +"${DEFAULT_BYTES}"c 2>/dev/null \
    | while IFS= read -r f; do
        size=$(stat -c '%s' "$f" 2>/dev/null || echo "")
        partdir=$(basename "$(dirname "$f")")

        topic_name="$partdir"
        if [[ "$partdir" =~ ^(.+)-([0-9]+)$ ]]; then
          topic_name="${BASH_REMATCH[1]}"
        fi

        echo -e "${topic_name}\t${partdir}\t${f}\t${size}"
      done
  done
fi
