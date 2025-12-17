#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP=""
COMMAND_CONFIG=""
BROKER_ID="0"
DEFAULT_BYTES=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bootstrap-server) BOOTSTRAP="${2:-}"; shift 2;;
    --command-config)   COMMAND_CONFIG="${2:-}"; shift 2;;
    --broker-id)        BROKER_ID="${2:-}"; shift 2;;
    --default-bytes)    DEFAULT_BYTES="${2:-}"; shift 2;;
    -h|--help)
      echo "Usage: $0 --command-config /path/client.properties [--bootstrap-server host:port] [--broker-id N] [--default-bytes BYTES]"
      exit 0
      ;;
    *) echo "Unknown arg: $1"; exit 1;;
  esac
done

if [[ -z "$COMMAND_CONFIG" ]] || [[ ! -f "$COMMAND_CONFIG" ]]; then
  echo "ERROR: --command-config /path/client.properties is required (mTLS)."
  exit 2
fi

if [[ -z "$BOOTSTRAP" ]]; then
  fqdn="$(hostname -f 2>/dev/null || true)"
  [[ -z "$fqdn" ]] && fqdn="$(hostname 2>/dev/null || true)"
  if [[ -z "$fqdn" ]]; then
    echo "ERROR: could not determine hostname/FQDN; provide --bootstrap-server explicitly."
    exit 2
  fi
  BOOTSTRAP="${fqdn}:9094"
fi

# Resolve kafka-configs binary (Confluent Platform supports kafka-configs or kafka-configs.sh)
CONFIGS_CMD="$(command -v kafka-configs 2>/dev/null || true)"
[[ -z "$CONFIGS_CMD" ]] && CONFIGS_CMD="$(command -v kafka-configs.sh 2>/dev/null || true)"
[[ -z "$CONFIGS_CMD" ]] && { echo "ERROR: kafka-configs(.sh) not found in PATH."; exit 2; }

# Get broker default log.segment.bytes (or use provided --default-bytes)
if [[ -z "$DEFAULT_BYTES" ]]; then
  DEFAULT_BYTES="$(
    "$CONFIGS_CMD" --bootstrap-server "$BOOTSTRAP" --command-config "$COMMAND_CONFIG" \
      --entity-type brokers --entity-name "$BROKER_ID" --describe --all 2>/dev/null \
    | awk 'match($0,/log\.segment\.bytes=([0-9]+)/,m){print m[1]; exit}'
  )"
  DEFAULT_BYTES="${DEFAULT_BYTES:-1073741824}"  # fallback 1GiB
fi

echo -e "BOOTSTRAP_SERVER\t${BOOTSTRAP}"
echo -e "BROKER_DEFAULT_LOG_SEGMENT_BYTES\t${DEFAULT_BYTES}"
echo -e "TOPIC\tSEGMENT_BYTES\tDEFAULT\tDELTA_BYTES\tRELATION"

# Single call across all topics; print only topics with segment.bytes override
"$CONFIGS_CMD" --bootstrap-server "$BOOTSTRAP" --command-config "$COMMAND_CONFIG" \
  --entity-type topics --describe --all 2>/dev/null \
| awk -v def="$DEFAULT_BYTES" '
  function relation(seg, def,  d) {
    d = seg - def
    if (d > 0) return "GT"
    if (d < 0) return "LT"
    return "EQ"
  }

  # Typical header:
  # Dynamic configs for topic <topic> are:
  /^Dynamic configs for topic[[:space:]]+/ {
    topic=$5
    gsub(/:$/,"",topic)
    next
  }

  # Some versions may show:
  # Dynamic configs for topic <topic> are:
  #   segment.bytes=123456 ...
  topic != "" && match($0, /segment\.bytes=([0-9]+)/, m) {
    seg = m[1] + 0
    delta = seg - (def + 0)
    printf "%s\t%d\t%d\t%d\t%s\n", topic, seg, def, delta, relation(seg, def)
    topic=""
  }
'
