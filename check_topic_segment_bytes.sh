#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP=""
COMMAND_CONFIG=""
BROKER_ID="0"
DEFAULT_BYTES=""
KAFKA_BIN_DIR="${KAFKA_BIN_DIR:-}"
SORT_MODE="topic"
ONLY_OVERRIDES="no"
ONLY_DIFF="no"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bootstrap-server)  BOOTSTRAP="${2:-}"; shift 2;;
    --command-config)    COMMAND_CONFIG="${2:-}"; shift 2;;
    --broker-id)         BROKER_ID="${2:-}"; shift 2;;
    --default-bytes)     DEFAULT_BYTES="${2:-}"; shift 2;;
    --kafka-bin-dir)     KAFKA_BIN_DIR="${2:-}"; shift 2;;
    --sort)              SORT_MODE="${2:-topic}"; shift 2;;
    --only-overrides)    ONLY_OVERRIDES="yes"; shift 1;;
    --only-diff)         ONLY_DIFF="yes"; shift 1;;
    -h|--help)
      echo "Usage: $0 --command-config /path/client.properties [--bootstrap-server host:port] [--broker-id N] [--default-bytes BYTES] [--kafka-bin-dir DIR] [--sort topic|segdesc|deltadesc|source] [--only-overrides] [--only-diff]"
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
  [[ -z "$fqdn" ]] && { echo "ERROR: could not determine hostname/FQDN; provide --bootstrap-server."; exit 2; }
  BOOTSTRAP="${fqdn}:9094"
fi

# Default Confluent Platform bin dir if not provided and exists
if [[ -z "${KAFKA_BIN_DIR:-}" ]] && [[ -d /opt/confluent/latest/bin ]]; then
  KAFKA_BIN_DIR="/opt/confluent/latest/bin"
fi

pick_cmd() {
  local base="$1"
  if [[ -n "${KAFKA_BIN_DIR:-}" ]] && [[ -x "$KAFKA_BIN_DIR/$base" ]]; then
    echo "$KAFKA_BIN_DIR/$base"; return 0
  fi
  if [[ -n "${KAFKA_BIN_DIR:-}" ]] && [[ -x "$KAFKA_BIN_DIR/$base.sh" ]]; then
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

CONFIGS_CMD="$(pick_cmd kafka-configs)" || { echo "ERROR: kafka-configs(.sh) not found. Expected in ${KAFKA_BIN_DIR:-PATH}."; exit 2; }
TOPICS_CMD="$(pick_cmd kafka-topics)"  || { echo "ERROR: kafka-topics(.sh) not found. Expected in ${KAFKA_BIN_DIR:-PATH}."; exit 2; }

tmp_overrides="$(mktemp)"
tmp_topics="$(mktemp)"
tmp_out="$(mktemp)"
trap 'rm -f "$tmp_overrides" "$tmp_topics" "$tmp_out"' EXIT

# Broker default log.segment.bytes (or use provided --default-bytes)
if [[ -z "$DEFAULT_BYTES" ]]; then
  DEFAULT_BYTES="$(
    "$CONFIGS_CMD" --bootstrap-server "$BOOTSTRAP" --command-config "$COMMAND_CONFIG" \
      --entity-type brokers --entity-name "$BROKER_ID" --describe --all 2>/dev/null \
    | awk 'match($0,/log\.segment\.bytes=([0-9]+)/,m){print m[1]; exit}'
  )"
  DEFAULT_BYTES="${DEFAULT_BYTES:-1073741824}"  # fallback 1GiB
fi

# 1) Dump ALL topics once
"$TOPICS_CMD" --bootstrap-server "$BOOTSTRAP" --command-config "$COMMAND_CONFIG" --list 2>/dev/null \
| sed '/^[[:space:]]*$/d' > "$tmp_topics"

# 2) Dump ALL topic configs once; extract segment.bytes overrides into TSV
"$CONFIGS_CMD" --bootstrap-server "$BOOTSTRAP" --command-config "$COMMAND_CONFIG" \
  --entity-type topics --describe --all 2>/dev/null \
| awk '
  match($0, /(Dynamic|All)[[:space:]]+configs[[:space:]]+for[[:space:]]+topic[[:space:]]+([^[:space:]]+)[[:space:]]+are:/, t) {
    topic=t[2]; next
  }
  topic != "" && match($0, /segment\.bytes=([0-9]+)/, m) {
    print topic "\t" m[1]
    topic=""
  }
' > "$tmp_overrides"

# 3) Join: all topics -> override or default; apply filters
awk -v def="$DEFAULT_BYTES" -v only_ov="$ONLY_OVERRIDES" -v only_diff="$ONLY_DIFF" '
  BEGIN { FS=OFS="\t" }
  FNR==NR { ov[$1]=$2; next }   # overrides file
  {
    topic=$0
    has_ov = (topic in ov)
    if (only_ov=="yes" && !has_ov) next

    seg=def+0
    src="DEFAULT"
    if (has_ov) { seg=ov[topic]+0; src="OVERRIDE" }

    delta = seg - (def+0)
    if (only_diff=="yes" && delta==0) next

    rel = (delta>0 ? "GT" : (delta<0 ? "LT" : "EQ"))
    print topic, seg, src, (def+0), delta, rel
  }
' "$tmp_overrides" "$tmp_topics" > "$tmp_out"

echo -e "BOOTSTRAP_SERVER\t${BOOTSTRAP}"
echo -e "KAFKA_BIN_DIR\t${KAFKA_BIN_DIR:-PATH}"
echo -e "BROKER_DEFAULT_LOG_SEGMENT_BYTES\t${DEFAULT_BYTES}"
echo -e "TOPIC\tSEGMENT_BYTES\tSOURCE\tDEFAULT\tDELTA_BYTES\tRELATION"

case "${SORT_MODE,,}" in
  topic)
    sort -t $'\t' -k1,1 "$tmp_out"
    ;;
  segdesc)
    sort -t $'\t' -k2,2nr -k1,1 "$tmp_out"
    ;;
  deltadesc)
    sort -t $'\t' -k5,5nr -k1,1 "$tmp_out"
    ;;
  source)
    awk -F $'\t' 'BEGIN{OFS=FS}{rank=($3=="OVERRIDE"?0:1); print rank,$0}' "$tmp_out" \
      | sort -t $'\t' -k1,1n -k2,2 \
      | cut -f2-
    ;;
  *)
    echo "WARN: unknown --sort ${SORT_MODE}; using topic" >&2
    sort -t $'\t' -k1,1 "$tmp_out"
    ;;
esac
