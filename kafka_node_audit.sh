#!/usr/bin/env bash
# Kafka Node Audit for RHEL8 + Confluent 7.6.1 (v4)
# Adds threshold guidance for:
#   num.network.threads, num.io.threads, num.replica.fetchers, num.recovery.threads.per.data.dir
# Keeps all earlier NIC, ring buffer, bonding, tuned, sysctl, Java/Confluent checks.

set -euo pipefail

SERVICE_USER="${SERVICE_USER:-stekafka}"

BOLD=$(tput bold 2>/dev/null || true); RESET=$(tput sgr0 2>/dev/null || true)
GREEN="\033[32m"; YELLOW="\033[33m"; RED="\033[31m"; NC="\033[0m"
pass(){ printf "%b[PASS]%b %s\n" "${GREEN}" "${NC}" "$1"; }
warn(){ printf "%b[WARN]%b %s\n" "${YELLOW}" "${NC}" "$1"; }
fail(){ printf "%b[FAIL]%b %s\n" "${RED}" "${NC}" "$1"; }
info(){ printf "%b[INFO]%b %s\n" "" "${NC}" "$1"; }
hr(){ printf "\n%s\n" "------------------------------------------------------------"; }
section(){ hr; printf "%s\n" "${BOLD}$1${RESET}"; hr; }

has_cmd(){ command -v "$1" >/dev/null 2>&1; }
kv(){ sysctl -n "$1" 2>/dev/null || echo "N/A"; }
clamp(){ # clamp value min max
  local v="$1" min="$2" max="$3"
  (( v < min )) && v="$min"
  (( v > max )) && v="$max"
  echo "$v"
}

# ---------- (A) SYSTEM & TUNED ----------
section "SYSTEM & TUNED PROFILE"
printf "Hostname: %s\n" "$(hostname)"
printf "Kernel  : %s\n" "$(uname -r)"
if has_cmd tuned-adm; then
  if systemctl is-active --quiet tuned; then pass "tuned service is active"; else warn "tuned service is NOT active"; fi
  active=$(tuned-adm active 2>/dev/null | awk -F': ' '/Current active profile/{print $2}')
  printf "Active tuned profile: %s\n" "${active:-unknown}"
  case "${active:-}" in
    throughput-performance|latency-performance|network-throughput|cpu-performance|high-performance) pass "High-performance tuned profile (${active})";;
    *) warn "Consider a high-performance tuned profile (throughput-performance or latency-performance).";;
  esac
else
  warn "tuned-adm not installed."
fi

# ---------- (B) THP ----------
section "TRANSPARENT HUGE PAGES (THP)"
for f in enabled defrag; do
  p="/sys/kernel/mm/transparent_hugepage/$f"
  val=$(cat "$p" 2>/dev/null || echo "N/A")
  printf "%-7s: %s\n" "$f" "$val"
done

# ---------- (C) SYSCTL ----------
section "SYSCTL CHECKS (Kafka-oriented)"
declare -A RECS=(
  [vm.swappiness]=1
  [net.core.netdev_max_backlog]=250000
  [net.core.rmem_max]=134217728
  [net.core.wmem_max]=134217728
)
for k in "${!RECS[@]}"; do
  cur=$(kv "$k"); want=${RECS[$k]}
  [[ "$cur" == "N/A" ]] && { warn "$k not readable"; continue; }
  [[ "$cur" -ge "$want" ]] && pass "$k=$cur (>= $want)" || warn "$k=$cur (< $want)"
done
tcp_rmem=$(kv net.ipv4.tcp_rmem); tcp_wmem=$(kv net.ipv4.tcp_wmem)
printf "net.ipv4.tcp_rmem: %s\n" "$tcp_rmem"
printf "net.ipv4.tcp_wmem: %s\n" "$tcp_wmem"

# ---------- (D) NICs & RING BUFFERS ----------
section "NETWORK INTERFACES & RING BUFFERS"
[[ -x "$(command -v ip)" ]] || warn "ip not found"
[[ -x "$(command -v ethtool)" ]] || warn "ethtool not found; limited NIC details"

mapfile -t ifaces < <(ls -1 /sys/class/net | grep -vE '^(lo)$' || true)
printf "Detected interfaces: %s\n" "${ifaces[*]:-none}"

has25g=0
for i in "${ifaces[@]}"; do
  [[ -d "/proc/net/bonding/$i" ]] && continue
  state=$(cat "/sys/class/net/$i/operstate" 2>/dev/null || echo "N/A")
  mtu=$(cat "/sys/class/net/$i/mtu" 2>/dev/null || echo "N/A")
  speed="N/A"; duplex="N/A"
  if has_cmd ethtool; then
    speed=$(ethtool "$i" 2>/dev/null | awk -F': ' '/Speed/{print $2}')
    duplex=$(ethtool "$i" 2>/dev/null | awk -F': ' '/Duplex/{print $2}')
  fi
  printf "\nInterface: %s\n  State : %s\n  MTU   : %s\n  Speed : %s\n  Duplex: %s\n" "$i" "$state" "$mtu" "$speed" "$duplex"
  [[ "$speed" =~ 25000|25Gb ]] && { pass "25Gb capable NIC: $i ($speed)"; has25g=1; }
  if has_cmd ethtool; then
    rb=$(ethtool -g "$i" 2>/dev/null || true)
    if [[ -n "$rb" ]]; then
      cur_rx=$(awk '/^RX:/ {print $2}' <<<"$rb" | head -n1)
      cur_tx=$(awk '/^TX:/ {print $2}' <<<"$rb" | head -n1)
      max_rx=$(awk '/RX:/ {getline; print $2}' <<<"$rb" | head -n1)
      max_tx=$(awk '/TX:/ {getline; print $2}' <<<"$rb" | head -n1)
      printf "  Rings (cur/max): RX %s/%s, TX %s/%s\n" "${cur_rx:-?}" "${max_rx:-?}" "${cur_tx:-?}" "${max_tx:-?}"
      [[ -n "${cur_rx:-}" && -n "${max_rx:-}" && "$cur_rx" -lt "$max_rx" ]] && warn "  RX ring below max (consider: ethtool -G $i rx $max_rx)" || pass "  RX ring OK"
      [[ -n "${cur_tx:-}" && -n "${max_tx:-}" && "$cur_tx" -lt "$max_tx" ]] && warn "  TX ring below max (consider: ethtool -G $i tx $max_tx)" || pass "  TX ring OK"
    fi
  fi
done
[[ "$has25g" -eq 1 ]] && pass "At least one 25Gb NIC detected" || warn "No 25Gb NIC detected by speed."

# ---------- (E) BONDING ----------
section "BONDING"
if ls /proc/net/bonding/* >/dev/null 2>&1; then
  for bf in /proc/net/bonding/*; do
    [ -e "$bf" ] || continue
    bname=$(basename "$bf")
    echo "Bond: $bname"
    mode=$(awk -F': ' '/Bonding Mode/{print $2}' "$bf")
    echo "  Mode: $mode"
    awk '
      /Slave Interface/ {iface=$3}
      /MII Status/ && iface {printf "  Slave %-10s link %s\n", iface, $3; iface=""}
    ' "$bf"
    pass "Bond $bname present ($mode)"
  done
else
  info "No Linux bonding configured."
fi

# ---------- (F) ULIMITS ----------
section "ULIMITS FOR SERVICE USER (${SERVICE_USER})"
if id "$SERVICE_USER" >/dev/null 2>&1; then
  read_ul(){ sudo -u "$SERVICE_USER" bash -lc "$1" 2>/dev/null || echo "N/A"; }
  nofile=$(read_ul 'ulimit -n'); nproc=$(read_ul 'ulimit -u'); memlock=$(read_ul 'ulimit -l')
  printf "nofile : %s\nnproc  : %s\nmemlock: %s\n" "$nofile" "$nproc" "$memlock"
  [[ "$nofile" != "N/A" && "$nofile" -ge 100000 ]] && pass "nofile >= 100000" || warn "nofile < 100000"
  [[ "$nproc"  != "N/A" && "$nproc"  -ge 4096   ]] && pass "nproc >= 4096"     || warn "nproc < 4096"
  [[ "$memlock" == "unlimited" || "$memlock" == "N/A" ]] && pass "memlock unlimited (or not enforced)" || warn "memlock not unlimited"
else
  warn "User ${SERVICE_USER} not found."
fi

# ---------- (G) JAVA & CONFLUENT ----------
section "JAVA & CONFLUENT VERSIONS"
if has_cmd java; then
  info "Java: $(java -version 2>&1 | head -n1)"
else
  warn "Java not found in PATH"
fi
if [ -d /opt/confluent ]; then
  verfile=$(find /opt/confluent -maxdepth 2 -name 'confluent.version' 2>/dev/null | head -n1 || true)
  if [[ -n "$verfile" ]]; then
    pass "Confluent Platform version: $(cat "$verfile")"
  elif [ -x /opt/confluent/bin/confluent ]; then
    info "Confluent CLI: $(/opt/confluent/bin/confluent --version 2>/dev/null | head -n1)"
  else
    warn "Confluent install present but version file/CLI not found."
  fi
else
  warn "/opt/confluent not found (OK if not installed yet)."
fi

# ---------- (H) KAFKA server.properties + THRESHOLDS ----------
section "KAFKA server.properties KEY CHECKS + RECOMMENDATIONS"

# Locate server.properties
declare -a CANDIDATE_PROPS=(
  "/etc/kafka/server.properties"
  "/etc/confluent-server/server.properties"
  "/opt/confluent/etc/kafka/server.properties"
  "/var/lib/kafka/config/server.properties"
)
FOUND_PROPS=""
for f in "${CANDIDATE_PROPS[@]}"; do
  [[ -r "$f" ]] && FOUND_PROPS+="$f"$'\n'
done
discover_props_from_unit(){
  local unit="$1"
  systemctl show "$unit" -p ExecStart 2>/dev/null \
    | awk -F= '/^ExecStart=/{print $2}' \
    | sed 's/\\x20/ /g' \
    | grep -Eo '(\S*server\.properties\S*)' \
    | head -n1
}
for unit in confluent-server kafka confluent-kafka cp-kafka; do
  p=$(discover_props_from_unit "$unit" || true)
  [[ -n "$p" && -r "$p" ]] && FOUND_PROPS+="$p"$'\n'
done
SERVER_PROPS=$(echo -e "$FOUND_PROPS" | awk 'NF' | xargs -r ls -1t 2>/dev/null | head -n1 || true)

if [[ -z "${SERVER_PROPS:-}" ]]; then
  warn "Could not locate server.properties (checked common paths and systemd)."
else
  pass "Using server.properties: $SERVER_PROPS"

  get_prop(){
    local file="$1" key="$2"
    awk -v k="$key" -F= '
      BEGIN{IGNORECASE=1}
      $0 ~ /^[[:space:]]*#/ {next}
      $0 !~ /./ {next}
      {
        gsub(/[[:space:]]+/,"",$1);
        if (tolower($1)==tolower(k)) {
          sub(/^[[:space:]]*/,"",$2); sub(/[[:space:]]*$/,"",$2);
          print $2;
        }
      }' "$file" | tail -n1
  }

  # CPU and data dirs
  VCPU=$(nproc 2>/dev/null || echo 1)
  logdirs=$(get_prop "$SERVER_PROPS" "log.dirs")
  DATA_DIRS=1
  if [[ -n "${logdirs:-}" ]]; then
    IFS=',' read -r -a LDIRS <<<"$logdirs"
    DATA_DIRS="${#LDIRS[@]}"
    printf "log.dirs: %s (data dirs: %d)\n" "$logdirs" "$DATA_DIRS"
  else
    info "log.dirs not set (Kafka may use default). Assuming data dirs: $DATA_DIRS"
  fi

  # Recommendations
  # - network threads: max(3, round(vcpu/4)), clamp to [3,32]
  # - io threads:      max(8, vcpu)
  # - replica fetchers: 2 (or 3 if vcpu>=16)
  # - recovery per dir: = #data_dirs (>=1)
  REC_NET_RAW=$(( (VCPU + 3) / 4 ))
  REC_NET=$(clamp "${REC_NET_RAW:-3}" 3 32)
  REC_IO=$(( VCPU >= 8 ? VCPU : 8 ))
  REC_REPF=$(( VCPU >= 16 ? 3 : 2 ))
  REC_RECOVERY=$(( DATA_DIRS >= 1 ? DATA_DIRS : 1 ))

  printf "vCPUs: %d | Recommended -> num.network.threads=%d, num.io.threads=%d, num.replica.fetchers=%d, num.recovery.threads.per.data.dir=%d\n" \
    "$VCPU" "$REC_NET" "$REC_IO" "$REC_REPF" "$REC_RECOVERY"

  check_key(){
    local key="$1" want="$2"
    local val
    val=$(get_prop "$SERVER_PROPS" "$key" || true)
    if [[ -z "${val:-}" ]]; then
      warn "$key is NOT set (Kafka will use default). Recommended: $want"
      return
    fi
    # extract first integer from value
    local num
    num=$(awk 'match($0,/[0-9]+/){print substr($0,RSTART,RLENGTH)}' <<<"$val")
    if [[ -z "${num:-}" ]]; then
      warn "$key has a non-integer value: '$val' (Recommended: $want)"
      return
    fi
    if (( num < want )); then
      warn "$key=$num (LOW). Recommended: >= $want"
    else
      pass "$key=$num (OK, >= $want)"
    fi
  }

  check_key "num.network.threads" "$REC_NET"
  check_key "num.io.threads" "$REC_IO"
  check_key "num.replica.fetchers" "$REC_REPF"
  check_key "num.recovery.threads.per.data.dir" "$REC_RECOVERY"

  echo
  info "Scanning systemd environment for overrides (if any):"
  for unit in confluent-server kafka confluent-kafka cp-kafka; do
    systemctl show "$unit" -p Environment >/tmp/.env.$$ 2>/dev/null || true
    if [[ -s /tmp/.env.$$ ]]; then
      echo "  Unit: $unit"
      grep -E 'num\.network\.threads|num\.io\.threads|num\.replica\.fetchers|num\.recovery\.threads\.per\.data\.dir' /tmp/.env.$$ || echo "    (no overrides found)"
    fi
  done
  rm -f /tmp/.env.$$
fi

# ---------- (I) KAFKA BROKER PROCESS ----------
section "KAFKA BROKER PROCESS"
if pgrep -f kafka.Kafka >/dev/null; then
  pass "Kafka broker process detected."
else
  warn "Kafka broker process not detected."
fi

# ---------- SUMMARY ----------
hr
printf "%s\n" "${BOLD}SUMMARY${RESET}"
printf "• server.properties: values checked & compared against CPU/data-dir-based recommendations.\n"
printf "• If WARN for any key, consider adjusting to >= Recommended.\n"
printf "• Review other WARN/FAIL above (tuned, sysctl, rings, ulimits, Java/Confluent).\n"
hr

exit 0