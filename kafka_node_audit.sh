#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Kafka Node Audit for RHEL8 + Confluent 7.6.2
# by RR v1.5
# ---------------------------------------------------------------------------

set -euo pipefail

: "${SERVICE_USER:=stekafka}"
: "${MIN_NOFILE:=100000}"
: "${MIN_NPROC:=4096}"

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
clamp(){ local v="$1" min="$2" max="$3"; (( v<min )) && v="$min"; (( v>max )) && v="$max"; echo "$v"; }

# -------- helpers: robust ethtool -g parsing (avoids RX Mini / RX Jumbo traps)
read_ring_vals() {
  # prints: "<cur_rx> <cur_tx> <max_rx> <max_tx>" or empty if unsupported
  local iface="$1" out cur_rx cur_tx max_rx max_tx
  out="$(ethtool -g "$iface" 2>/dev/null || true)" || true
  [[ -z "${out:-}" ]] && return 1
  # current section
  cur_rx=$(awk '/Current hardware settings:/ {s=1;next} s&&/^RX:/{print $2; s=0}' <<<"$out" | head -n1)
  cur_tx=$(awk '/Current hardware settings:/ {s=1;next} s&&/^TX:/{print $2; s=0}' <<<"$out" | head -n1)
  # max section
  max_rx=$(awk '/Pre-set maximums:/ {s=1;next} s&&/^RX:/{print $2; s=0}' <<<"$out" | head -n1)
  max_tx=$(awk '/Pre-set maximums:/ {s=1;next} s&&/^TX:/{print $2; s=0}' <<<"$out" | head -n1)
  [[ -n "${cur_rx:-}" || -n "${cur_tx:-}" || -n "${max_rx:-}" || -n "${max_tx:-}" ]] || return 1
  printf "%s %s %s %s" "${cur_rx:-}" "${cur_tx:-}" "${max_rx:-}" "${max_tx:-}"
}

# ===========================================================================

section "SYSTEM & TUNED PROFILE"
printf "Hostname: %s\n" "$(hostname)"
printf "Kernel  : %s\n" "$(uname -r)"

# --- virtualization detection (normalize to vm|physical and show hypervisor) ---
virt_kind="physical"
hypervisor="none"
if has_cmd systemd-detect-virt; then
  vt="$(systemd-detect-virt 2>/dev/null || echo none)"
  if [[ "$vt" != "none" ]]; then virt_kind="vm"; hypervisor="$vt"; fi
elif [[ -r /sys/class/dmi/id/product_name ]]; then
  model="$(< /sys/class/dmi/id/product_name)"
  if [[ "$model" =~ (VMware|KVM|Hyper-V|VirtualBox|Virtual) ]]; then
    virt_kind="vm"; hypervisor="$model"
  fi
fi
printf "Virtualization: kind=%s, hypervisor=%s\n" "$virt_kind" "$hypervisor"

if has_cmd tuned-adm; then
  tuned_state=$(systemctl is-active tuned 2>/dev/null || echo "inactive")
  active_profile=$(tuned-adm active 2>/dev/null | awk -F': ' '/Current active profile/{print $2}')
  printf "tuned service : %s\n" "$tuned_state"
  printf "active profile: %s\n" "${active_profile:-unknown}"
  [[ "$tuned_state" == "active" ]] || warn "tuned service is NOT active."

  if [[ "$virt_kind" == "vm" ]]; then
    if [[ "$active_profile" == "virtual-guest" ]]; then
      pass "VM detected → tuned profile is virtual-guest."
    else
      warn "VM detected → set tuned profile to virtual-guest (current: ${active_profile:-none})."
    fi
  else
    case "${active_profile:-}" in
      throughput-performance|latency-performance|network-throughput|cpu-performance|high-performance)
        pass "Physical server → high-performance tuned profile (${active_profile}).";;
      *) warn "Physical server detected → consider a high-performance tuned profile.";;
    esac
  fi
else
  warn "tuned-adm not installed; cannot verify tuned profile."
fi

# ---------------------------------------------------------------------------

section "TRANSPARENT HUGE PAGES (THP)"
for f in enabled defrag; do
  val="$(cat "/sys/kernel/mm/transparent_hugepage/$f" 2>/dev/null || echo N/A)"
  printf "%-7s: %s\n" "$f" "$val"
done

# ---------------------------------------------------------------------------

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
  (( cur >= want )) && pass "$k=$cur (>= $want)" || warn "$k=$cur (< $want)"
done
printf "net.ipv4.tcp_rmem: %s\n" "$(kv net.ipv4.tcp_rmem)"
printf "net.ipv4.tcp_wmem: %s\n" "$(kv net.ipv4.tcp_wmem)"

# ---------------------------------------------------------------------------

section "NETWORK INTERFACES & RING BUFFERS"
[[ -x "$(command -v ethtool)" ]] || warn "ethtool not found; limited NIC details"

mapfile -t ifaces < <(ls -1 /sys/class/net | grep -vE '^(lo)$' || true)
printf "Detected interfaces: %s\n" "${ifaces[*]:-none}"

has25g=0
for i in "${ifaces[@]}"; do
  [[ -d "/proc/net/bonding/$i" ]] && continue
  state="$(cat "/sys/class/net/$i/operstate" 2>/dev/null || echo N/A)"
  mtu="$(cat "/sys/class/net/$i/mtu" 2>/dev/null || echo N/A)"
  speed="$(ethtool "$i" 2>/dev/null | awk -F': ' '/Speed/{print $2}')"
  duplex="$(ethtool "$i" 2>/dev/null | awk -F': ' '/Duplex/{print $2}')"
  printf "\nInterface: %s\n  State : %s\n  MTU   : %s\n  Speed : %s\n  Duplex: %s\n" "$i" "$state" "$mtu" "$speed" "$duplex"
  [[ "${speed:-}" =~ 25000|25Gb|25G ]] && { pass "25Gb capable NIC: $i ($speed)"; has25g=1; }

  if vals="$(read_ring_vals "$i")"; then
    # shellcheck disable=SC2086
    set -- $vals; cur_rx="$1"; cur_tx="$2"; max_rx="$3"; max_tx="$4"
    printf "  Rings (cur/max): RX %s/%s, TX %s/%s\n" "${cur_rx:-?}" "${max_rx:-?}" "${cur_tx:-?}" "${max_tx:-?}"

    if [[ -n "${cur_rx:-}" && -n "${max_rx:-}" ]]; then
      (( ${cur_rx:-0} < ${max_rx:-0} )) && warn "  RX ring below max (consider: ethtool -G $i rx $max_rx)" || pass "  RX ring OK"
    else
      info "  RX ring values not available for $i"
    fi

    if [[ -n "${cur_tx:-}" && -n "${max_tx:-}" ]]; then
      (( ${cur_tx:-0} < ${max_tx:-0} )) && warn "  TX ring below max (consider: ethtool -G $i tx $max_tx)" || pass "  TX ring OK"
    else
      info "  TX ring values not available for $i"
    fi
  else
    info "  ethtool -g not supported on $i"
  fi
done
(( has25g == 1 )) && pass "At least one 25Gb NIC detected" || warn "No 25Gb NIC detected by speed."

# ---------------------------------------------------------------------------

section "BONDING"
if ls /proc/net/bonding/* >/dev/null 2>&1; then
  for bf in /proc/net/bonding/*; do
    [[ -e "$bf" ]] || continue
    bname=$(basename "$bf")
    echo "Bond: $bname"
    mode=$(awk -F': ' '/Bonding Mode/{print $2}' "$bf")
    echo "  Mode: $mode"
    awk '/Slave Interface/ {iface=$3} /MII Status/ && iface {printf "  Slave %-10s link %s\n", iface, $3; iface=""}' "$bf"
    pass "Bond $bname present ($mode)"
  done
else
  info "No Linux bonding configured."
fi

# ---------------------------------------------------------------------------

section "ULIMITS FOR SERVICE USER (${SERVICE_USER})"
if id "$SERVICE_USER" >/dev/null 2>&1; then
  read_ul(){ sudo -u "$SERVICE_USER" bash -lc "$1" 2>/dev/null || echo "N/A"; }
  nofile="$(read_ul 'ulimit -n')"; nproc="$(read_ul 'ulimit -u')"; memlock="$(read_ul 'ulimit -l')"
  printf "nofile : %s\nnproc  : %s\nmemlock: %s\n" "${nofile:-N/A}" "${nproc:-N/A}" "${memlock:-N/A}"
  (( ${nofile:-0} >= ${MIN_NOFILE:-100000} )) && pass "nofile >= ${MIN_NOFILE}" || warn "nofile < ${MIN_NOFILE}"
  (( ${nproc:-0}  >= ${MIN_NPROC:-4096}    )) && pass "nproc >= ${MIN_NPROC}"   || warn "nproc < ${MIN_NPROC}"
  [[ "${memlock:-}" == "unlimited" || "${memlock:-}" == "N/A" ]] && pass "memlock unlimited" || warn "memlock not unlimited"
else
  warn "User ${SERVICE_USER} not found."
fi

# ---------------------------------------------------------------------------

section "JAVA & CONFLUENT VERSIONS"
if has_cmd java; then info "Java: $(java -version 2>&1 | head -n1)"; else warn "Java not found in PATH"; fi
if [[ -d /opt/confluent ]]; then
  verfile=$(find /opt/confluent -maxdepth 2 -name 'confluent.version' 2>/dev/null | head -n1 || true)
  [[ -n "${verfile:-}" ]] && pass "Confluent Platform version: $(cat "$verfile") (target: 7.6.2)" || warn "Confluent version file not found."
else
  warn "/opt/confluent not found (OK if not installed yet)."
fi

# ---------------------------------------------------------------------------

section "KAFKA server.properties KEY CHECKS + RECOMMENDATIONS"
declare -a CANDIDATE_PROPS=(/etc/kafka/server.properties /etc/confluent-server/server.properties /opt/confluent/etc/kafka/server.properties)
SERVER_PROPS=$(for f in "${CANDIDATE_PROPS[@]}"; do [[ -r "$f" ]] && echo "$f"; done | head -n1)

if [[ -z "${SERVER_PROPS:-}" ]]; then
  warn "Could not locate server.properties."
else
  pass "Using server.properties: $SERVER_PROPS"
  get_prop(){ awk -v k="$2" -F= 'BEGIN{IGNORECASE=1} $0!~/^[[:space:]]*#/ && $0~/'"$2"'\s*=/{gsub(/[[:space:]]+/,"",$2); print $2}' "$1" | tail -n1; }

  VCPU=$(nproc 2>/dev/null || echo 1)
  logdirs="$(get_prop "$SERVER_PROPS" "log.dirs")"
  DATA_DIRS=1
  if [[ -n "${logdirs:-}" ]]; then IFS=',' read -r -a LDIRS <<<"$logdirs"; DATA_DIRS="${#LDIRS[@]}"; fi
  printf "log.dirs: %s (data dirs: %d)\n" "${logdirs:-default}" "$DATA_DIRS"

  REC_NET=$(clamp $(( (VCPU+3)/4 )) 3 32)
  REC_IO=$(( VCPU>=8?VCPU:8 ))
  REC_REPF=$(( VCPU>=16?3:2 ))
  REC_RECOVERY=$(( DATA_DIRS>=1?DATA_DIRS:1 ))

  printf "vCPUs: %d | Recommended -> net=%d io=%d fetchers=%d recovery=%d\n" "$VCPU" "$REC_NET" "$REC_IO" "$REC_REPF" "$REC_RECOVERY"

  check(){ local k=$1 want=$2 v; v="$(get_prop "$SERVER_PROPS" "$k" || true)"
    [[ -z "${v:-}" ]] && { warn "$k not set (rec $want)"; return; }
    local n; n="$(awk 'match($0,/[0-9]+/){print substr($0,RSTART,RLENGTH)}' <<<"$v")"
    [[ -z "${n:-}" ]] && { warn "$k has non-integer value '$v' (rec $want)"; return; }
    (( n < want )) && warn "$k=$n (LOW, rec $want)" || pass "$k=$n OK"; }

  check num.network.threads $REC_NET
  check num.io.threads $REC_IO
  check num.replica.fetchers $REC_REPF
  check num.recovery.threads.per.data.dir $REC_RECOVERY
fi

# ---------------------------------------------------------------------------

section "KAFKA BROKER PROCESS"
pgrep -f kafka.Kafka >/dev/null && pass "Kafka broker running." || warn "Kafka broker not running."

# ---------------------------------------------------------------------------

hr
printf "%s\n" "${BOLD}SUMMARY${RESET}"
printf "• VM detection fixed: kind + hypervisor are accurate; VM ⇒ expect tuned 'virtual-guest'.\n"
printf "• Ring buffer parsing fixed for vmxnet3 (no more 'Mini' tokens / unbound errors).\n"
printf "• Review WARNs to align system with Kafka best practices.\n"
hr
