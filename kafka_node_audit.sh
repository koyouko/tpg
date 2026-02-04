#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Kafka Node Audit for RHEL8 + Confluent 7.6.2 +
# Read-only data gathering and compliance checking script
#
# Usage:
#   ./kafka-node-audit.sh [OPTIONS]
#
# Options:
#   --output=FILE       Write output to specified file (in addition to stdout)
#   --json              Output results in JSON format
#   --skip-sudo-check   Skip checks that require sudo access
#   --help              Show this help message
#
# Requirements:
#   - Passwordless sudo for ulimit checks (optional, will warn and skip if unavailable)
#
# Environment Variables:
#   SERVICE_USER        Kafka service user (default: auto-detect from running process)
#   MIN_NOFILE          Minimum open files limit (default: 100000 per Confluent)
#   MIN_NPROC           Minimum process limit (default: 65536 per cp-ansible)
#   CONFLUENT_HOME      Confluent installation path (default: /opt/confluent)
#   SERVER_PROPS_PATH   Path to server.properties (auto-detected if not set)
#
# Based on Confluent Production Deployment Guide:
#   https://docs.confluent.io/platform/current/kafka/deployment.html
# Based on cp-ansible defaults:
#   https://github.com/confluentinc/cp-ansible
# ---------------------------------------------------------------------------

set -euo pipefail

# Configuration with defaults
: "${SERVICE_USER:=}"  # Will auto-detect from running Kafka process
: "${MIN_NOFILE:=100000}"  # Confluent Production Recommendation: 100,000 minimum
: "${MIN_NPROC:=65536}"    # cp-ansible standard: 65,536 (increased from 4096)
: "${CONFLUENT_HOME:=/opt/confluent}"
: "${SERVER_PROPS_PATH:=}"

# Auto-detect Kafka service user from running process
if [[ -z "$SERVICE_USER" ]]; then
  KAFKA_PID=$(pgrep -f "kafka\.Kafka" | head -n1)
  if [[ -n "$KAFKA_PID" ]]; then
    SERVICE_USER=$(ps -o user= -p "$KAFKA_PID" 2>/dev/null | tr -d ' ')
    if [[ -z "$SERVICE_USER" ]]; then
      SERVICE_USER="stekafka"  # Fallback to default
    fi
  else
    SERVICE_USER="stekafka"  # Fallback if Kafka not running
  fi
fi

# Counters for summary
PASS_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0
INFO_COUNT=0

# Portable color using tput with fallback to ANSI
if command -v tput >/dev/null 2>&1 && [[ -t 1 ]]; then
  BOLD=$(tput bold 2>/dev/null || echo "\033[1m")
  RESET=$(tput sgr0 2>/dev/null || echo "\033[0m")
  GREEN=$(tput setaf 2 2>/dev/null || echo "\033[32m")
  YELLOW=$(tput setaf 3 2>/dev/null || echo "\033[33m")
  RED=$(tput setaf 1 2>/dev/null || echo "\033[31m")
  BLUE=$(tput setaf 4 2>/dev/null || echo "\033[34m")
  NC="$RESET"
else
  BOLD="\033[1m"
  RESET="\033[0m"
  GREEN="\033[32m"
  YELLOW="\033[33m"
  RED="\033[31m"
  BLUE="\033[34m"
  NC="\033[0m"
fi

# Output helpers
pass() { 
  printf "%b[PASS]%b %s\n" "${GREEN}" "${NC}" "$1"
  ((PASS_COUNT++))
}

warn() { 
  printf "%b[WARN]%b %s\n" "${YELLOW}" "${NC}" "$1"
  ((WARN_COUNT++))
}

fail() { 
  printf "%b[FAIL]%b %s\n" "${RED}" "${NC}" "$1"
  ((FAIL_COUNT++))
}

info() { 
  printf "%b[INFO]%b %s\n" "${BLUE}" "${NC}" "$1"
  ((INFO_COUNT++))
}

hr() { 
  printf "\n%s\n" "------------------------------------------------------------"
}

section() { 
  hr
  printf "%s\n" "${BOLD}$1${RESET}"
  hr
}

# Utility functions
has_cmd() { 
  command -v "$1" >/dev/null 2>&1
}

kv() { 
  sysctl -n "$1" 2>/dev/null || echo "N/A"
}

clamp() { 
  local v="$1" min="$2" max="$3"
  ((v < min)) && v="$min"
  ((v > max)) && v="$max"
  echo "$v"
}

# Read network ring buffer values from ethtool
read_ring_vals() {
  local iface="$1" out cur_rx cur_tx max_rx max_tx
  
  if ! has_cmd ethtool; then
    return 1
  fi
  
  out="$(ethtool -g "$iface" 2>/dev/null || true)"
  [[ -z "${out:-}" ]] && return 1
  
  cur_rx=$(awk '/Current hardware settings:/ {s=1;next} s&&/^RX:/{print $2; exit}' <<<"$out")
  cur_tx=$(awk '/Current hardware settings:/ {s=1;next} s&&/^TX:/{print $2; exit}' <<<"$out")
  max_rx=$(awk '/Pre-set maximums:/ {s=1;next} s&&/^RX:/{print $2; exit}' <<<"$out")
  max_tx=$(awk '/Pre-set maximums:/ {s=1;next} s&&/^TX:/{print $2; exit}' <<<"$out")
  
  # Validate we got at least some values
  if [[ -z "${cur_rx:-}" && -z "${cur_tx:-}" && -z "${max_rx:-}" && -z "${max_tx:-}" ]]; then
    return 1
  fi
  
  printf "%s %s %s %s" "${cur_rx:-0}" "${cur_tx:-0}" "${max_rx:-0}" "${max_tx:-0}"
}

# Show help
show_help() {
  sed -n '2,/^# --------/p' "$0" | sed 's/^# \?//'
  exit 0
}

# Parse command-line arguments
OUTPUT_FILE=""
JSON_OUTPUT=0
SKIP_SUDO_CHECK=0

for arg in "$@"; do
  case "$arg" in
    --output=*)
      OUTPUT_FILE="${arg#--output=}"
      ;;
    --json)
      JSON_OUTPUT=1
      ;;
    --skip-sudo-check)
      SKIP_SUDO_CHECK=1
      ;;
    --help|-h)
      show_help
      ;;
    *)
      echo "Unknown option: $arg"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Setup output redirection if needed
if [[ -n "$OUTPUT_FILE" ]]; then
  exec > >(tee -a "$OUTPUT_FILE")
  exec 2>&1
  info "Output will be written to: $OUTPUT_FILE"
fi

# JSON output setup (simplified for now, full implementation would require jq or similar)
if ((JSON_OUTPUT == 1)); then
  warn "JSON output format not yet implemented, using standard format"
fi

# =============================================================================
# SYSTEM & TUNED PROFILE
# =============================================================================
section "SYSTEM & TUNED PROFILE"

printf "Hostname: %s\n" "$(hostname)"
printf "Kernel  : %s\n" "$(uname -r)"
printf "Date    : %s\n" "$(date '+%Y-%m-%d %H:%M:%S %Z')"

# Get OS release info
if [[ -r /etc/os-release ]]; then
  # shellcheck source=/dev/null
  . /etc/os-release
  printf "OS      : %s %s\n" "${NAME:-Unknown}" "${VERSION:-}"
fi

# Detect virtualization
virt_kind="physical"
hypervisor="none"

if has_cmd systemd-detect-virt; then
  vt="$(systemd-detect-virt 2>/dev/null || echo none)"
  if [[ "$vt" != "none" ]]; then 
    virt_kind="vm"
    hypervisor="$vt"
  fi
elif [[ -r /sys/class/dmi/id/product_name ]]; then
  model="$(cat /sys/class/dmi/id/product_name 2>/dev/null || echo unknown)"
  if [[ "$model" =~ (VMware|KVM|Hyper-V|VirtualBox|Virtual) ]]; then
    virt_kind="vm"
    hypervisor="$model"
  fi
fi

printf "Virtualization: kind=%s, hypervisor=%s\n" "$virt_kind" "$hypervisor"

# Check tuned profile
if has_cmd tuned-adm; then
  tuned_state=$(systemctl is-active tuned 2>/dev/null || echo "inactive")
  active_profile=$(tuned-adm active 2>/dev/null | awk -F': ' '/Current active profile/{print $2}' | tr -d ' ')
  
  printf "tuned service : %s\n" "$tuned_state"
  printf "active profile: %s\n" "${active_profile:-unknown}"
  
  if [[ "$tuned_state" != "active" ]]; then
    warn "tuned service is NOT active"
    info "  Recommendation: systemctl start tuned && systemctl enable tuned"
  else
    pass "tuned service is active"
  fi
  
  # Profile recommendations based on VM vs physical
  if [[ "$virt_kind" == "vm" ]]; then
    if [[ "$active_profile" == "virtual-guest" ]]; then
      pass "VM → tuned profile 'virtual-guest' is active (recommended)"
    else
      warn "VM → tuned profile should be 'virtual-guest' (current: ${active_profile:-none})"
      info "  Recommendation: tuned-adm profile virtual-guest"
    fi
  else
    case "${active_profile:-}" in
      throughput-performance|latency-performance|network-throughput|cpu-performance|high-performance)
        pass "Physical → high-performance tuned profile (${active_profile}) is active"
        ;;
      *)
        warn "Physical → should use a high-performance tuned profile (current: ${active_profile:-none})"
        info "  Recommendation: tuned-adm profile throughput-performance"
        ;;
    esac
  fi
else
  warn "tuned-adm not installed"
  info "  Recommendation: yum install tuned"
fi

# =============================================================================
# TRANSPARENT HUGE PAGES (THP)
# =============================================================================
section "TRANSPARENT HUGE PAGES (THP)"

for f in enabled defrag; do
  thp_path="/sys/kernel/mm/transparent_hugepage/$f"
  if [[ -r "$thp_path" ]]; then
    val="$(cat "$thp_path" 2>/dev/null || echo N/A)"
    printf "%-7s: %s\n" "$f" "$val"
    
    if [[ "$val" == *"[never]"* ]]; then
      pass "THP $f set to '[never]' (recommended for Kafka)"
    else
      warn "THP $f not set to '[never]' (current: $val)"
      info "  Recommendation: Disable THP for Kafka workloads"
      info "  Runtime: echo never > $thp_path"
      info "  Persistent: Add 'transparent_hugepage=never' to kernel boot parameters"
    fi
  else
    warn "Cannot read $thp_path"
  fi
done

# =============================================================================
# SYSCTL CHECKS (Kafka/cp-ansible recommendations)
# =============================================================================
section "SYSCTL KERNEL PARAMETERS"

# Combined recommendations (cp-ansible + Kafka best practices)
declare -A SYSCTL_RECS=(
  [vm.swappiness]=1
  [vm.max_map_count]=262144
  [net.core.netdev_max_backlog]=250000
  [net.core.rmem_max]=134217728
  [net.core.wmem_max]=134217728
  [fs.file-max]=1000000
  [net.core.somaxconn]=65535
  [net.ipv4.tcp_rmem]="4096 87380 67108864"
  [net.ipv4.tcp_wmem]="4096 65536 67108864"
)

for k in "${!SYSCTL_RECS[@]}"; do
  cur=$(kv "$k")
  want=${SYSCTL_RECS[$k]}
  
  if [[ "$cur" == "N/A" ]]; then
    warn "$k not readable"
    continue
  fi
  
  # Handle space-separated values (tcp_rmem, tcp_wmem)
  if [[ "$want" == *" "* ]]; then
    if [[ "$cur" == "$want" ]]; then
      pass "$k='$cur' (matches recommended)"
    else
      warn "$k='$cur' (recommended: '$want')"
      info "  Recommendation: sysctl -w $k=\"$want\""
      info "  Persist in: /etc/sysctl.d/99-kafka.conf"
    fi
  else
    # Numeric comparison
    if ((cur >= want)); then
      pass "$k=$cur (>= recommended $want)"
    else
      warn "$k=$cur (< recommended $want)"
      info "  Recommendation: sysctl -w $k=$want"
      info "  Persist in: /etc/sysctl.d/99-kafka.conf"
    fi
  fi
done

# =============================================================================
# NETWORK INTERFACES & RING BUFFERS
# =============================================================================
section "NETWORK INTERFACES & RING BUFFERS"

if ! has_cmd ethtool; then
  warn "ethtool not found"
  info "  Recommendation: yum install ethtool"
fi

# Filter out loopback and common virtual interfaces
mapfile -t ifaces < <(
  ls -1 /sys/class/net 2>/dev/null | grep -vE '^(lo|docker|virbr|veth|br-)' || true
)

if ((${#ifaces[@]} == 0)); then
  warn "No non-loopback network interfaces detected"
else
  info "Detected interfaces: ${ifaces[*]}"
fi

has25g=0
has10g=0
declare -A iface_25g
declare -A iface_10g

for i in "${ifaces[@]}"; do
  # Skip bonding master interfaces (will check them separately)
  if [[ -d "/proc/net/bonding/$i" ]]; then
    continue
  fi
  
  state="$(cat "/sys/class/net/$i/operstate" 2>/dev/null || echo N/A)"
  mtu="$(cat "/sys/class/net/$i/mtu" 2>/dev/null || echo N/A)"
  
  speed=""
  duplex=""
  if has_cmd ethtool; then
    speed="$(ethtool "$i" 2>/dev/null | awk -F': ' '/Speed/{print $2}' | tr -d ' ')"
    duplex="$(ethtool "$i" 2>/dev/null | awk -F': ' '/Duplex/{print $2}' | tr -d ' ')"
  fi
  
  printf "\nInterface: %s\n" "$i"
  printf "  State : %s\n" "$state"
  printf "  MTU   : %s\n" "$mtu"
  printf "  Speed : %s\n" "${speed:-unknown}"
  printf "  Duplex: %s\n" "${duplex:-unknown}"
  
  # Check for high-speed NICs
  if [[ "${speed//[[:space:]]/}" =~ ^25000Mb/s|^25Gb ]]; then
    pass "  25Gb capable NIC detected"
    has25g=1
    iface_25g["$i"]=1
  elif [[ "${speed//[[:space:]]/}" =~ ^10000Mb/s|^10Gb ]]; then
    info "  10Gb NIC detected"
    has10g=1
    iface_10g["$i"]=1
  fi
  
  # Check MTU for jumbo frames
  if [[ "$mtu" =~ ^[0-9]+$ ]]; then
    if ((mtu >= 9000)); then
      pass "  Jumbo frames enabled (MTU: $mtu)"
    elif ((mtu == 1500)); then
      info "  Standard MTU (1500) - consider jumbo frames (9000) for Kafka"
    else
      warn "  Non-standard MTU: $mtu"
    fi
  fi
  
  # Check ring buffers
  if vals="$(read_ring_vals "$i")"; then
    read -r cur_rx cur_tx max_rx max_tx <<<"$vals"
    printf "  Rings (cur/max): RX %s/%s, TX %s/%s\n" "$cur_rx" "$max_rx" "$cur_tx" "$max_tx"
    
    if ((max_rx > 0)) && ((cur_rx < max_rx)); then
      warn "  RX ring buffer below maximum"
      info "  Recommendation: ethtool -G $i rx $max_rx"
    elif ((max_rx > 0)); then
      pass "  RX ring buffer at maximum"
    fi
    
    if ((max_tx > 0)) && ((cur_tx < max_tx)); then
      warn "  TX ring buffer below maximum"
      info "  Recommendation: ethtool -G $i tx $max_tx"
    elif ((max_tx > 0)); then
      pass "  TX ring buffer at maximum"
    fi
  else
    info "  Ring buffer info not available for $i"
  fi
done

if ((has25g == 1)); then
  pass "At least one 25Gb NIC detected"
elif ((has10g == 1)); then
  info "10Gb NICs detected (25Gb recommended for high-throughput Kafka)"
else
  warn "No 25Gb or 10Gb NIC detected (may limit Kafka performance)"
  info "  Recommendation: Consider upgrading to 10Gb or 25Gb NICs for production Kafka"
fi

# =============================================================================
# ROUTING & STATIC ROUTES
# =============================================================================
section "ROUTING & STATIC ROUTES"

if ! has_cmd ip; then
  warn "'ip' command not available; cannot inspect routing table"
else
  default_route=$(ip route show default 2>/dev/null | head -n1)
  printf "Default Route : %s\n" "${default_route:-none}"
  
  default_gw=$(awk '/^default / {for (i=1;i<=NF;i++) if ($i=="via") print $(i+1)}' <<<"$default_route")
  default_iface=$(awk '/^default / {for (i=1;i<=NF;i++) if ($i=="dev") print $(i+1)}' <<<"$default_route")
  printf "Default Gateway: %s\n" "${default_gw:-none}"
  printf "Default Interface: %s\n" "${default_iface:-none}"
  
  # Get static routes (excluding default, linkdown, unreachable)
  mapfile -t static_routes < <(
    ip route 2>/dev/null | grep -v '^default' | grep -v '^linkdown' | grep -v '^unreachable' || true
  )
  
  if ((${#static_routes[@]} > 0)); then
    info "Static routes found: ${#static_routes[@]}"
    for route in "${static_routes[@]}"; do
      printf "  %s\n" "$route"
    done
    
    # Check if any static routes use high-speed interfaces
    found_static_highspeed=0
    for route in "${static_routes[@]}"; do
      iface=$(awk '{for (i=1;i<=NF;i++) {if ($i=="dev") print $(i+1)}}' <<<"$route")
      if [[ -n "${iface_25g[$iface]:-}" ]]; then
        pass "Static route uses 25Gb interface: $route"
        found_static_highspeed=1
      elif [[ -n "${iface_10g[$iface]:-}" ]]; then
        info "Static route uses 10Gb interface: $route"
        found_static_highspeed=1
      fi
    done
    
    if ((found_static_highspeed == 0)) && ((has25g == 1 || has10g == 1)); then
      warn "No static routes configured via high-speed network interfaces"
      info "  Recommendation: Route Kafka traffic through 10Gb/25Gb interfaces"
    fi
  else
    info "No static routes found (using default routing)"
  fi
fi

# =============================================================================
# BONDING
# =============================================================================
section "BONDING"

if compgen -G "/proc/net/bonding/*" >/dev/null 2>&1; then
  for bf in /proc/net/bonding/*; do
    [[ -e "$bf" ]] || continue
    bname=$(basename "$bf")
    
    echo ""
    info "Bond: $bname"
    mode=$(awk -F': ' '/Bonding Mode/{print $2}' "$bf")
    echo "  Mode: $mode"
    
    # Show slave interfaces and their status
    awk '/Slave Interface/ {iface=$3} 
         /MII Status/ && iface {printf "  Slave %-10s link %s\n", iface, $3; iface=""}' "$bf"
    
    pass "Bond $bname configured ($mode)"
    
    # Recommendation for Kafka
    if [[ "$mode" =~ 802\.3ad|LACP|mode[[:space:]]4 ]]; then
      pass "  LACP/802.3ad mode is optimal for Kafka throughput"
    elif [[ "$mode" =~ balance-rr|mode[[:space:]]0 ]]; then
      info "  balance-rr mode: verify switch configuration supports this"
    elif [[ "$mode" =~ active-backup|mode[[:space:]]1 ]]; then
      warn "  active-backup mode: consider LACP for better throughput"
      info "  Recommendation: Configure 802.3ad LACP bonding for Kafka"
    fi
  done
else
  info "No Linux bonding configured"
  info "  Recommendation: Consider NIC bonding for redundancy and throughput"
fi

# =============================================================================
# ULIMITS FOR SERVICE USER
# =============================================================================
section "ULIMITS FOR SERVICE USER (${SERVICE_USER})"

# Check sudo availability
SUDO_OK=false
if ((SKIP_SUDO_CHECK == 0)); then
  if sudo -n true 2>/dev/null; then
    SUDO_OK=true
  else
    warn "Passwordless sudo not available; skipping runtime ulimit checks"
    info "  For accurate runtime checks, grant NOPASSWD sudo access"
  fi
fi

if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  warn "User ${SERVICE_USER} not found on this system"
  info "  Recommendation: useradd -r -s /bin/bash $SERVICE_USER"
else
  pass "Service user ${SERVICE_USER} exists"
  
  # Runtime ulimit checks (requires sudo)
  if [[ "$SUDO_OK" == "true" ]]; then
    read_ul() { 
      sudo -n -u "$SERVICE_USER" bash -lc "$1" 2>/dev/null || echo "N/A"
    }
    
    nofile="$(read_ul 'ulimit -n')"
    nproc="$(read_ul 'ulimit -u')"
    memlock="$(read_ul 'ulimit -l')"
    
    printf "\nCurrent (runtime) ulimit values:\n"
    printf "  nofile : %s\n" "${nofile:-N/A}"
    printf "  nproc  : %s\n" "${nproc:-N/A}"
    printf "  memlock: %s\n" "${memlock:-N/A}"
    
    # Validate nofile
    if [[ "${nofile:-}" =~ ^[0-9]+$ ]]; then
      if ((nofile >= MIN_NOFILE)); then
        pass "nofile ($nofile) >= recommended minimum ($MIN_NOFILE)"
      else
        warn "nofile ($nofile) < recommended minimum ($MIN_NOFILE)"
      fi
    else
      warn "nofile not numeric or unavailable (got '${nofile:-N/A}')"
    fi
    
    # Validate nproc
    if [[ "${nproc:-}" =~ ^[0-9]+$ ]]; then
      if ((nproc >= MIN_NPROC)); then
        pass "nproc ($nproc) >= recommended minimum ($MIN_NPROC)"
      else
        warn "nproc ($nproc) < recommended minimum ($MIN_NPROC)"
      fi
    else
      warn "nproc not numeric or unavailable (got '${nproc:-N/A}')"
    fi
    
    # Validate memlock
    if [[ "${memlock:-}" == "unlimited" ]]; then
      pass "memlock unlimited (recommended)"
    elif [[ "${memlock:-}" == "N/A" ]]; then
      warn "memlock value unavailable"
    elif [[ "${memlock:-}" =~ ^[0-9]+$ ]]; then
      warn "memlock is finite (${memlock}); should be unlimited for Kafka"
    else
      warn "memlock unknown (got '${memlock:-N/A}')"
    fi
  else
    info "Skipping runtime ulimit checks (sudo not available)"
  fi
  
  # Persistent limits check (doesn't require sudo)
  hr
  echo "Persistent limits (from /etc/security/limits.conf and limits.d/*.conf):"
  
  found_limits=0
  for file in /etc/security/limits.conf /etc/security/limits.d/*.conf; do
    [[ -f "$file" ]] || continue
    
    matches="$(awk -v u="$SERVICE_USER" '
      $0 !~ /^[[:space:]]*#/ && NF>=4 {
        usr=$1; typ=$2; itm=$3; val=$4
        if ((usr==u || usr=="*") && (itm=="nofile" || itm=="nproc" || itm=="memlock")) {
          printf "    %-10s %-6s %-12s %s\n", usr, typ, itm, val
        }
      }' "$file" 2>/dev/null || true)"
    
    if [[ -n "${matches:-}" ]]; then
      echo "  $file:"
      printf "%s\n" "$matches"
      found_limits=1
    fi
  done
  
  if ((found_limits == 0)); then
    warn "No persistent limits for ${SERVICE_USER} found in /etc/security/limits.*"
    info "  Recommendation: Create /etc/security/limits.d/99-kafka.conf with:"
    info "    $SERVICE_USER soft nofile $MIN_NOFILE"
    info "    $SERVICE_USER hard nofile $MIN_NOFILE"
    info "    $SERVICE_USER soft nproc $MIN_NPROC"
    info "    $SERVICE_USER hard nproc $MIN_NPROC"
    info "    $SERVICE_USER soft memlock unlimited"
    info "    $SERVICE_USER hard memlock unlimited"
  else
    pass "Persistent limits configured for ${SERVICE_USER}"
  fi
fi

# =============================================================================
# KAFKA INSTALLATION TYPE
# =============================================================================
section "KAFKA INSTALLATION TYPE"

# Detect Confluent Server vs Apache Kafka
kafka_package_found=0
if command -v rpm &>/dev/null; then
  if rpm -q confluent-server &>/dev/null 2>&1; then
    pass "Confluent Server detected (commercial features available)"
    confluent_version=$(rpm -q confluent-server --queryformat '%{VERSION}' 2>/dev/null || echo "unknown")
    printf "  Version: %s\n" "$confluent_version"
    kafka_package_found=1
  elif rpm -q confluent-kafka &>/dev/null 2>&1; then
    info "Apache Kafka (Confluent distribution) detected"
    kafka_version=$(rpm -q confluent-kafka --queryformat '%{VERSION}' 2>/dev/null || echo "unknown")
    printf "  Version: %s\n" "$kafka_version"
    kafka_package_found=1
  elif rpm -q confluent-community-kafka &>/dev/null 2>&1; then
    info "Confluent Community Kafka detected"
    kafka_version=$(rpm -q confluent-community-kafka --queryformat '%{VERSION}' 2>/dev/null || echo "unknown")
    printf "  Version: %s\n" "$kafka_version"
    kafka_package_found=1
  fi
elif command -v dpkg &>/dev/null; then
  if dpkg -l 2>/dev/null | grep -q confluent-server; then
    pass "Confluent Server detected (commercial features available)"
    kafka_package_found=1
  elif dpkg -l 2>/dev/null | grep -q confluent-kafka; then
    info "Apache Kafka (Confluent distribution) detected"
    kafka_package_found=1
  fi
fi

if ((kafka_package_found == 0)); then
  # Check for archive installation
  if [[ -d "$CONFLUENT_HOME" ]]; then
    info "Archive installation detected at $CONFLUENT_HOME"
  else
    warn "Kafka installation not detected (check CONFLUENT_HOME path)"
  fi
fi

# =============================================================================
# KAFKA SERVICE USER VALIDATION
# =============================================================================
section "KAFKA SERVICE USER"

echo "Detected Kafka service user: $SERVICE_USER"

# Validate service account
if id "$SERVICE_USER" &>/dev/null 2>&1; then
  pass "Service user '$SERVICE_USER' exists"
  
  # Check if service account has a login shell (security best practice)
  user_shell=$(getent passwd "$SERVICE_USER" 2>/dev/null | cut -d: -f7)
  if [[ -n "$user_shell" ]]; then
    if [[ "$user_shell" == "/bin/false" ]] || [[ "$user_shell" == "/sbin/nologin" ]] || [[ "$user_shell" == "/usr/sbin/nologin" ]]; then
      pass "Service account properly secured (no login shell: $user_shell)"
    else
      warn "Service account has login shell: $user_shell (security concern)"
      info "  Recommendation: For security, set shell to /bin/false or /sbin/nologin"
      info "    Command: sudo usermod -s /bin/false $SERVICE_USER"
    fi
  fi
  
  # Check home directory
  user_home=$(getent passwd "$SERVICE_USER" 2>/dev/null | cut -d: -f6)
  if [[ -n "$user_home" ]]; then
    printf "  Home directory: %s\n" "$user_home"
    if [[ ! -d "$user_home" ]]; then
      warn "  Home directory does not exist"
    fi
  fi
  
  # Check user's primary group
  user_group=$(id -gn "$SERVICE_USER" 2>/dev/null)
  if [[ -n "$user_group" ]]; then
    printf "  Primary group: %s\n" "$user_group"
  fi
else
  fail "Service user '$SERVICE_USER' does not exist"
  info "  Recommendation: Create dedicated service user for Kafka"
  info "    Example (cp-ansible default): cp-kafka"
fi

# =============================================================================
# JAVA & CONFLUENT VERSIONS
# =============================================================================
section "JAVA & CONFLUENT VERSIONS"

if has_cmd java; then
  java_version=$(java -version 2>&1 | head -n1)
  info "Java: $java_version"
  
  # Check Java version (Kafka 3.x+ requires Java 11+)
  if [[ "$java_version" =~ version[[:space:]]\"([0-9]+) ]]; then
    java_major="${BASH_REMATCH[1]}"
    if ((java_major >= 17)); then
      pass "Java version $java_major (17+ recommended for Kafka 3.x+)"
    elif ((java_major >= 11)); then
      pass "Java version $java_major (meets minimum requirement)"
      info "  Recommendation: Upgrade to Java 17 or later for best performance"
    else
      warn "Java version $java_major is below minimum requirement (11+)"
      info "  Recommendation: yum install java-17-openjdk-devel"
    fi
  fi
else
  warn "Java not found in PATH"
  info "  Recommendation: yum install java-17-openjdk-devel"
fi

# Check Confluent installation
if [[ -d "$CONFLUENT_HOME" ]]; then
  info "Confluent home: $CONFLUENT_HOME"
  kafka_topics="$CONFLUENT_HOME/latest/bin/kafka-topics"
  
  if [[ -x "$kafka_topics" ]]; then
    verout=$("$kafka_topics" --version 2>&1 | head -n1 || echo "")
    if [[ -n "$verout" ]]; then
      pass "Confluent Platform detected: $verout"
    else
      warn "Could not determine Confluent version from kafka-topics --version"
    fi
  else
    warn "$kafka_topics not executable or not found"
  fi
  
  # Check for additional Confluent components
  for component in kafka-server-start kafka-console-producer kafka-console-consumer; do
    comp_path="$CONFLUENT_HOME/latest/bin/$component"
    if [[ -x "$comp_path" ]]; then
      info "  $component: found"
    else
      warn "  $component: not found"
    fi
  done
else
  warn "$CONFLUENT_HOME not found"
  info "  If Confluent is installed elsewhere, set CONFLUENT_HOME environment variable"
fi

# =============================================================================
# KAFKA server.properties CHECKS + RECOMMENDATIONS
# =============================================================================
section "KAFKA server.properties CONFIGURATION"

# Auto-detect server.properties if not set
if [[ -z "$SERVER_PROPS_PATH" ]]; then
  for candidate in \
    "$CONFLUENT_HOME/latest/etc/kafka/server.properties" \
    "$CONFLUENT_HOME/etc/kafka/server.properties" \
    "/etc/kafka/server.properties" \
    "/opt/kafka/config/server.properties"; do
    if [[ -r "$candidate" ]]; then
      SERVER_PROPS_PATH="$candidate"
      break
    fi
  done
fi

if [[ ! -r "$SERVER_PROPS_PATH" ]]; then
  warn "Could not locate server.properties"
  info "  Set SERVER_PROPS_PATH environment variable or check $CONFLUENT_HOME/etc/kafka/"
else
  pass "Using server.properties: $SERVER_PROPS_PATH"
  
  # Function to extract property value
  get_prop() {
    awk -v k="$2" -F= '
      BEGIN{IGNORECASE=1}
      $0 !~ /^[[:space:]]*#/ && $0 ~ ("^"k"[[:space:]]*=") {
        v=$2
        for(i=3;i<=NF;i++) v=v"="$i  # Handle values with = in them
        gsub(/^[[:space:]]+|[[:space:]]+$/,"",v)
        print v
      }' "$1" | tail -n1
  }
  
  # Get vCPU count
  VCPU=$(nproc 2>/dev/null || echo 1)
  
  # Get data directories
  logdirs="$(get_prop "$SERVER_PROPS_PATH" "log.dirs")"
  DATA_DIRS=1
  if [[ -n "${logdirs:-}" ]]; then
    IFS=',' read -r -a LDIRS <<<"$logdirs"
    DATA_DIRS="${#LDIRS[@]}"
    
    # Check if log.dirs is in /tmp (not recommended for production)
    if [[ "$logdirs" == *"/tmp"* ]]; then
      warn "log.dirs contains /tmp path (NOT recommended for production)"
      info "  Recommendation: Use dedicated disks (e.g., /data/kafka, /mnt/kafka)"
    fi
  else
    warn "log.dirs not set (will use default: /tmp/kafka-logs)"
    info "  Recommendation: Set log.dirs to dedicated disk path"
  fi
  
  printf "\nConfiguration Summary:\n"
  printf "  log.dirs  : %s (%d data director%s)\n" "${logdirs:-[NOT SET]}" "$DATA_DIRS" "$( ((DATA_DIRS > 1)) && echo "ies" || echo "y")"
  printf "  vCPUs     : %d\n" "$VCPU"
  
  # Calculate recommendations based on vCPUs and data dirs
  REC_NET=$(clamp $(((VCPU + 3) / 4)) 3 32)
  REC_IO=$((VCPU >= 8 ? VCPU : 8))
  REC_REPF=$((VCPU >= 16 ? 3 : 2))
  REC_RECOVERY=$((DATA_DIRS >= 1 ? DATA_DIRS : 1))
  
  printf "\nRecommended thread values (based on %d vCPUs, %d data dir%s):\n" "$VCPU" "$DATA_DIRS" "$( ((DATA_DIRS > 1)) && echo "s" || echo "")"
  printf "  num.network.threads               : %d\n" "$REC_NET"
  printf "  num.io.threads                    : %d\n" "$REC_IO"
  printf "  num.replica.fetchers              : %d\n" "$REC_REPF"
  printf "  num.recovery.threads.per.data.dir : %d\n" "$REC_RECOVERY"
  echo ""
  
  # Check actual values against recommendations
  check_property() {
    local key=$1 want=$2 val
    
    val="$(get_prop "$SERVER_PROPS_PATH" "$key" 2>/dev/null || true)"
    
    if [[ -z "${val:-}" ]]; then
      warn "$key not set in server.properties"
      info "  Recommendation: Add '$key=$want' to server.properties"
      return
    fi
    
    # Extract and validate numeric value
    if [[ "$val" =~ ^[0-9]+$ ]]; then
      if ((val < want)); then
        warn "$key=$val (below recommended $want)"
        info "  Recommendation: Update to '$key=$want'"
      elif ((val > want * 2)); then
        warn "$key=$val (significantly above recommended $want)"
        info "  Review if this high value is intentional"
      else
        pass "$key=$val (>= recommended $want)"
      fi
    else
      warn "$key has invalid value '$val' (expected numeric)"
      info "  Recommendation: Set to '$key=$want'"
    fi
  }
  
  check_property "num.network.threads" "$REC_NET"
  check_property "num.io.threads" "$REC_IO"
  check_property "num.replica.fetchers" "$REC_REPF"
  check_property "num.recovery.threads.per.data.dir" "$REC_RECOVERY"
  
  # Additional important settings to verify
  echo ""
  info "Critical production settings (Confluent recommendations):"
  
  # Check replication settings
  repl_factor="$(get_prop "$SERVER_PROPS_PATH" "default.replication.factor")"
  if [[ -n "$repl_factor" && "$repl_factor" =~ ^[0-9]+$ ]]; then
    if ((repl_factor >= 3)); then
      pass "default.replication.factor=$repl_factor (>= 3 for production)"
    elif ((repl_factor >= 2)); then
      warn "default.replication.factor=$repl_factor (minimum acceptable, 3 recommended)"
    else
      fail "default.replication.factor=$repl_factor (< 2, critical for production)"
      info "  Recommendation: Set default.replication.factor=3"
    fi
  else
    warn "default.replication.factor not set (defaults to 1 - too low!)"
    info "  Recommendation: Set default.replication.factor=3"
  fi
  
  min_isr="$(get_prop "$SERVER_PROPS_PATH" "min.insync.replicas")"
  if [[ -n "$min_isr" && "$min_isr" =~ ^[0-9]+$ ]]; then
    if ((min_isr >= 2)); then
      pass "min.insync.replicas=$min_isr (>= 2 for production)"
    else
      warn "min.insync.replicas=$min_isr (< 2, should be at least 2)"
      info "  Recommendation: Set min.insync.replicas=2"
    fi
  else
    warn "min.insync.replicas not set (defaults to 1 - too low!)"
    info "  Recommendation: Set min.insync.replicas=2"
  fi
  
  unclean_leader="$(get_prop "$SERVER_PROPS_PATH" "unclean.leader.election.enable")"
  if [[ -n "$unclean_leader" ]]; then
    if [[ "$unclean_leader" == "false" ]]; then
      pass "unclean.leader.election.enable=false (prevents data loss)"
    else
      warn "unclean.leader.election.enable=$unclean_leader (should be false)"
      info "  Recommendation: Set unclean.leader.election.enable=false"
    fi
  else
    pass "unclean.leader.election.enable not set (defaults to false)"
  fi
  
  echo ""
  info "Other configuration parameters:"
  
  critical_props=(
    "broker.id"
    "listeners"
    "advertised.listeners"
    "log.retention.hours"
    "log.segment.bytes"
    "num.partitions"
    "default.replication.factor"
    "min.insync.replicas"
    "zookeeper.connect"
  )
  
  for prop in "${critical_props[@]}"; do
    val="$(get_prop "$SERVER_PROPS_PATH" "$prop" 2>/dev/null || true)"
    if [[ -n "$val" ]]; then
      printf "  %-30s : %s\n" "$prop" "$val"
    else
      printf "  %-30s : %s\n" "$prop" "[NOT SET]"
    fi
  done
  
  # Specific recommendations for common misconfigurations
  echo ""
  repl_factor="$(get_prop "$SERVER_PROPS_PATH" "default.replication.factor")"
  if [[ "$repl_factor" =~ ^[0-9]+$ ]] && ((repl_factor < 3)); then
    warn "default.replication.factor=$repl_factor (should be >= 3 for production)"
    info "  Recommendation: Set default.replication.factor=3"
  fi
  
  min_isr="$(get_prop "$SERVER_PROPS_PATH" "min.insync.replicas")"
  if [[ "$min_isr" =~ ^[0-9]+$ ]] && ((min_isr < 2)); then
    warn "min.insync.replicas=$min_isr (should be >= 2 for production)"
    info "  Recommendation: Set min.insync.replicas=2"
  fi
fi

# =============================================================================
# KAFKA BROKER PROCESS
# =============================================================================
section "KAFKA BROKER PROCESS"

if pgrep -f "kafka\.Kafka" >/dev/null 2>&1; then
  pass "Kafka broker process is running"
  
  # Get process info
  kafka_pid=$(pgrep -f "kafka\.Kafka" | head -n1)
  if [[ -n "$kafka_pid" ]]; then
    printf "  PID: %s\n" "$kafka_pid"
    
    # Get JVM heap size if possible
    if [[ -r "/proc/$kafka_pid/cmdline" ]]; then
      heap_xmx=$(tr '\0' '\n' < "/proc/$kafka_pid/cmdline" | grep -oP '^-Xmx\K.*' | head -n1)
      heap_xms=$(tr '\0' '\n' < "/proc/$kafka_pid/cmdline" | grep -oP '^-Xms\K.*' | head -n1)
      
      if [[ -n "$heap_xmx" ]]; then
        printf "  Heap Max (Xmx): %s\n" "$heap_xmx"
        
        # Convert heap to MB for comparison (Confluent recommends 6GB = 6144MB)
        heap_mb=0
        if [[ "$heap_xmx" =~ ^([0-9]+)([gGmMkK])?$ ]]; then
          num="${BASH_REMATCH[1]}"
          unit="${BASH_REMATCH[2]}"
          case "${unit,,}" in
            g) heap_mb=$((num * 1024)) ;;
            m) heap_mb=$num ;;
            k) heap_mb=$((num / 1024)) ;;
            *) heap_mb=$((num / 1024 / 1024)) ;;
          esac
        fi
        
        if ((heap_mb >= 6144)); then
          pass "  Heap size adequate (>= 6GB Confluent recommendation)"
        elif ((heap_mb >= 4096)); then
          warn "  Heap size $heap_xmx (Confluent recommends 6GB for production)"
          info "  Recommendation: Set -Xmx6g -Xms6g"
        elif ((heap_mb > 0)); then
          warn "  Heap size $heap_xmx is below 4GB (too low for production)"
          info "  Recommendation: Set -Xmx6g -Xms6g"
        fi
      fi
      
      if [[ -n "$heap_xms" ]]; then
        printf "  Heap Min (Xms): %s\n" "$heap_xms"
      fi
      
      # Check if Xmx and Xms match (recommended)
      if [[ -n "$heap_xmx" && -n "$heap_xms" ]]; then
        if [[ "$heap_xmx" == "$heap_xms" ]]; then
          pass "  Heap min and max are equal (prevents heap resizing)"
        else
          warn "  Heap min ($heap_xms) != max ($heap_xmx)"
          info "  Recommendation: Set Xms and Xmx to same value (e.g., -Xms6g -Xmx6g)"
        fi
      fi
      
      # Get GC settings
      gc_type=$(tr '\0' '\n' < "/proc/$kafka_pid/cmdline" | grep -oP '^-XX:\+Use\K.*GC' | head -n1)
      if [[ -n "$gc_type" ]]; then
        printf "  GC Type: %s\n" "$gc_type"
        if [[ "$gc_type" == "G1GC" ]]; then
          pass "  Using G1GC (recommended for Kafka)"
        fi
      fi
    fi
    
    # Check process open files
    if [[ -r "/proc/$kafka_pid/limits" ]]; then
      open_files=$(awk '/open files/ {print $4}' "/proc/$kafka_pid/limits")
      if [[ -n "$open_files" ]]; then
        printf "  Open files limit: %s\n" "$open_files"
        if [[ "$open_files" == "unlimited" ]] || [[ "$open_files" =~ ^[0-9]+$ && "$open_files" -ge "$MIN_NOFILE" ]]; then
          pass "  Process open files limit is adequate"
        else
          warn "  Process open files limit may be too low"
        fi
      fi
    fi
  fi
else
  fail "Kafka broker process is NOT running"
  info "  Recommendation: Start Kafka broker service"
  info "    systemctl start confluent-kafka (or appropriate service name)"
fi

# =============================================================================
# DISK CONFIGURATION
# =============================================================================
section "DISK CONFIGURATION"

if [[ -n "${logdirs:-}" ]]; then
  IFS=',' read -r -a log_dirs <<<"$logdirs"
  
  echo "Kafka data directories:"
  for dir in "${log_dirs[@]}"; do
    dir=$(echo "$dir" | tr -d ' ')  # Remove any whitespace
    
    if [[ -d "$dir" ]]; then
      pass "  $dir: exists"
      
      # Get filesystem type and mount options
      mount_info=$(df -PT "$dir" 2>/dev/null | tail -n1)
      fs_type=$(awk '{print $2}' <<<"$mount_info")
      mount_point=$(awk '{print $7}' <<<"$mount_info")
      
      printf "    Filesystem: %s\n" "$fs_type"
      printf "    Mount point: %s\n" "$mount_point"
      
      # Get mount options
      mount_opts=$(mount | grep " $mount_point " | sed 's/.*(\(.*\))/\1/')
      printf "    Mount options: %s\n" "$mount_opts"
      
      # Check for recommended mount options
      if [[ "$mount_opts" == *"noatime"* ]]; then
        pass "    'noatime' option set (reduces disk I/O)"
      else
        warn "    'noatime' option not set"
        info "    Recommendation: Remount with 'noatime' to reduce disk I/O"
      fi
      
      # Check disk space
      disk_info=$(df -h "$dir" 2>/dev/null | tail -n1)
      used_pct=$(awk '{print $5}' <<<"$disk_info" | tr -d '%')
      avail=$(awk '{print $4}' <<<"$disk_info")
      
      printf "    Available: %s (Used: %s%%)\n" "$avail" "$used_pct"
      
      if ((used_pct >= 90)); then
        fail "    Disk usage at ${used_pct}% (critical)"
        info "    Recommendation: Free up disk space or increase retention policy"
      elif ((used_pct >= 80)); then
        warn "    Disk usage at ${used_pct}% (high)"
      else
        pass "    Disk usage at ${used_pct}% (acceptable)"
      fi
      
      # Check for XFS filesystem (recommended for Kafka)
      if [[ "$fs_type" == "xfs" ]]; then
        pass "    XFS filesystem (recommended for Kafka)"
      elif [[ "$fs_type" == "ext4" ]]; then
        info "    EXT4 filesystem (acceptable, XFS recommended)"
      else
        warn "    $fs_type filesystem (XFS recommended for Kafka)"
      fi
      
    else
      warn "  $dir: does not exist"
      info "    Recommendation: Create directory and set proper ownership"
    fi
  done
else
  info "No log.dirs configured in server.properties"
fi

# =============================================================================
# SUMMARY
# =============================================================================
section "AUDIT SUMMARY"

printf "\n"
printf "%s\n" "${BOLD}Results Summary:${RESET}"
printf "  %b%-10s%b : %3d checks\n" "${GREEN}" "PASS" "${NC}" "$PASS_COUNT"
printf "  %b%-10s%b : %3d checks\n" "${YELLOW}" "WARN" "${NC}" "$WARN_COUNT"
printf "  %b%-10s%b : %3d checks\n" "${RED}" "FAIL" "${NC}" "$FAIL_COUNT"
printf "  %b%-10s%b : %3d informational\n" "${BLUE}" "INFO" "${NC}" "$INFO_COUNT"

echo ""
if ((FAIL_COUNT > 0)); then
  fail "System has $FAIL_COUNT critical failures that must be addressed"
elif ((WARN_COUNT > 0)); then
  warn "System has $WARN_COUNT warnings - review recommendations above"
else
  pass "System passes all checks!"
fi

echo ""
info "This is a read-only audit script - no system changes were made"
info "Review the recommendations above and apply changes as appropriate"

echo ""
info "For more Kafka tuning guidance, visit:"
info "  https://docs.confluent.io/platform/current/installation/system-requirements.html"
info "  https://kafka.apache.org/documentation/#os"
info "  https://docs.confluent.io/ansible/current/overview.html"

hr

# Generate simple configuration recommendation summary
if ((WARN_COUNT > 0 || FAIL_COUNT > 0)); then
  echo ""
  section "QUICK FIX SUMMARY"
  
  echo "Consider creating these configuration files based on findings above:"
  echo ""
  echo "1. /etc/sysctl.d/99-kafka.conf - Kernel parameters"
  echo "2. /etc/security/limits.d/99-kafka.conf - User limits"  
  echo "3. Update $SERVER_PROPS_PATH - Kafka broker settings"
  echo "4. Configure tuned profile for your environment"
  echo "5. Review disk mount options for Kafka data directories"
  echo ""
  info "Re-run this audit after applying changes to verify improvements"
fi
