#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Kafka Node Audit for RHEL8 + Confluent 7.6.2 +
# Checks OS/network/kafka settings, compares sysctl to cp-ansible standards.
#
# Usage:
#   ./kafka-node-audit.sh [OPTIONS]
#
# Options:
#   -apply              Apply recommended kernel sysctl values at runtime (requires confirmation)
#   --dry-run           Show what would be changed without applying
#   --output=FILE       Write output to specified file (in addition to stdout)
#   --skip-sudo-check   Skip checks that require sudo access
#   --help              Show this help message
#
# Requirements:
#   - Passwordless sudo for ulimit checks (optional, will warn and skip if unavailable)
#   - Root/sudo access for -apply flag
#
# Environment Variables:
#   SERVICE_USER        Kafka service user (default: cp-kafka)
#   MIN_NOFILE          Minimum open files limit (default: 100000)
#   MIN_NPROC           Minimum process limit (default: 4096)
#   CONFLUENT_HOME      Confluent installation path (default: /opt/confluent)
#   SERVER_PROPS_PATH   Path to server.properties (auto-detected if not set)
# ---------------------------------------------------------------------------

set -euo pipefail

# Configuration with defaults
: "${SERVICE_USER:=stekafka}"
: "${MIN_NOFILE:=100000}"
: "${MIN_NPROC:=4096}"
: "${CONFLUENT_HOME:=/opt/confluent}"
: "${SERVER_PROPS_PATH:=}"

# Counters for summary
PASS_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0

# Portable color using tput with fallback to ANSI
if command -v tput >/dev/null 2>&1 && [[ -t 1 ]]; then
  BOLD=$(tput bold 2>/dev/null || echo "\033[1m")
  RESET=$(tput sgr0 2>/dev/null || echo "\033[0m")
  GREEN=$(tput setaf 2 2>/dev/null || echo "\033[32m")
  YELLOW=$(tput setaf 3 2>/dev/null || echo "\033[33m")
  RED=$(tput setaf 1 2>/dev/null || echo "\033[31m")
  NC="$RESET"
else
  BOLD="\033[1m"
  RESET="\033[0m"
  GREEN="\033[32m"
  YELLOW="\033[33m"
  RED="\033[31m"
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
  printf "%b[INFO]%b %s\n" "" "${NC}" "$1"
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
APPLY=0
DRY_RUN=0
OUTPUT_FILE=""
SKIP_SUDO_CHECK=0

for arg in "$@"; do
  case "$arg" in
    -apply)
      APPLY=1
      ;;
    --dry-run)
      DRY_RUN=1
      ;;
    --output=*)
      OUTPUT_FILE="${arg#--output=}"
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

# Validate -apply requirements
if ((APPLY == 1)); then
  if [[ $EUID -ne 0 ]]; then
    fail "-apply flag requires root/sudo privileges"
    exit 1
  fi
  
  echo ""
  warn "WARNING: About to modify kernel parameters at runtime!"
  warn "These changes will NOT persist across reboots unless added to /etc/sysctl.conf"
  echo ""
  read -p "Do you want to continue? (yes/no): " -r confirm
  echo ""
  
  if [[ "$confirm" != "yes" ]]; then
    info "Operation cancelled by user."
    exit 0
  fi
fi

if ((DRY_RUN == 1)); then
  info "DRY-RUN MODE: No changes will be applied"
  echo ""
fi

# =============================================================================
# SYSTEM & TUNED PROFILE
# =============================================================================
section "SYSTEM & TUNED PROFILE"

printf "Hostname: %s\n" "$(hostname)"
printf "Kernel  : %s\n" "$(uname -r)"
printf "Date    : %s\n" "$(date '+%Y-%m-%d %H:%M:%S %Z')"

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
    warn "tuned service is NOT active. Start with: systemctl start tuned"
  else
    pass "tuned service is active"
  fi
  
  # Profile recommendations based on VM vs physical
  if [[ "$virt_kind" == "vm" ]]; then
    if [[ "$active_profile" == "virtual-guest" ]]; then
      pass "VM → tuned profile 'virtual-guest' is recommended and active"
    else
      warn "VM → consider setting tuned profile to 'virtual-guest' (current: ${active_profile:-none})"
      info "  Run: tuned-adm profile virtual-guest"
    fi
  else
    case "${active_profile:-}" in
      throughput-performance|latency-performance|network-throughput|cpu-performance|high-performance)
        pass "Physical → high-performance tuned profile (${active_profile}) is active"
        ;;
      *)
        warn "Physical → consider a high-performance tuned profile (current: ${active_profile:-none})"
        info "  Suggested profiles: throughput-performance, network-throughput"
        info "  Run: tuned-adm profile throughput-performance"
        ;;
    esac
  fi
else
  warn "tuned-adm not installed; consider installing: yum install tuned"
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
      warn "THP $f not set to '[never]'; recommended to disable for Kafka"
      info "  To disable temporarily: echo never > $thp_path"
      info "  To persist: Add 'transparent_hugepage=never' to kernel boot params"
    fi
  else
    warn "Cannot read $thp_path"
  fi
done

# =============================================================================
# SYSCTL CHECKS (cp-ansible Kafka recommendations)
# =============================================================================
section "SYSCTL CHECKS (Kafka/cp-ansible recommendations)"

# Combined recommendations (cp-ansible + Kafka best practices)
declare -A SYSCTL_RECS=(
  [vm.swappiness]=1
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
      
      if ((APPLY == 1)) && ((DRY_RUN == 0)); then
        info "Applying: sysctl -w $k=\"$want\""
        sysctl -w "$k=$want" >/dev/null 2>&1 || warn "Failed to apply $k"
      elif ((DRY_RUN == 1)); then
        info "[DRY-RUN] Would apply: sysctl -w $k=\"$want\""
      fi
    fi
  else
    # Numeric comparison
    if ((cur >= want)); then
      pass "$k=$cur (>= recommended $want)"
    else
      warn "$k=$cur (< recommended $want)"
      
      if ((APPLY == 1)) && ((DRY_RUN == 0)); then
        info "Applying: sysctl -w $k=$want"
        sysctl -w "$k=$want" >/dev/null 2>&1 || warn "Failed to apply $k"
      elif ((DRY_RUN == 1)); then
        info "[DRY-RUN] Would apply: sysctl -w $k=$want"
      fi
    fi
  fi
done

if ((APPLY == 1)) && ((DRY_RUN == 0)); then
  echo ""
  info "Runtime sysctl changes applied successfully."
  warn "To persist across reboots, add these settings to /etc/sysctl.conf or /etc/sysctl.d/99-kafka.conf"
  info "Example: echo 'vm.swappiness=1' >> /etc/sysctl.d/99-kafka.conf"
fi

# =============================================================================
# NETWORK INTERFACES & RING BUFFERS
# =============================================================================
section "NETWORK INTERFACES & RING BUFFERS"

if ! has_cmd ethtool; then
  warn "ethtool not found; install for NIC details: yum install ethtool"
fi

# Filter out loopback and common virtual interfaces
mapfile -t ifaces < <(
  ls -1 /sys/class/net 2>/dev/null | grep -vE '^(lo|docker|virbr|veth|br-)' || true
)

if ((${#ifaces[@]} == 0)); then
  warn "No non-loopback network interfaces detected"
else
  printf "Detected interfaces: %s\n" "${ifaces[*]}"
fi

has25g=0
declare -A iface_25g

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
  
  # Check for 25Gb NIC
  if [[ "${speed//[[:space:]]/}" =~ ^25000Mb/s|^25Gb ]]; then
    pass "  25Gb capable NIC detected"
    has25g=1
    iface_25g["$i"]=1
  fi
  
  # Check ring buffers
  if vals="$(read_ring_vals "$i")"; then
    read -r cur_rx cur_tx max_rx max_tx <<<"$vals"
    printf "  Rings (cur/max): RX %s/%s, TX %s/%s\n" "$cur_rx" "$max_rx" "$cur_tx" "$max_tx"
    
    if ((max_rx > 0)) && ((cur_rx < max_rx)); then
      warn "  RX ring below max"
      info "  Increase with: ethtool -G $i rx $max_rx"
    elif ((max_rx > 0)); then
      pass "  RX ring at maximum"
    fi
    
    if ((max_tx > 0)) && ((cur_tx < max_tx)); then
      warn "  TX ring below max"
      info "  Increase with: ethtool -G $i tx $max_tx"
    elif ((max_tx > 0)); then
      pass "  TX ring at maximum"
    fi
  else
    info "  Ring buffer info not available"
  fi
done

if ((has25g == 1)); then
  pass "At least one 25Gb NIC detected"
else
  warn "No 25Gb NIC detected (10Gb or 1Gb may limit performance)"
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
  printf "Default Gateway: %s\n" "${default_gw:-none}"
  
  # Get static routes (excluding default, linkdown, unreachable)
  mapfile -t static_routes < <(
    ip route 2>/dev/null | grep -v '^default' | grep -v '^linkdown' | grep -v '^unreachable' || true
  )
  
  if ((${#static_routes[@]} > 0)); then
    info "Static routes found:"
    for route in "${static_routes[@]}"; do
      printf "  %s\n" "$route"
    done
    
    # Check if any static routes use 25Gb interfaces
    found_static_25g=0
    for route in "${static_routes[@]}"; do
      iface=$(awk '{for (i=1;i<=NF;i++) {if ($i=="dev") print $(i+1)}}' <<<"$route")
      if [[ -n "${iface_25g[$iface]:-}" ]]; then
        pass "Static route uses 25Gb interface: $route"
        found_static_25g=1
      fi
    done
    
    if ((found_static_25g == 0)) && ((has25g == 1)); then
      warn "No static routes configured via 25Gb network interface(s)"
      info "Consider routing Kafka traffic through high-speed interfaces"
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
    
    echo "Bond: $bname"
    mode=$(awk -F': ' '/Bonding Mode/{print $2}' "$bf")
    echo "  Mode: $mode"
    
    # Show slave interfaces and their status
    awk '/Slave Interface/ {iface=$3} 
         /MII Status/ && iface {printf "  Slave %-10s link %s\n", iface, $3; iface=""}' "$bf"
    
    pass "Bond $bname configured ($mode)"
    
    # Recommendation for Kafka
    if [[ "$mode" =~ 802\.3ad|LACP|mode[[:space:]]4 ]]; then
      pass "  LACP/802.3ad mode is good for Kafka throughput"
    elif [[ "$mode" =~ balance-rr|mode[[:space:]]0 ]]; then
      info "  balance-rr mode: ensure switch supports this configuration"
    fi
  done
else
  info "No Linux bonding configured"
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
    warn "Passwordless sudo not available; cannot check runtime ulimits"
    info "Run 'visudo' to grant NOPASSWD for specific commands if needed"
  fi
fi

if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  warn "User ${SERVICE_USER} not found on this system"
  info "Create user with: useradd -r -s /bin/bash $SERVICE_USER"
else
  # Runtime ulimit checks (requires sudo)
  if [[ "$SUDO_OK" == "true" ]]; then
    read_ul() { 
      sudo -n -u "$SERVICE_USER" bash -lc "$1" 2>/dev/null || echo "N/A"
    }
    
    nofile="$(read_ul 'ulimit -n')"
    nproc="$(read_ul 'ulimit -u')"
    memlock="$(read_ul 'ulimit -l')"
    
    printf "Current (runtime) ulimit values:\n"
    printf "  nofile : %s\n" "${nofile:-N/A}"
    printf "  nproc  : %s\n" "${nproc:-N/A}"
    printf "  memlock: %s\n" "${memlock:-N/A}"
    
    # Validate nofile
    if [[ "${nofile:-}" =~ ^[0-9]+$ ]]; then
      if ((nofile >= MIN_NOFILE)); then
        pass "nofile >= ${MIN_NOFILE}"
      else
        warn "nofile ($nofile) < ${MIN_NOFILE}"
      fi
    else
      warn "nofile not numeric (got '${nofile:-N/A}')"
    fi
    
    # Validate nproc
    if [[ "${nproc:-}" =~ ^[0-9]+$ ]]; then
      if ((nproc >= MIN_NPROC)); then
        pass "nproc >= ${MIN_NPROC}"
      else
        warn "nproc ($nproc) < ${MIN_NPROC}"
      fi
    else
      warn "nproc not numeric (got '${nproc:-N/A}')"
    fi
    
    # Validate memlock
    if [[ "${memlock:-}" == "unlimited" ]]; then
      pass "memlock unlimited (recommended)"
    elif [[ "${memlock:-}" == "N/A" ]]; then
      warn "memlock value unavailable"
    elif [[ "${memlock:-}" =~ ^[0-9]+$ ]]; then
      warn "memlock is finite (${memlock}); consider setting to unlimited"
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
    info "Add to /etc/security/limits.d/99-kafka.conf:"
    info "  $SERVICE_USER soft nofile $MIN_NOFILE"
    info "  $SERVICE_USER hard nofile $MIN_NOFILE"
    info "  $SERVICE_USER soft nproc $MIN_NPROC"
    info "  $SERVICE_USER hard nproc $MIN_NPROC"
    info "  $SERVICE_USER soft memlock unlimited"
    info "  $SERVICE_USER hard memlock unlimited"
  else
    pass "Persistent limits configured for ${SERVICE_USER}"
  fi
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
    if ((java_major >= 11)); then
      pass "Java version $java_major is supported (11+ required)"
    else
      warn "Java version $java_major may be too old (11+ recommended)"
    fi
  fi
else
  warn "Java not found in PATH"
  info "Install Java: yum install java-11-openjdk-devel"
fi

# Check Confluent installation
if [[ -d "$CONFLUENT_HOME" ]]; then
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
else
  warn "$CONFLUENT_HOME not found"
  info "If Confluent is installed elsewhere, set CONFLUENT_HOME environment variable"
fi

# =============================================================================
# KAFKA server.properties CHECKS + RECOMMENDATIONS
# =============================================================================
section "KAFKA server.properties CHECKS"

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
  info "Set SERVER_PROPS_PATH environment variable or check $CONFLUENT_HOME/etc/kafka/"
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
  fi
  
  printf "\nConfiguration:\n"
  printf "  log.dirs  : %s (%d data dir%s)\n" "${logdirs:-default}" "$DATA_DIRS" "$( ((DATA_DIRS > 1)) && echo "s" || echo "")"
  printf "  vCPUs     : %d\n" "$VCPU"
  
  # Calculate recommendations based on vCPUs and data dirs
  REC_NET=$(clamp $(((VCPU + 3) / 4)) 3 32)
  REC_IO=$((VCPU >= 8 ? VCPU : 8))
  REC_REPF=$((VCPU >= 16 ? 3 : 2))
  REC_RECOVERY=$((DATA_DIRS >= 1 ? DATA_DIRS : 1))
  
  printf "\nRecommended thread values:\n"
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
      warn "$key not set in server.properties (recommended: $want)"
      info "  Add: $key=$want"
      return
    fi
    
    # Extract numeric value
    if [[ "$val" =~ ^[0-9]+$ ]]; then
      if ((val < want)); then
        warn "$key=$val (below recommended $want)"
        info "  Update to: $key=$want"
      else
        pass "$key=$val (>= recommended $want)"
      fi
    else
      warn "$key has invalid value '$val' (expected numeric, recommended: $want)"
    fi
  }
  
  check_property "num.network.threads" "$REC_NET"
  check_property "num.io.threads" "$REC_IO"
  check_property "num.replica.fetchers" "$REC_REPF"
  check_property "num.recovery.threads.per.data.dir" "$REC_RECOVERY"
  
  # Additional important settings
  echo ""
  info "Additional important settings to verify:"
  
  for prop in "broker.id" "listeners" "advertised.listeners" "log.retention.hours" "log.segment.bytes"; do
    val="$(get_prop "$SERVER_PROPS_PATH" "$prop" 2>/dev/null || true)"
    if [[ -n "$val" ]]; then
      printf "  %-25s : %s\n" "$prop" "$val"
    else
      printf "  %-25s : %s\n" "$prop" "[NOT SET]"
    fi
  done
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
      [[ -n "$heap_xmx" ]] && printf "  Heap Max: %s\n" "$heap_xmx"
      [[ -n "$heap_xms" ]] && printf "  Heap Min: %s\n" "$heap_xms"
    fi
  fi
else
  warn "Kafka broker process is NOT running"
  info "Start with: systemctl start confluent-kafka (or appropriate service name)"
fi

# =============================================================================
# SUMMARY
# =============================================================================
section "AUDIT SUMMARY"

printf "Total Checks:\n"
printf "  %b%-10s%b : %d\n" "${GREEN}" "PASS" "${NC}" "$PASS_COUNT"
printf "  %b%-10s%b : %d\n" "${YELLOW}" "WARN" "${NC}" "$WARN_COUNT"
printf "  %b%-10s%b : %d\n" "${RED}" "FAIL" "${NC}" "$FAIL_COUNT"

echo ""
if ((WARN_COUNT > 0 || FAIL_COUNT > 0)); then
  warn "System has $WARN_COUNT warnings and $FAIL_COUNT failures"
  info "Review warnings and failures above for recommended actions"
else
  pass "System passes all checks!"
fi

echo ""
info "For more Kafka tuning guidance, visit:"
info "  https://docs.confluent.io/platform/current/installation/system-requirements.html"
info "  https://kafka.apache.org/documentation/#os"

hr
