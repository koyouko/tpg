#!/usr/bin/env bash
# ==============================================================================
# Script Name: check_host_keys_sudo.sh
# Description: Reads a server list, connects via passwordless SSH key, elevates
#              to root via sudo (with fallback), checks RHEL SSH host key
#              rotation (private keys + journalctl), prints a formatted table,
#              and exports to a CSV file.
# Usage:       ./check_host_keys_sudo.sh [server_list.txt] [ssh_user] [ssh_key]
# ==============================================================================

set -u

# --- Configuration & Arguments ---
SERVER_LIST="${1:-servers.txt}"
SSH_USER="${2:-$USER}"
SSH_KEY="${3:-$HOME/.ssh/id_rsa}"
OUTPUT_CSV="ssh_host_key_audit_$(date +%Y%m%d_%H%M%S).csv"
SSH_TIMEOUT=5

# --- Pre-flight Checks ---
if [[ ! -f "$SERVER_LIST" ]]; then
    echo "Error: Server list file '$SERVER_LIST' not found." >&2
    echo "Usage: $0 <server_list_file> [ssh_user] [ssh_private_key_path]" >&2
    exit 1
fi

if [[ ! -f "$SSH_KEY" ]]; then
    echo "Error: SSH private key '$SSH_KEY' not found." >&2
    exit 1
fi

# --- Initialize CSV File with Header ---
echo '"Server_Input","Actual_Hostname","Connection_Status","Privilege_Used","Rotation_Status","OS_Install_Date","RSA_Key_Date","RSA_Fingerprint","ECDSA_Key_Date","ECDSA_Fingerprint","ED25519_Key_Date","ED25519_Fingerprint","Days_Diff_From_OS","Last_Keygen_Service_Log"' > "$OUTPUT_CSV"

# --- Print Console Table Header ---
printf "\n%-24s | %-16s | %-6s | %-10s | %-16s | %-16s | %-9s | %s\n" \
    "SERVER" "HOSTNAME" "PRIV" "STATUS" "OS INSTALL DATE" "LATEST KEY DATE" "DAYS DIFF" "ED25519 / PRIMARY FINGERPRINT"
printf '%.0s-' {1..148}
echo ""

# --- Define the Remote Payload ---
REMOTE_SCRIPT='
export LC_ALL=C
ACTUAL_HOST=$(hostname -s 2>/dev/null || hostname)
if [ "$(id -u)" -eq 0 ]; then
    PRIV="root"
else
    PRIV="user"
fi

# 1. Get RHEL OS install timestamp from basesystem RPM (fallback to / filesystem birth/ctime)
OS_TS=$(rpm -q --qf "%{INSTALLTIME}\n" basesystem 2>/dev/null | head -n 1)
if [ -z "$OS_TS" ] || ! [ "$OS_TS" -eq "$OS_TS" ] 2>/dev/null; then
    OS_TS=$(stat -c %W / 2>/dev/null)
    [ -z "$OS_TS" ] || [ "$OS_TS" -le 0 ] && OS_TS=$(stat -c %Z / 2>/dev/null)
fi
OS_DATE=$(date -d @"$OS_TS" "+%Y-%m-%d %H:%M" 2>/dev/null || echo "Unknown")

# 2. Inspect Private Keys (if root) or Public Keys (if non-root fallback)
get_key_meta() {
    local priv_key="/etc/ssh/ssh_host_${1}_key"
    local pub_key="/etc/ssh/ssh_host_${1}_key.pub"
    local target_file=""

    if [ -r "$priv_key" ]; then
        target_file="$priv_key"
    elif [ -r "$pub_key" ]; then
        target_file="$pub_key"
    fi

    if [ -n "$target_file" ]; then
        local kts=$(stat -c %Y "$target_file" 2>/dev/null)
        local kdate=$(date -d @"$kts" "+%Y-%m-%d %H:%M" 2>/dev/null)
        local kfp=$(ssh-keygen -lf "$target_file" 2>/dev/null | awk "{print \$2}")
        echo "${kts}|${kdate}|${kfp}"
    else
        echo "0|N/A|N/A"
    fi
}

IFS="|" read -r RSA_TS RSA_DATE RSA_FP <<< "$(get_key_meta rsa)"
IFS="|" read -r ECDSA_TS ECDSA_DATE ECDSA_FP <<< "$(get_key_meta ecdsa)"
IFS="|" read -r ED_TS ED_DATE ED_FP <<< "$(get_key_meta ed25519)"

# 3. Find the newest host key timestamp across RSA, ECDSA, and ED25519
MAX_KEY_TS=0
LATEST_KEY_DATE="N/A"
for pair in "${RSA_TS}:${RSA_DATE}" "${ECDSA_TS}:${ECDSA_DATE}" "${ED_TS}:${ED_DATE}"; do
    ts="${pair%%:*}"
    dt="${pair#*:}"
    if [ "$ts" -gt "$MAX_KEY_TS" ] 2>/dev/null; then
        MAX_KEY_TS="$ts"
        LATEST_KEY_DATE="$dt"
    fi
done

# 4. Determine Primary Fingerprint (prefer ED25519 -> ECDSA -> RSA)
PRIMARY_FP="$ED_FP"
[ "$PRIMARY_FP" = "N/A" ] && PRIMARY_FP="$ECDSA_FP"
[ "$PRIMARY_FP" = "N/A" ] && PRIMARY_FP="$RSA_FP"

# 5. Check systemd journal for sshd-keygen service activity (requires root)
KEYGEN_LOG="N/A"
if [ "$PRIV" = "root" ] && command -v journalctl >/dev/null 2>&1; then
    LAST_LOG=$(journalctl -u "sshd-keygen*" --no-pager -n 1 -o short-iso 2>/dev/null | tail -n 1 | tr "\"" "'"'"'" | tr "|" " ")
    if [ -n "$LAST_LOG" ] && ! [[ "$LAST_LOG" =~ ^-- ]]; then
        KEYGEN_LOG="$LAST_LOG"
    fi
fi

# 6. Calculate difference in days between OS build and latest host key modification
if [ "$MAX_KEY_TS" -gt 0 ] && [ -n "$OS_TS" ] && [ "$OS_TS" -gt 0 ]; then
    DIFF_SEC=$(( MAX_KEY_TS - OS_TS ))
    DIFF_DAYS=$(( DIFF_SEC / 86400 ))
    if [ "$DIFF_SEC" -gt 86400 ]; then
        ROT_STATUS="ROTATED"
    else
        ROT_STATUS="ORIGINAL"
    fi
else
    DIFF_DAYS="N/A"
    ROT_STATUS="UNKNOWN"
fi

echo "${ACTUAL_HOST}|${PRIV}|${ROT_STATUS}|${OS_DATE}|${LATEST_KEY_DATE}|${RSA_DATE}|${RSA_FP}|${ECDSA_DATE}|${ECDSA_FP}|${ED_DATE}|${ED_FP}|${DIFF_DAYS}|${PRIMARY_FP}|${KEYGEN_LOG}"
'

# --- Read Server List Line by Line ---
while IFS= read -r raw_line <&3 || [[ -n "$raw_line" ]]; do
    server=$(echo "$raw_line" | tr -d '\r' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
    [[ -z "$server" || "$server" =~ ^# ]] && continue

    # Pass the script via base64 or stdin and attempt passwordless sudo to root first (`sudo -n bash`),
    # falling back to standard user (`bash`) if sudo requires a password or isn't permitted.
    remote_data=$(ssh -q \
        -i "$SSH_KEY" \
        -o BatchMode=yes \
        -o PasswordAuthentication=no \
        -o ConnectTimeout="$SSH_TIMEOUT" \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        "${SSH_USER}@${server}" \
        "sudo -n bash -c $(printf '%q' "$REMOTE_SCRIPT") 2>/dev/null || bash -c $(printf '%q' "$REMOTE_SCRIPT")" 2>/dev/null)

    ssh_exit_code=$?

    if [[ $ssh_exit_code -eq 0 && -n "$remote_data" ]]; then
        IFS="|" read -r actual_host priv_used rot_status os_date latest_key_date rsa_date rsa_fp ecdsa_date ecdsa_fp ed_date ed_fp diff_days primary_fp keygen_log <<< "$remote_data"

        # Print formatted row to terminal
        printf "%-24s | %-16s | %-6s | %-10s | %-16s | %-16s | %-9s | %s\n" \
            "$server" "$actual_host" "$priv_used" "$rot_status" "$os_date" "$latest_key_date" "${diff_days}d" "$primary_fp"

        # Write properly quoted row to CSV
        printf '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\n' \
            "$server" "$actual_host" "CONNECTED" "$priv_used" "$rot_status" "$os_date" \
            "$rsa_date" "$rsa_fp" "$ecdsa_date" "$ecdsa_fp" "$ed_date" "$ed_fp" "$diff_days" "$keygen_log" >> "$OUTPUT_CSV"
    else
        # Handle unreachable or authentication failures cleanly
        printf "%-24s | %-16s | %-6s | %-10s | %-16s | %-16s | %-9s | %s\n" \
            "$server" "-" "-" "UNREACHABLE" "-" "-" "-" "SSH Auth or Network Failed"

        printf '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\n' \
            "$server" "N/A" "UNREACHABLE_OR_AUTH_FAILED" "N/A" "N/A" "N/A" \
            "N/A" "N/A" "N/A" "N/A" "N/A" "N/A" "N/A" "N/A" >> "$OUTPUT_CSV"
    fi

done 3< "$SERVER_LIST"

echo ""
echo "Audit complete. CSV report saved to: $OUTPUT_CSV"
