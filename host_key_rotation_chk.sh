#!/usr/bin/env bash
# ==============================================================================
# Script Name: check_host_keys.sh
# Description: Reads a server list, connects via passwordless SSH key, checks if
#              RHEL SSH host keys have been rotated, prints a formatted table,
#              and exports the results to a CSV file.
# Usage:       ./check_host_keys.sh [server_list.txt] [ssh_user] [path_to_ssh_key]
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
echo '"Server_Input","Actual_Hostname","Connection_Status","Rotation_Status","OS_Install_Date","RSA_Key_Date","RSA_Fingerprint","ECDSA_Key_Date","ECDSA_Fingerprint","ED25519_Key_Date","ED25519_Fingerprint","Days_Diff_From_OS"' > "$OUTPUT_CSV"

# --- Print Console Table Header ---
printf "\n%-25s | %-18s | %-12s | %-16s | %-16s | %-10s | %s\n" \
    "SERVER" "HOSTNAME" "STATUS" "OS INSTALL DATE" "LATEST KEY DATE" "DAYS DIFF" "ED25519 / PRIMARY FINGERPRINT"
printf '%.0s-' {1..145}
echo ""

# --- Read Server List Line by Line ---
# Using fd 3 so ssh does not consume the while loop's standard input
while IFS= read -r raw_line <&3 || [[ -n "$raw_line" ]]; do
    # Strip carriage returns (\r), leading/trailing whitespace, and skip comments/empty lines
    server=$(echo "$raw_line" | tr -d '\r' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
    [[ -z "$server" || "$server" =~ ^# ]] && continue

    # Execute remote check via passwordless SSH
    # StrictHostKeyChecking=no and UserKnownHostsFile=/dev/null ensure rotated host keys don't block the audit
    remote_data=$(ssh -q -n \
        -i "$SSH_KEY" \
        -o BatchMode=yes \
        -o PasswordAuthentication=no \
        -o ConnectTimeout="$SSH_TIMEOUT" \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        "${SSH_USER}@${server}" '
        export LC_ALL=C
        ACTUAL_HOST=$(hostname -s 2>/dev/null || hostname)

        # Get RHEL OS install timestamp from basesystem RPM (fallback to root filesystem birth/ctime)
        OS_TS=$(rpm -q --qf "%{INSTALLTIME}\n" basesystem 2>/dev/null | head -n 1)
        if [ -z "$OS_TS" ] || ! [ "$OS_TS" -eq "$OS_TS" ] 2>/dev/null; then
            OS_TS=$(stat -c %W / 2>/dev/null)
            [ -z "$OS_TS" ] || [ "$OS_TS" -le 0 ] && OS_TS=$(stat -c %Z / 2>/dev/null)
        fi
        OS_DATE=$(date -d @"$OS_TS" "+%Y-%m-%d %H:%M" 2>/dev/null || echo "Unknown")

        get_key_meta() {
            local kfile="/etc/ssh/ssh_host_${1}_key.pub"
            if [ -r "$kfile" ]; then
                local kts=$(stat -c %Y "$kfile" 2>/dev/null)
                local kdate=$(date -d @"$kts" "+%Y-%m-%d %H:%M" 2>/dev/null)
                local kfp=$(ssh-keygen -lf "$kfile" 2>/dev/null | awk "{print \$2}")
                echo "${kts}|${kdate}|${kfp}"
            else
                echo "0|N/A|N/A"
            fi
        }

        IFS="|" read -r RSA_TS RSA_DATE RSA_FP <<< "$(get_key_meta rsa)"
        IFS="|" read -r ECDSA_TS ECDSA_DATE ECDSA_FP <<< "$(get_key_meta ecdsa)"
        IFS="|" read -r ED_TS ED_DATE ED_FP <<< "$(get_key_meta ed25519)"

        # Find the newest host key timestamp across RSA, ECDSA, and ED25519
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

        # Determine Primary Fingerprint (prefer ED25519 -> ECDSA -> RSA)
        PRIMARY_FP="$ED_FP"
        [ "$PRIMARY_FP" = "N/A" ] && PRIMARY_FP="$ECDSA_FP"
        [ "$PRIMARY_FP" = "N/A" ] && PRIMARY_FP="$RSA_FP"

        # Calculate difference in days between OS installation and latest host key modification
        if [ "$MAX_KEY_TS" -gt 0 ] && [ -n "$OS_TS" ] && [ "$OS_TS" -gt 0 ]; then
            DIFF_SEC=$(( MAX_KEY_TS - OS_TS ))
            DIFF_DAYS=$(( DIFF_SEC / 86400 ))
            # If host keys were modified >24 hours (86400s) after OS build, mark as ROTATED
            if [ "$DIFF_SEC" -gt 86400 ]; then
                ROT_STATUS="ROTATED"
            else
                ROT_STATUS="ORIGINAL"
            fi
        else
            DIFF_DAYS="N/A"
            ROT_STATUS="UNKNOWN"
        fi

        echo "${ACTUAL_HOST}|${ROT_STATUS}|${OS_DATE}|${LATEST_KEY_DATE}|${RSA_DATE}|${RSA_FP}|${ECDSA_DATE}|${ECDSA_FP}|${ED_DATE}|${ED_FP}|${DIFF_DAYS}|${PRIMARY_FP}"
    ' 2>/dev/null)

    ssh_exit_code=$?

    if [[ $ssh_exit_code -eq 0 && -n "$remote_data" ]]; then
        IFS="|" read -r actual_host rot_status os_date latest_key_date rsa_date rsa_fp ecdsa_date ecdsa_fp ed_date ed_fp diff_days primary_fp <<< "$remote_data"

        # Print formatted row to terminal
        printf "%-25s | %-18s | %-12s | %-16s | %-16s | %-10s | %s\n" \
            "$server" "$actual_host" "$rot_status" "$os_date" "$latest_key_date" "${diff_days}d" "$primary_fp"

        # Write properly quoted row to CSV
        printf '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\n' \
            "$server" "$actual_host" "CONNECTED" "$rot_status" "$os_date" \
            "$rsa_date" "$rsa_fp" "$ecdsa_date" "$ecdsa_fp" "$ed_date" "$ed_fp" "$diff_days" >> "$OUTPUT_CSV"
    else
        # Handle unreachable or authentication failures cleanly
        printf "%-25s | %-18s | %-12s | %-16s | %-16s | %-10s | %s\n" \
            "$server" "-" "UNREACHABLE" "-" "-" "-" "SSH Auth or Network Failed"

        printf '"%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"\n' \
            "$server" "N/A" "UNREACHABLE_OR_AUTH_FAILED" "N/A" "N/A" \
            "N/A" "N/A" "N/A" "N/A" "N/A" "N/A" "N/A" >> "$OUTPUT_CSV"
    fi

done 3< "$SERVER_LIST"

echo ""
echo "Audit complete. CSV report saved to: $OUTPUT_CSV"
