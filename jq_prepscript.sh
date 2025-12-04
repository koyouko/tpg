#!/bin/bash

# ---------------------------------------
# Require root privileges
# ---------------------------------------
if [[ $EUID -ne 0 ]]; then
  echo "[ERROR] This script must be run as root."
  exit 1
fi

# ---------------------------------------
# Verify jq installation (RHEL 8)
# ---------------------------------------
if ! command -v jq >/dev/null 2>&1; then
  echo "[INFO] jq not found. Installing using yum..."
  yum install -y jq
  if [[ $? -ne 0 ]]; then
      echo "[ERROR] Failed to install jq on RHEL 8."
      exit 1
  fi
  echo "[INFO] jq installed successfully."
else
  echo "[INFO] jq is already installed."
fi

# ---------------------------------------
# Variables
# ---------------------------------------
SOEID="${p:SOEID}"
INPUT_HST_NAMES="${p:INPUT_HST_NAMES}"
TOPICS="${p:TOPICS}"

CURRENT_HOST=$(hostname)

SCRIPT_DIR="/home/stekafka/scripts"
ARTIFACT_URL="https://www.artifactrepository.citigroup.net/artifactory/generic-icg-dev-local/tts-bdste-kafka-168898/2/szm/stp_topic_dump.sh"
OUTFILE="${SCRIPT_DIR}/stp_topic_dump.sh"

# ---------------------------------------
# Ensure directory exists and has correct ownership
# ---------------------------------------
if [[ ! -d "$SCRIPT_DIR" ]]; then
    echo "[INFO] Directory does not exist. Creating: $SCRIPT_DIR"
    mkdir -p "$SCRIPT_DIR"

    # Set correct ownership on creation
    chown stekafka:stekafka "$SCRIPT_DIR"
else
    echo "[INFO] Directory exists: $SCRIPT_DIR"

    # Validate ownership if directory already exists
    OWNER=$(stat -c %U "$SCRIPT_DIR")
    GROUP=$(stat -c %G "$SCRIPT_DIR")

    if [[ "$OWNER" != "stekafka" || "$GROUP" != "stekafka" ]]; then
        echo "[WARN] Ownership mismatch. Fixing ownership on $SCRIPT_DIR"
        chown -R stekafka:stekafka "$SCRIPT_DIR"
    else
        echo "[INFO] Ownership for $SCRIPT_DIR is already correct."
    fi
fi

# ---------------------------------------
# Curl Download with Retry Logic
# ---------------------------------------
download_with_retry() {
    local url="$1"
    local outfile="$2"
    local max_attempts=3
    local attempt=1
    local delay=5

    while (( attempt <= max_attempts )); do
        echo "[INFO] Attempt $attempt: Downloading stp_topic_dump.sh..."

        curl -k -s \
          -H "X-JFrog-Art-Api:AKCpBkr1Jxn2hjiW0hlLmFZmKdTDMFptFvrmcjt3PYbrNTULx3GgWTz1lRw1qe1xERk1znz6V" \
          -o "$outfile" "$url"

        if [[ $? -eq 0 && -s "$outfile" ]]; then
            echo "[INFO] Download successful."
            return 0
        fi

        echo "[WARN] Download failed. Retrying in ${delay}s..."
        sleep $delay
        attempt=$((attempt + 1))
        delay=$((delay * 2))   # exponential backoff
    done

    echo "[ERROR] All download attempts failed."
    return 1
}

# ---------------------------------------
# Main Execution Logic
# ---------------------------------------
if [[ "$INPUT_HST_NAMES" == "$CURRENT_HOST" ]]; then

    # Download the script with retry logic
    if ! download_with_retry "$ARTIFACT_URL" "$OUTFILE"; then
        exit 1
    fi

    # Ensure ownership and permissions of downloaded file
    chown stekafka:stekafka "$OUTFILE"
    chmod 755 "$OUTFILE"

    # Execute the downloaded script
    sh "$OUTFILE" "$SOEID" "$INPUT_HST_NAMES" "$TOPICS"

    exit 0
fi
