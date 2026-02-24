#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# UCD Wrapper – calls stp_kafka_dump.sh with named parameters
# instead of exported environment variables (avoids leaking
# passwords via /proc/*/environ or `ps e`).
# ============================================================================

CURRENT_HOST=$(hostname)

BOOTSTRAP_SERVERS="${p:INPUT_HST_NAMES}"

# Only run on the matching Kafka host
if [[ "$BOOTSTRAP_SERVERS" == "$CURRENT_HOST" ]]; then

    echo "Im in the if"

    cd /home/stekafka/scripts

    # Download latest script from Artifactory
    curl -k -s \
        -H "X-JFrog-Art-Api:" \
        -O "https://www.artifactrepository.citigroup.net:443/artifactory/generic-icg-dev-local/tts-stpemp-kafka/stp_kafka_dump.sh"

    chown stekafka:stekafka /home/stekafka/scripts/*
    chmod 755 /home/stekafka/scripts/*

    # Call the script with named parameters
    bash /home/stekafka/scripts/stp_kafka_dump.sh \
        --inc               "${p:INC_NUMBER}" \
        --req               "${p:REQ_NUMBER}" \
        --topic             "${p:KAFKA_TOPIC}" \
        --bootstrap         "${p:INPUT_HST_NAMES}" \
        --otp               "${p:OTP_PASSWORD}" \
        --requestor         "${p:REQUESTOR}" \
        --artifactory-url   "https://www.artifactrepository.citigroup.net/artifactory/generic-icg-dev-local/tts-stpemp-kafka-161782" \
        --artifactory-user  "${p:ARTIFACTORY_USER}" \
        --artifactory-pass  "${p:ARTIFACTORY_PASSWORD}" \
        --run-id            "${p:componentProcess.run.id}" \
        --soeid             "${p:SOEID}" \
        --email-cc          "dl.icg.global.kafka.ste.admin@imcnam.ssmb.com"

    exit 0
    echo "after exit 0"
fi

echo "$BOOTSTRAP_SERVERS"
echo "$CURRENT_HOST"
echo "done"
