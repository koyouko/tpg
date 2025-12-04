#!/bin/bash

export INC="${p:INC_NUMBER}"
export REQ="${p:REQ_NUMBER}"
export KAFKA_BOOTSTRAP="${p:BOOTSTRAP_SERVERS}"
export TOPIC="${p:KAFKA_TOPIC}"
export OTP="${p:OTP_PASSWORD}"
export REQUESTOR="${p:REQUESTOR}"

export CLUSTER="${p:CLUSTER_NAME}"
export ENV_NAME="${p:ENVIRONMENT_NAME}"

export ARTIFACTORY_BASE_URL="${p:ARTIFACTORY_BASE_URL}"
export ARTIFACTORY_USER="${p:ARTIFACTORY_USER}"
export ARTIFACTORY_PASSWORD="${p:ARTIFACTORY_PASSWORD}"

# For traceability in logs
export UCD_RUN_ID="${p:componentProcess.run.id}"

# Debug (mask sensitive values)
echo "INC=$INC"
echo "REQ=$REQ"
echo "TOPIC=$TOPIC"
echo "KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP"
echo "REQUESTOR=$REQUESTOR"
echo "Cluster=$CLUSTER Env=$ENV_NAME"
echo "Artifactory User=$ARTIFACTORY_USER"
echo "UCD Run ID=$UCD_RUN_ID"

chmod +x ${p:component.workspace}/scripts/kafka_dump.sh

${p:component.workspace}/scripts/kafka_dump.sh
