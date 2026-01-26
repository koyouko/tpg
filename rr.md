Root Cause Analysis (RCA)

Incident: Kafka client connectivity failure after broker certificate renewal
Client: CSF
Platform Team: STP

Summary

During a scheduled certificate renewal, Kafka broker certificates were upgraded from Citi Device CA1 G2 to Citi Device CA2 G2. After the upgrade, CSF client applications were unable to establish TLS connections due to a missing intermediate CA certificate in the client trust store.

To immediately restore service, the STP team temporarily rolled back the broker certificate to Citi Device CA1 G1, which was already trusted by the client. However, Citi Device CA1 G1 is deprecated and cannot be used as a long-term solution. The target and compliant state remains Citi Device CA2 G2.

Impact

CSF client applications experienced TLS connection failures to Kafka brokers.

Kafka message flow for CSF applications was interrupted.

No data loss occurred.

Temporary reliance on a deprecated CA to restore service.

Timeline

T0: Kafka broker certificates upgraded from Citi Device CA1 G2 to Citi Device CA2 G2.

T0 + shortly after: CSF applications failed TLS handshakes.

T1: STP identified missing Citi Device CA2 G2 intermediate CA in CSF trust store.

T2: Temporary rollback to Citi Device CA1 G1 to restore connectivity.

T3: CSF connectivity restored.

T4 (Planned): Re-deploy Citi Device CA2 G2 after client trust store validation.

Root Cause

The CSF client trust store did not include the Citi Device CA2 G2 intermediate CA, resulting in TLS certificate chain validation failure after the broker certificate upgrade.

Contributing Factors

Certificate renewal involved a change in CA hierarchy.

Client trust store readiness was not validated prior to the broker certificate upgrade.

Operational urgency required a temporary rollback to restore service.

Resolution

Kafka broker certificates were temporarily rolled back to Citi Device CA1 G1 to restore service.

This rollback is temporary only, as G1 is deprecated.

Risk & Compliance Statement

Citi Device CA1 G1 is deprecated and must be fully retired.

Kafka brokers must operate with Citi Device CA2 G2 to remain compliant.

Temporary rollback approved strictly as a short-term mitigation.

Corrective & Preventive Actions
Immediate

CSF to add Citi Device CA2 G2 intermediate CA to the client trust store.

Short-Term

Ensure all Kafka clients have fully moved away from CA1 or have the complete CA2 certificate chain present in their trust stores.

Re-deploy Kafka broker certificates using Citi Device CA2 G2 after confirming client readiness.

Long-Term

Maintain an inventory of all Kafka clients and their trusted CA chains.

Enforce a mandatory validation checkpoint to confirm all clients trust CA2 before retiring CA1.

Prevent long-term use of deprecated CAs without formal risk acceptance.

Update Kafka operational runbooks to include certificate chain dependency checks and client confirmation steps.

Status

Service restored via temporary mitigation

Permanent remediation in progress

Target state: All Kafka brokers and clients operating exclusively with Citi Device CA2 G2
