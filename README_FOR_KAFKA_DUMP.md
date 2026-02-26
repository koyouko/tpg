# Kafka STP Topic Dump

Automated Kafka topic dump utility for incident response and data recovery. Consumes all messages from a specified Kafka topic, encrypts the output with AES-256 (GPG), uploads the artifact to Artifactory, and sends an HTML email notification to the requestor.

**Version:** 3.0.0

---

## Overview

The solution consists of two scripts:

| Script | Purpose |
|--------|---------|
| `ucd_kafka_dump_wrapper.sh` | UCD process step — resolves UCD properties and invokes the main script with named CLI parameters |
| `stp_kafka_dump.sh` | Main script — dump, encrypt, upload, notify |

### Execution Flow

```
UCD Process Step
  └─ ucd_kafka_dump_wrapper.sh
       ├─ Downloads latest stp_kafka_dump.sh from Artifactory
       └─ Calls stp_kafka_dump.sh --inc ... --req ... --topic ...
            ├─ 1. Validate inputs & check disk space
            ├─ 2. Consume topic (kafka-console-consumer)
            ├─ 3. Create metadata.json
            ├─ 4. tar + gzip + GPG encrypt (AES-256)
            ├─ 5. SHA-256 checksum
            ├─ 6. Upload .gpg + .sha256 to Artifactory
            ├─ 7. Send HTML email notification
            └─ 8. Output JSON result & cleanup
```

---

## Prerequisites

- **Confluent Kafka** binaries installed at `/opt/confluent/latest/bin/` (or in `$PATH`)
- **Kafka client config** at `/home/stekafka/config/stekafka_client.properties` (SSL/SASL credentials)
- **GPG** (GnuPG) for symmetric AES-256 encryption
- **curl** for Artifactory uploads
- **sendmail** (`/usr/sbin/sendmail`) for HTML email notifications
- **coreutils** (`shred`, `stat`, `sha256sum`, `timeout`)

---

## Usage

### CLI Parameters

```bash
bash stp_kafka_dump.sh [OPTIONS]
```

#### Required

| Flag | Description | Example |
|------|-------------|---------|
| `--inc` | Incident number | `INC852369` |
| `--req` | Request number | `REQ452145` |
| `--topic` | Kafka topic name | `confluent-audit-log-events` |
| `--bootstrap` | Bootstrap server(s), comma-separated | `broker1:9094,broker2:9094` |
| `--otp` | One-time password for GPG encryption | *(secure property)* |
| `--artifactory-url` | Artifactory base URL | `https://artifactory.example.com/artifactory/repo` |
| `--artifactory-user` | Artifactory username | `rr78709` |
| `--artifactory-pass` | Artifactory password | *(secure property)* |

#### Optional

| Flag | Default | Description |
|------|---------|-------------|
| `--requestor` | `ucd-user` | Requestor identifier (logged in metadata and audit) |
| `--run-id` | epoch seconds | UCD run ID for traceability |
| `--soeid` | *(empty)* | SOE ID — if provided, sends email to `SOEID@citi.com` |
| `--email-cc` | `dl.icg.global.kafka.ste.admin@imcnam.ssmb.com` | CC address for notification email |
| `--timeout-ms` | `120000` | Kafka consumer idle timeout in milliseconds |
| `--max-messages` | *(unlimited)* | Maximum number of messages to consume |
| `--wall-timeout` | `600` | Hard wall-clock timeout in seconds (kills consumer if exceeded) |

### Example — Standalone

```bash
bash stp_kafka_dump.sh \
    --inc           "INC852369" \
    --req           "REQ452145" \
    --topic         "confluent-audit-log-events" \
    --bootstrap     "broker1.example.com,broker2.example.com" \
    --otp           "my-secret-otp" \
    --requestor     "rr78709" \
    --artifactory-url  "https://artifactory.example.com/artifactory/generic-repo" \
    --artifactory-user "rr78709" \
    --artifactory-pass "secret" \
    --soeid         "rr78709" \
    --max-messages  500000 \
    --wall-timeout  300
```

### Example — UCD Wrapper

The wrapper resolves `${p:...}` UCD properties and passes them as CLI flags:

```bash
bash stp_kafka_dump.sh \
    --inc               "${p:INC_NUMBER}" \
    --req               "${p:REQ_NUMBER}" \
    --topic             "${p:KAFKA_TOPIC}" \
    --bootstrap         "${p:INPUT_HST_NAMES}" \
    --otp               "${p:OTP_PASSWORD}" \
    --requestor         "${p:REQUESTOR}" \
    --artifactory-url   "https://www.artifactrepository.citigroup.net/..." \
    --artifactory-user  "${p:ARTIFACTORY_USER}" \
    --artifactory-pass  "${p:ARTIFACTORY_PASSWORD}" \
    --run-id            "${p:componentProcess.run.id}" \
    --soeid             "${p:REQUESTOR}" \
    --email-cc          "dl.icg.global.kafka.ste.admin@imcnam.ssmb.com"
```

> **Note:** `--soeid` uses `${p:REQUESTOR}` since the SOEID maps to the requestor's ID.

---

## Security

### Credential Handling

| Concern | Mitigation |
|---------|------------|
| Password exposure in environment | All inputs are CLI parameters — no `export` statements. Passwords are never stored in environment variables. |
| Password exposure in process list | Artifactory credentials are written to a `chmod 600` temp file and passed via `curl -H @file`. |
| Sensitive data on disk | Dump files, tarballs, and compressed archives are tracked and securely removed (`shred -u`) on exit via a trap handler. |
| OTP password | Passed to GPG via `--passphrase-fd 0` (stdin), never written to disk. |

### Input Validation

All user-supplied parameters are validated against strict regex patterns to prevent path traversal and injection:

- `INC`, `REQ`: `^[A-Za-z0-9_-]+$`
- `TOPIC`: `^[A-Za-z0-9._-]+$`
- `REQUESTOR`: `^[A-Za-z0-9._@-]+$`

### Cleanup

A `trap cleanup EXIT` handler runs on every exit (success or failure) and:

1. Shreds sensitive intermediate files (dump, tar, gz)
2. Removes temp files (curl response bodies)
3. Removes the working directory
4. Shreds the curl header file containing Artifactory credentials

---

## Output

### JSON (stdout)

On success, the script prints a single JSON line to stdout:

```json
{
  "status": "OK",
  "artifactory_url": "https://artifactory.example.com/.../REQ452145.tar.gz.gpg",
  "messages": 148230,
  "size_bytes": 39241728,
  "sha256": "a1b2c3d4e5f6..."
}
```

On failure:

```json
{
  "status": "ERROR",
  "message": "Kafka dump failed after retries"
}
```

### Artifactory Artifacts

Uploaded to: `<ARTIFACTORY_BASE_URL>/kafka-dump/INC/<INC>/<REQ>/<TOPIC>/`

| File | Description |
|------|-------------|
| `<REQ>.tar.gz.gpg` | AES-256 encrypted archive containing the dump and metadata |
| `<REQ>.tar.gz.gpg.sha256` | SHA-256 checksum of the encrypted file |

### Email Notification

An HTML-formatted email is sent via `sendmail` on successful completion:

| Field | Value |
|-------|-------|
| **From** | `dl.icg.global.kafka.ste.admin@imcnam.ssmb.com` |
| **To** | `<SOEID>@citi.com` |
| **CC** | `dl.icg.global.kafka.ste.admin@imcnam.ssmb.com` (configurable via `--email-cc`) |
| **Subject** | `Kafka STP : <TOPIC> — Dump Complete [<INC>]` |

The email includes: job details (INC, REQ, topic, bootstrap, message count, encrypted size, SHA-256, requestor, run ID), a clickable Artifactory download link, and decryption instructions.

> Email is skipped if `--soeid` is not provided (logged as a non-fatal warning).

---

## Decrypting the Dump

The recipient decrypts the artifact using the OTP password from the original request:

```bash
# Decrypt and extract in one step
gpg --decrypt REQ452145.tar.gz.gpg | tar xzf -

# This produces:
#   confluent-audit-log-events.jsonl   — raw Kafka messages
#   metadata.json                       — dump metadata
```

### metadata.json

```json
{
  "inc": "INC852369",
  "req": "REQ452145",
  "topic": "confluent-audit-log-events",
  "bootstrap": "broker1:9094,broker2:9094",
  "message_count": 148230,
  "dump_size_bytes": 37421056,
  "run_id": "1740412496",
  "requestor": "rr78709",
  "timestamp": "2026-02-24T19:16:14Z",
  "script_version": "3.0.0"
}
```

---

## Logging & Audit

### Run Log

Location: `/var/log/confluent/kafka_dump/<INC>/<REQ>/<TOPIC>/<RUN_ID>/kafka_dump.log`

Structured JSON lines with timestamp, level, run context, and message. Cleaned up after successful completion.

### Audit Log

Location: `/var/log/confluent/kafka_dump/audit.log`

Persistent, append-only log of all dump operations across runs. Events: `START`, `DUMP_COMPLETE`, `UPLOAD_COMPLETE`, `SUCCESS`, `FAILURE`.

---

## Timeouts & Safeguards

| Safeguard | Default | Flag |
|-----------|---------|------|
| Consumer idle timeout | 120s | `--timeout-ms` |
| Wall-clock hard timeout | 600s (10 min) | `--wall-timeout` |
| Max messages | unlimited | `--max-messages` |
| Max dump file size | 5 GB | *(hardcoded: `MAX_DUMP_SIZE_MB`)* |
| Disk usage threshold | 85% | *(hardcoded: `DISK_THRESHOLD`)* |
| Upload timeout (curl) | 300s | *(hardcoded: `CURL_TIMEOUT`)* |
| Retry attempts (consumer) | 3 | *(hardcoded)* |
| Retry attempts (upload) | 3 | *(hardcoded)* |

If the wall-clock timeout is hit, the consumer is killed and the script continues with a partial dump (logged as a warning).

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| Script hangs indefinitely | Topic has active producers; consumer never idles | Use `--max-messages` or reduce `--wall-timeout` |
| `p: unbound variable` | UCD property not defined | Ensure the property exists in UCD or use a fallback (e.g., `${p:REQUESTOR}` for SOEID) |
| Email shows raw HTML | `mailx` doesn't honor content-type flag | Already fixed — script uses `sendmail` with explicit MIME headers |
| `Disk usage exceeds limit` | Partition >85% full | Free space on `/var/log/confluent/` or adjust `DISK_THRESHOLD` |
| `Dump file exceeds maximum` | Topic has >5 GB of data | Use `--max-messages` to cap output or increase `MAX_DUMP_SIZE_MB` |
| `GPG encryption failed` | Bad OTP or GPG not installed | Verify GPG is available and OTP is non-empty |
| Upload returns HTTP 403 | Artifactory credentials invalid or repo permissions | Verify `--artifactory-user` / `--artifactory-pass` and repo write access |

---

## File Structure

```
/var/log/confluent/kafka_dump/
├── audit.log                              # Persistent audit log (all runs)
└── <INC>/
    └── <REQ>/
        └── <TOPIC>/
            └── <RUN_ID>/
                ├── kafka_dump.log         # Run-specific log
                ├── <TOPIC>.jsonl          # Raw dump (shredded after encryption)
                ├── metadata.json          # Job metadata
                ├── <REQ>.tar             # Intermediate (shredded)
                ├── <REQ>.tar.gz          # Intermediate (shredded)
                ├── <REQ>.tar.gz.gpg      # Encrypted artifact (uploaded)
                └── <REQ>.tar.gz.gpg.sha256  # Checksum (uploaded)
```

> The working directory is removed on exit. Only `audit.log` persists.
