Kafka Node Audit Script
This script audits a Red Hat Enterprise Linux 8 (RHEL8) system for readiness as a Confluent Platform (Kafka) broker, compares current kernel/sysctl tuning with Confluent's cp-ansible best practices, and can optionally update kernel tunables at runtime.

Features
Checks essential RHEL/system tuning for optimal Kafka performance

Compares live system values to cp-ansible kernel sysctl recommendations

Validates network, Java, ulimits, and key Kafka parameters

Prints a color-coded summary of system state vs. recommended values

Applies recommended sysctl kernel parameters at runtime if invoked with -apply (root required)

No changes are made unless -apply is specified

Usage
bash
./kafka_node_audit.sh
By default, runs in diagnostic mode: only reports differences, no system changes.

To apply cp-ansible kernel/sysctl recommendations at runtime:

bash
sudo ./kafka_node_audit.sh -apply
Use sudo or run as the root user to allow sysctl changes.

Note: Changes made with -apply affect the current running system/session. To persist across reboots, add tunables to /etc/sysctl.conf or /etc/sysctl.d/*.conf.

Requirements
RHEL 8 or compatible system

Bash (version 4+ recommended)

Utilities: sysctl, awk, ethtool (for NIC checks), sudo

Passwordless sudo for the Kafka service user (default: stekafka) is recommended for full ulimit inspection

What is Checked
Host OS, kernel, and virtualization detection

tuned profile and Transparent Huge Pages (THP)

Kernel sysctl tuning (both common Kafka and cp-ansible-standard)

Example keys: vm.swappiness, net.core.rmem_max, net.core.wmem_max, fs.file-max, net.core.somaxconn, net.ipv4.tcp_rmem, net.ipv4.tcp_wmem, and more

Network links, interfaces, and ring buffer sizes

NIC speed detection (including warning if < 25Gbps)

Bonding/link aggregation

Java and Confluent Platform versions

User limits (nofile, nproc, memlock) for service user

Key Kafka server.properties recommendations (threads, dirs, etc.)

Kafka broker running status

Example Output
PASS (green): Current value meets or exceeds Kafka/cp-ansible recommendations

WARN (yellow): Value below recommendation, or not configured as suggested

INFO (no color): Informational output

FAIL (red): Critical parameter missing or found to be unsatisfactory
