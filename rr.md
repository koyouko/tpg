Sub-task 1: Automate Consumer Group Deletion

Description
Automate deletion of Kafka consumer groups with strict validation and safety controls.

Scope

Delete explicitly specified consumer groups only

Validate group existence and ensure group state is Empty

Capture offset snapshot before deletion

Block protected/system consumer groups

Acceptance Criteria

Dry-run shows groups targeted for deletion

Deletion fails if group has active members

Audit log includes group name, offsets snapshot, and execution result

No wildcard deletions allowed in PROD

Sub-task 2: Automate Topic Deletion

Description
Automate Kafka topic deletion with pre-checks and metadata capture.

Scope

Delete explicitly listed topics only

Block deletion of internal/protected topics

Capture topic metadata (partitions, RF, configs) before deletion

Validate delete.topic.enable=true

Acceptance Criteria

Dry-run displays topics and metadata to be deleted

Protected topics cannot be deleted

Topic config snapshot is stored before deletion

Audit log captures topic name and execution outcome

Sub-task 3: Automate Kafka ACL Grant and Removal

Description
Provide controlled automation for managing Kafka ACLs using ZooKeeper-backed authorization.

Scope

Grant and revoke ACLs for Topic, Group, Cluster, and TransactionalId

Support LITERAL and restricted PREFIXED patterns

Display ACL diff in dry-run mode

Validate principal and resource inputs

Acceptance Criteria

Dry-run shows exact ACL changes (before vs after)

No wildcard ACLs allowed in PROD

ACL rollback supported via inverse operation

ACL changes are fully auditable

Sub-task 4: Automate Consumer Group Offset Management

Description
Automate consumer group offset reset, export, and import operations.

Scope

Offset reset strategies:

earliest, latest

timestamp, duration

absolute offset

Enforce inactive (Empty) consumer group requirement

Export offsets prior to modification

Acceptance Criteria

Dry-run displays offset delta before execution

Offset reset fails if group is active

Offset snapshot is captured before apply

Rollback supported if snapshot exists

Sub-task 5: Automate Topic Message Count Calculation

Description
Provide topic message count capability using safe, offset-based estimation.

Scope

Calculate message count using:

endOffset - earliestOffset per partition

Provide partition-level and aggregated output

Clearly label counts as estimates

Acceptance Criteria

Output includes per-partition offsets and total count

Compacted topic caveats are documented

No full topic scan in PROD by default

Results are timestamped and auditable

Security & Compliance Requirements

mTLS authentication using platform-approved certificates

Kafka ACL validation before execution

Protected resources enforced via deny-list

Immutable audit logs per execution

PROD execution requires ticket reference and approval metadata

Dependencies

Kafka Admin client access with mTLS

Required Kafka ACLs for admin service principal

Approved protected resource allow/deny lists

Definition of Done

All sub-tasks implemented and tested in DEV and UAT

PROD execution gated and validated
