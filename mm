Kafka Topic Approval and Automation Process

Date/Time: January 27, 2026, 09:24 AM EST

Overall Summary

Rajeev and Hashem discussed the Kafka topic approval workflow, focusing on the CMP (Change Management Process) request system and potential automation via a self-service portal. The CMP acts as a marketplace where approval is granted for Kafka topic creation, including architecture review and capacity checks. They explored integrating CMP status checks into the portal to streamline topic onboarding.

Key Points
	•	Kafka topic creation requires CMP approval, which includes submitting detailed questionnaires and architecture review
	•	Approval is valid across all environments (dev to production) and is tracked via CMP numbers during onboarding
	•	Current topic creation is semi-automated; plans exist to build a fully automated self-service portal that queries CMP status and manages approvals
	•	Manual checks include verifying cluster existence, capacity, and onboarding status to discovery portal before proceeding with topic creation
	•	The CMP marketplace likely supports APIs for integration, enabling the portal to pull approval status and streamline governance

Action Items
	•	Hashem to share the standard CMP questionnaire and any relevant documentation with Rajeev
	•	Rajeev to provide any existing documentation on the CMP process and Kafka topic onboarding
	•	Hashem to send the latest architecture review diagram to Rajeev for confirmation of process consistency

Open Questions
	•	Is the CMP marketplace API fully accessible for integration with the self-service portal?
	•	Are there any recent changes to the Kafka topic approval process beyond the documented version 1.0?
	•	What is the timeline for moving from semi-automated to fully automated topic creation in the portal?
