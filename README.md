# aws-automated-snapshot

Automated EBS snapshot creation and cleanup using Python 3, Boto3, and AWS volume tags.

This script scans your AWS EBS volumes for a specific tag (e.g., `Snapshot:Yes`), creates snapshots, tags them accordingly, deletes old snapshots based on a retention policy, and optionally sends SNS notifications.

