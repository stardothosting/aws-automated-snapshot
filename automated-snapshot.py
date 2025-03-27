#!/usr/bin/env python3

import boto3
import logging
import traceback
import sys
import argparse
import importlib.util
from datetime import datetime, timedelta

# Load config.py if it exists, else fallback to embedded default
DEFAULT_CONFIG = {
    'tagname': 'Snapshot',
    'tagvalues': ['Yes'],
    'retention_days': 7,
    'sns_topic': None
}

def load_config():
    spec = importlib.util.find_spec("config")
    if spec is not None:
        try:
            config_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(config_module)
            return config_module.config
        except Exception as e:
            logging.error("Failed to load config.py, using default config.")
            logging.error(traceback.format_exc())
    return DEFAULT_CONFIG

# Load configuration
config = load_config()

# Argument parsing
parser = argparse.ArgumentParser(description='Create EBS volume snapshots with tag filtering.')
parser.add_argument('--debug', action='store_true', help='Enable debug logging')
args = parser.parse_args()

# Logging config
logging.basicConfig(
    level=logging.DEBUG if args.debug else logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# AWS clients
ec2 = boto3.client('ec2')
sns = boto3.client('sns')

def get_volumes():
    try:
        filters = [{
            'Name': f'tag:{config["tagname"]}',
            'Values': config['tagvalues']
        }]
        result = ec2.describe_volumes(Filters=filters)
        return result['Volumes']
    except Exception as e:
        logging.error("Error fetching volumes:")
        logging.error(traceback.format_exc())
        return []

def create_snapshot(volume):
    try:
        vol_id = volume['VolumeId']
        description = f"Automated snapshot for {vol_id}"
        logging.info(f"Creating snapshot for volume {vol_id}")

        tags = volume.get('Tags', [])
        snapshot = ec2.create_snapshot(
            VolumeId=vol_id,
            Description=description,
            TagSpecifications=[{
                'ResourceType': 'snapshot',
                'Tags': tags
            }]
        )
        snapshot_id = snapshot['SnapshotId']
        logging.info(f"Created snapshot {snapshot_id} for volume {vol_id}")
        return snapshot_id
    except Exception as e:
        logging.error(f"Failed to create snapshot for volume {volume.get('VolumeId', 'Unknown')}:")
        logging.error(traceback.format_exc())
        return None

def cleanup_snapshots(volume):
    try:
        vol_id = volume['VolumeId']
        retention_days = int(config.get('retention_days', 7))
        cutoff = datetime.utcnow() - timedelta(days=retention_days)

        snapshots = ec2.describe_snapshots(
            Filters=[
                {'Name': 'volume-id', 'Values': [vol_id]},
                {'Name': f'tag:{config["tagname"]}', 'Values': config['tagvalues']}
            ],
            OwnerIds=['self']
        )['Snapshots']

        for snap in snapshots:
            start_time = snap['StartTime'].replace(tzinfo=None)
            if start_time < cutoff:
                snap_id = snap['SnapshotId']
                ec2.delete_snapshot(SnapshotId=snap_id)
                logging.info(f"Deleted snapshot {snap_id} (created on {start_time})")
    except Exception as e:
        logging.error(f"Error during snapshot cleanup for volume {volume.get('VolumeId', 'Unknown')}:")
        logging.error(traceback.format_exc())

def send_notification(message):
    topic_arn = config.get('sns_topic')
    if not topic_arn:
        return

    try:
        sns.publish(
            TopicArn=topic_arn,
            Subject="EBS Snapshot Notification",
            Message=message
        )
        logging.info("SNS notification sent.")
    except Exception as e:
        logging.error("Failed to send SNS notification:")
        logging.error(traceback.format_exc())

def main():
    try:
        volumes = get_volumes()
        if not volumes:
            logging.info("No volumes found for snapshotting.")
            return

        snap_ids = []
        for volume in volumes:
            snap_id = create_snapshot(volume)
            if snap_id:
                snap_ids.append(snap_id)
            cleanup_snapshots(volume)

        msg = f"{len(snap_ids)} snapshot(s) created: {', '.join(snap_ids)}" if snap_ids else "No snapshots created."
        send_notification(msg)
    except Exception as e:
        logging.error("Unhandled error in main execution:")
        logging.error(traceback.format_exc())

if __name__ == '__main__':
    main()

