import boto3
import json
import os
from pathlib import Path

config_path = Path(__file__).parent / "config.json"
with open(config_path) as f:
    config = json.load(f)

def lambda_handler(event, context):
    sqs = boto3.client('sqs')
    QUEUE_URL = os.environ.get("QUEUE_URL")
    regions = config['regions']
    for region in regions:
        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps({
                'region_id': region['region_id']
            })
        )
    return {
        "statusCode": 200,
        "body": "Successfully sent messages to SQS."
    }
