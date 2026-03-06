import os
import json
import uuid
import boto3
from datetime import datetime, timezone

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Authorization,Content-Type",
    "Access-Control-Allow-Methods": "POST,OPTIONS",
}

def _region():
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-southeast-1"

def lambda_handler(event, context):
    dynamodb = boto3.resource("dynamodb", region_name=_region())
    sqs      = boto3.client("sqs", region_name=_region())

    export_job_table = os.environ["EXPORT_JOB_TABLE"]
    export_queue_url = os.environ["EXPORT_QUEUE_URL"]

    body   = json.loads(event.get("body") or "{}")
    job_id = str(uuid.uuid4())
    now    = datetime.now(timezone.utc).isoformat()

    filters = {}
    if body.get("status"): filters["status"] = body["status"]
    if body.get("from"):   filters["from"]   = body["from"]
    if body.get("to"):     filters["to"]     = body["to"]

    table = dynamodb.Table(export_job_table)
    table.put_item(Item={
        "jobId":       job_id,
        "status":      "PENDING",
        "filters":     filters,
        "requestedBy": body.get("requestedBy", "anonymous"),
        "createdAt":   now,
    })

    sqs.send_message(
        QueueUrl=export_queue_url,
        MessageBody=json.dumps({"jobId": job_id, "filters": filters}),
        MessageGroupId="export-orders",
        MessageDeduplicationId=job_id,
    )

    return {
        "statusCode": 202,
        "headers": CORS_HEADERS,
        "body": json.dumps({"jobId": job_id, "status": "PENDING"}),
    }