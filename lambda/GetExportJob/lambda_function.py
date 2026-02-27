import os
import json
import boto3
from datetime import datetime, timezone

dynamodb = boto3.resource("dynamodb")
s3       = boto3.client("s3")

EXPORT_JOB_TABLE = os.environ["EXPORT_JOB_TABLE"]
REPORTS_BUCKET   = os.environ["REPORTS_BUCKET"]
PRESIGNED_EXPIRY = int(os.environ.get("PRESIGNED_EXPIRY", 3600))  # 1 hour default


def lambda_handler(event, context):
    job_id = event["pathParameters"]["jobId"]
    table  = dynamodb.Table(EXPORT_JOB_TABLE)

    result = table.get_item(Key={"jobId": job_id})
    job    = result.get("Item")

    if not job:
        return {
            "statusCode": 404,
            "body": json.dumps({"message": f"Job {job_id} not found"}),
        }

    response = {
        "jobId":       job["jobId"],
        "status":      job["status"],
        "createdAt":   job.get("createdAt"),
        "completedAt": job.get("completedAt"),
    }

    # If DONE, generate presigned URL on-demand
    if job["status"] == "DONE" and job.get("s3Key"):
        presigned_url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": REPORTS_BUCKET, "Key": job["s3Key"]},
            ExpiresIn=PRESIGNED_EXPIRY,
        )
        expires_at = datetime.now(timezone.utc).timestamp() + PRESIGNED_EXPIRY

        response["downloadUrl"] = presigned_url
        response["expiresAt"]   = datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat()
        response["s3Key"]       = job["s3Key"]

    if job["status"] == "FAILED":
        response["errorMessage"] = job.get("errorMessage")

    return {
        "statusCode": 200,
        "body": json.dumps(response, default=str),
    }