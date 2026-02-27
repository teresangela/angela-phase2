import os
import csv
import io
import json
import uuid
import boto3
from datetime import datetime, timezone

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

REPORTS_BUCKET   = os.environ["REPORTS_BUCKET"]
ORDER_TABLE_NAME = os.environ["ORDER_TABLE_NAME"]
JOBS_TABLE_NAME  = os.environ["JOBS_TABLE_NAME"]


def lambda_handler(event, context):
    job_id = str(uuid.uuid4())
    now    = datetime.now(timezone.utc)
    jobs_table = dynamodb.Table(JOBS_TABLE_NAME)

    # ── 1. Write job record: RUNNING ──────────────────────────────────────────
    jobs_table.put_item(Item={
        "jobId":     job_id,
        "status":    "RUNNING",
        "createdAt": now.isoformat(),
    })

    try:
        # ── 2. Query orders from DynamoDB ─────────────────────────────────────
        order_table = dynamodb.Table(ORDER_TABLE_NAME)
        response    = order_table.scan()          # swap for query/filter as needed
        orders      = response.get("Items", [])

        # Handle pagination
        while "LastEvaluatedKey" in response:
            response = order_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            orders.extend(response.get("Items", []))

        # ── 3. Generate CSV in memory ─────────────────────────────────────────
        fieldnames = ["orderId", "userId", "status", "processedAt"]
        output     = io.StringIO()
        writer     = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(orders)

        # ── 4. Upload to S3 ───────────────────────────────────────────────────
        # s3://reports-bucket/reports/orders/YYYY/MM/DD/orders_report_<timestamp>.csv
        timestamp  = now.strftime("%Y%m%d_%H%M%S")
        s3_key     = (
            f"reports/orders/"
            f"{now.strftime('%Y')}/"
            f"{now.strftime('%m')}/"
            f"{now.strftime('%d')}/"
            f"orders_report_{timestamp}.csv"
        )

        s3.put_object(
            Bucket=REPORTS_BUCKET,
            Key=s3_key,
            Body=output.getvalue(),
            ContentType="text/csv",
        )

        print(f"[OK] Uploaded report → s3://{REPORTS_BUCKET}/{s3_key} ({len(orders)} rows)")

        # ── 5. Update job record: COMPLETED ───────────────────────────────────
        jobs_table.update_item(
            Key={"jobId": job_id},
            UpdateExpression="SET #s = :s, fileKey = :k, completedAt = :t, rowCount = :r",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":s": "COMPLETED",
                ":k": s3_key,
                ":t": datetime.now(timezone.utc).isoformat(),
                ":r": len(orders),
            },
        )

        return {"statusCode": 200, "body": json.dumps({"jobId": job_id, "fileKey": s3_key})}

    except Exception as e:
        print(f"[ERROR] {e}")

        # Update job record: FAILED
        jobs_table.update_item(
            Key={"jobId": job_id},
            UpdateExpression="SET #s = :s, errorMessage = :e",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":s": "FAILED",
                ":e": str(e),
            },
        )
        raise