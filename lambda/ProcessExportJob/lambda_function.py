import os
import csv
import io
import json
import boto3
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Attr


def _region():
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-southeast-1"


def lambda_handler(event, context):
    # IMPORTANT: create clients/resources inside handler so moto can mock them
    dynamodb = boto3.resource("dynamodb", region_name=_region())
    s3 = boto3.client("s3", region_name=_region())

    export_job_table = os.environ["EXPORT_JOB_TABLE"]
    order_table_name = os.environ["ORDER_TABLE_NAME"]
    reports_bucket   = os.environ["REPORTS_BUCKET"]

    for record in event["Records"]:
        body    = json.loads(record["body"])
        job_id  = body["jobId"]
        filters = body.get("filters", {})

        jobs_table = dynamodb.Table(export_job_table)

        # Update job: RUNNING
        jobs_table.update_item(
            Key={"jobId": job_id},
            UpdateExpression="SET #s = :s",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": "RUNNING"},
        )

        try:
            # Query orders with filters
            order_table = dynamodb.Table(order_table_name)
            scan_kwargs = {}
            filter_exprs = []

            if filters.get("status"):
                filter_exprs.append(Attr("status").eq(filters["status"]))
            if filters.get("from"):
                filter_exprs.append(Attr("processedAt").gte(filters["from"]))
            if filters.get("to"):
                filter_exprs.append(Attr("processedAt").lte(filters["to"]))

            if filter_exprs:
                expr = filter_exprs[0]
                for f in filter_exprs[1:]:
                    expr = expr & f
                scan_kwargs["FilterExpression"] = expr

            response = order_table.scan(**scan_kwargs)
            orders = response.get("Items", [])

            while "LastEvaluatedKey" in response:
                response = order_table.scan(
                    ExclusiveStartKey=response["LastEvaluatedKey"],
                    **scan_kwargs,
                )
                orders.extend(response.get("Items", []))

            # Generate CSV
            fieldnames = ["orderId", "userId", "status", "processedAt"]
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(orders)

            # Upload to S3
            now = datetime.now(timezone.utc)
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            s3_key = (
                f"exports/orders/"
                f"{now.strftime('%Y')}/"
                f"{now.strftime('%m')}/"
                f"{now.strftime('%d')}/"
                f"export_{job_id}_{timestamp}.csv"
            )

            s3.put_object(
                Bucket=reports_bucket,
                Key=s3_key,
                Body=output.getvalue(),
                ContentType="text/csv",
            )

            # Update job: DONE
            jobs_table.update_item(
                Key={"jobId": job_id},
                UpdateExpression="SET #s = :s, s3Key = :k, completedAt = :t, rowCount = :r",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":s": "DONE",
                    ":k": s3_key,
                    ":t": now.isoformat(),
                    ":r": len(orders),
                },
            )

        except Exception as e:
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