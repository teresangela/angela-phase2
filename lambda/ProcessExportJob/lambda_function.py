import os
import csv
import io
import json
import boto3
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Attr
from botocore.config import Config


def _region():
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-southeast-1"


def _build_filter_expression(filters):
    filter_exprs = []
    if filters.get("status"):
        filter_exprs.append(Attr("status").eq(filters["status"]))
    if filters.get("from"):
        filter_exprs.append(Attr("processedAt").gte(filters["from"]))
    if filters.get("to"):
        filter_exprs.append(Attr("processedAt").lte(filters["to"]))

    if not filter_exprs:
        return {}

    expr = filter_exprs[0]
    for f in filter_exprs[1:]:
        expr = expr & f
    return {"FilterExpression": expr}


def _scan_all_orders(order_table, scan_kwargs):
    response = order_table.scan(**scan_kwargs)
    orders = response.get("Items", [])
    while "LastEvaluatedKey" in response:
        response = order_table.scan(
            ExclusiveStartKey=response["LastEvaluatedKey"],
            **scan_kwargs,
        )
        orders.extend(response.get("Items", []))
    return orders


def _generate_csv(orders):
    fieldnames = ["orderId", "userId", "status", "processedAt"]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(orders)
    return output


def _build_s3_key(job_id):
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    s3_key = (
        f"exports/orders/"
        f"{now.strftime('%Y')}/"
        f"{now.strftime('%m')}/"
        f"{now.strftime('%d')}/"
        f"export_{job_id}_{timestamp}.csv"
    )
    return now, s3_key


def _process_record(dynamodb, s3, account_id, record, export_job_table, order_table_name, reports_bucket):
    body    = json.loads(record["body"])
    job_id  = body["jobId"]
    filters = body.get("filters", {})

    jobs_table = dynamodb.Table(export_job_table)

    jobs_table.update_item(
        Key={"jobId": job_id},
        UpdateExpression="SET #s = :s",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": "RUNNING"},
    )

    try:
        order_table = dynamodb.Table(order_table_name)
        scan_kwargs = _build_filter_expression(filters)
        orders = _scan_all_orders(order_table, scan_kwargs)

        output = _generate_csv(orders)
        now, s3_key = _build_s3_key(job_id)

        s3.put_object(
            Bucket=reports_bucket,
            Key=s3_key,
            Body=output.getvalue(),
            ContentType="text/csv",
            ExpectedBucketOwner=account_id,  # fix S7608
        )

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


_boto_config = Config(connect_timeout=5, read_timeout=10)  # fix S7618


def lambda_handler(event, context):
  dynamodb = boto3.resource("dynamodb", region_name=_region(), config=_boto_config)  # NOSONAR
  s3 = boto3.client("s3", region_name=_region(), config=_boto_config)  # NOSONAR
  account_id = boto3.client("sts", region_name=_region(), config=_boto_config).get_caller_identity()["Account"]  # NOSONAR

    export_job_table = os.environ["EXPORT_JOB_TABLE"]
    order_table_name = os.environ["ORDER_TABLE_NAME"]
    reports_bucket   = os.environ["REPORTS_BUCKET"]

    for record in event["Records"]:
        _process_record(dynamodb, s3, account_id, record, export_job_table, order_table_name, reports_bucket)