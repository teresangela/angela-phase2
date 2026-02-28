import json
import pytest
import boto3
from moto import mock_aws
from unittest.mock import patch
import os
import sys
import importlib.util

os.environ["EXPORT_JOB_TABLE"]   = "ExportJob"
os.environ["ORDER_TABLE_NAME"]   = "Order"
os.environ["REPORTS_BUCKET"]     = "angel-phase2-reports"
os.environ["EXPORT_QUEUE_URL"]   = "https://sqs.ap-southeast-1.amazonaws.com/123456789012/ExportJobQueue.fifo"
os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-1"
os.environ["AWS_ACCESS_KEY_ID"]  = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m
    spec.loader.exec_module(m)
    return m


BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
create_export_job_handler  = _load(f"{BASE}/lambda/CreateExportJob/lambda_function.py",  "create_export_job_fn").lambda_handler
get_export_job_handler     = _load(f"{BASE}/lambda/GetExportJob/lambda_function.py",     "get_export_job_fn").lambda_handler
process_export_job_handler = _load(f"{BASE}/lambda/ProcessExportJob/lambda_function.py", "process_export_job_fn").lambda_handler

EXPORT_JOB_TABLE = "ExportJob"
ORDER_TABLE_NAME = "Order"
REPORTS_BUCKET   = "angel-phase2-reports"


def setup_aws():
    dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-1")

    export_job_table = dynamodb.create_table(
        TableName=EXPORT_JOB_TABLE,
        KeySchema=[{"AttributeName": "jobId", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "jobId", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    order_table = dynamodb.create_table(
        TableName=ORDER_TABLE_NAME,
        KeySchema=[{"AttributeName": "orderId", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "orderId", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    s3 = boto3.client("s3", region_name="ap-southeast-1")
    s3.create_bucket(
        Bucket=REPORTS_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-1"},
    )

    sqs = boto3.client("sqs", region_name="ap-southeast-1")
    queue = sqs.create_queue(
        QueueName="ExportJobQueue.fifo",
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "false",
        },
    )

    return export_job_table, order_table, s3, queue["QueueUrl"]


# ── CreateExportJob Tests ─────────────────────────────────────────────────────

@mock_aws
def test_create_export_job_success():
    """Happy path - writes PENDING to DynamoDB, sends to SQS."""
    export_job_table, _, _, queue_url = setup_aws()

    event = {
        "body": json.dumps({
            "requestedBy": "angela",
            "status":      "PROCESSED",
            "from":        "2026-01-01T00:00:00",
            "to":          "2026-12-31T23:59:59",
        })
    }

    with patch.dict(os.environ, {"EXPORT_QUEUE_URL": queue_url}):
        response = create_export_job_handler(event, {})

    assert response["statusCode"] == 202
    body = json.loads(response["body"])
    assert "jobId" in body
    assert body["status"] == "PENDING"

    job = export_job_table.get_item(Key={"jobId": body["jobId"]})["Item"]
    assert job["status"] == "PENDING"
    assert job["requestedBy"] == "angela"


@mock_aws
def test_create_export_job_no_filters():
    """No filters provided - still creates job."""
    _, _, _, queue_url = setup_aws()

    event = {"body": json.dumps({"requestedBy": "angela"})}

    with patch.dict(os.environ, {"EXPORT_QUEUE_URL": queue_url}):
        response = create_export_job_handler(event, {})

    assert response["statusCode"] == 202


# ── GetExportJob Tests ────────────────────────────────────────────────────────

@mock_aws
def test_get_export_job_pending():
    """Job is PENDING - returns status without downloadUrl."""
    export_job_table, _, _, _ = setup_aws()

    export_job_table.put_item(Item={
        "jobId":     "job-001",
        "status":    "PENDING",
        "createdAt": "2026-02-26T00:00:00+00:00",
    })

    event = {"pathParameters": {"jobId": "job-001"}}

    response = get_export_job_handler(event, {})

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["status"] == "PENDING"
    assert "downloadUrl" not in body


@mock_aws
def test_get_export_job_done_has_presigned_url():
    """Job is DONE - returns downloadUrl presigned URL."""
    export_job_table, _, s3, _ = setup_aws()

    s3_key = "exports/orders/2026/02/26/export_job-001.csv"
    s3.put_object(Bucket=REPORTS_BUCKET, Key=s3_key, Body="orderId,userId\n")

    export_job_table.put_item(Item={
        "jobId":       "job-001",
        "status":      "DONE",
        "s3Key":       s3_key,
        "createdAt":   "2026-02-26T00:00:00+00:00",
        "completedAt": "2026-02-26T00:01:00+00:00",
    })

    event = {"pathParameters": {"jobId": "job-001"}}

    response = get_export_job_handler(event, {})

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["status"] == "DONE"
    assert "downloadUrl" in body
    assert "expiresAt" in body


@mock_aws
def test_get_export_job_not_found():
    """Job not found - returns 404."""
    setup_aws()

    event = {"pathParameters": {"jobId": "non-existent-job"}}

    response = get_export_job_handler(event, {})

    assert response["statusCode"] == 404


# ── ProcessExportJob Tests ────────────────────────────────────────────────────

@mock_aws
def test_process_export_job_success():
    """Happy path - generates CSV, uploads to S3, updates job to DONE."""
    export_job_table, order_table, s3, _ = setup_aws()

    order_table.put_item(Item={
        "orderId":     "order-001",
        "userId":      "user-001",
        "status":      "PROCESSED",
        "processedAt": "2026-02-26T00:00:00+00:00",
    })

    export_job_table.put_item(Item={
        "jobId":  "job-001",
        "status": "PENDING",
    })

    sqs_event = {
        "Records": [{
            "body": json.dumps({
                "jobId":   "job-001",
                "filters": {"status": "PROCESSED"},
            })
        }]
    }

    process_export_job_handler(sqs_event, {})

    job = export_job_table.get_item(Key={"jobId": "job-001"})["Item"]
    assert job["status"] == "DONE"
    assert job["rowCount"] == 1
    assert "s3Key" in job

    s3_obj = s3.get_object(Bucket=REPORTS_BUCKET, Key=job["s3Key"])
    csv_content = s3_obj["Body"].read().decode()
    assert "order-001" in csv_content


@mock_aws
def test_process_export_job_failed_on_s3_error():
    """S3 upload fails - job status = FAILED."""
    export_job_table, order_table, _, _ = setup_aws()

    order_table.put_item(Item={"orderId": "order-001", "userId": "u1", "status": "PROCESSED"})
    export_job_table.put_item(Item={"jobId": "job-001", "status": "PENDING"})

    sqs_event = {
        "Records": [{
            "body": json.dumps({"jobId": "job-001", "filters": {}})
        }]
    }

    with patch.dict(os.environ, {"REPORTS_BUCKET": "non-existent-bucket"}):
        with pytest.raises(Exception):
            process_export_job_handler(sqs_event, {})

    job = export_job_table.get_item(Key={"jobId": "job-001"})["Item"]
    assert job["status"] == "FAILED"
    assert "errorMessage" in job