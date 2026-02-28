import json
import pytest
import boto3
from moto import mock_aws
from unittest.mock import patch
from datetime import datetime, timezone
import os
import sys
import importlib.util

os.environ["REPORTS_BUCKET"]     = "angel-phase2-reports"
os.environ["ORDER_TABLE_NAME"]   = "Order"
os.environ["JOBS_TABLE_NAME"]    = "ReportJob"
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
lambda_handler = _load(f"{BASE}/lambda/ReportGenerator/lambda_function.py", "report_generator_fn").lambda_handler

REPORTS_BUCKET   = "angel-phase2-reports"
ORDER_TABLE_NAME = "Order"
JOBS_TABLE_NAME  = "ReportJob"


def setup_aws():
    s3 = boto3.client("s3", region_name="ap-southeast-1")
    s3.create_bucket(
        Bucket=REPORTS_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-1"},
    )

    dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-1")

    order_table = dynamodb.create_table(
        TableName=ORDER_TABLE_NAME,
        KeySchema=[{"AttributeName": "orderId", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "orderId", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    jobs_table = dynamodb.create_table(
        TableName=JOBS_TABLE_NAME,
        KeySchema=[{"AttributeName": "jobId", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "jobId", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    return s3, order_table, jobs_table


@mock_aws
def test_report_generator_success():
    """Happy path - generates CSV, uploads to S3, job = COMPLETED."""
    s3, order_table, jobs_table = setup_aws()

    order_table.put_item(Item={
        "orderId": "order-001",
        "userId": "user-001",
        "status": "PROCESSED",
        "processedAt": "2026-02-26T00:00:00+00:00",
    })
    order_table.put_item(Item={
        "orderId": "order-002",
        "userId": "user-002",
        "status": "PROCESSED",
        "processedAt": "2026-02-26T01:00:00+00:00",
    })

    response = lambda_handler({}, {})

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert "jobId" in body
    assert "fileKey" in body
    assert "reports/orders/" in body["fileKey"]

    job = jobs_table.get_item(Key={"jobId": body["jobId"]})["Item"]
    assert job["status"] == "COMPLETED"
    assert job["rowCount"] == 2

    s3_obj = s3.get_object(Bucket=REPORTS_BUCKET, Key=body["fileKey"])
    csv_content = s3_obj["Body"].read().decode()
    assert "orderId" in csv_content
    assert "order-001" in csv_content


@mock_aws
def test_report_generator_empty_orders():
    """Empty orders table - still generates CSV with just headers."""
    s3, order_table, jobs_table = setup_aws()

    response = lambda_handler({}, {})

    assert response["statusCode"] == 200
    body = json.loads(response["body"])

    job = jobs_table.get_item(Key={"jobId": body["jobId"]})["Item"]
    assert job["status"] == "COMPLETED"
    assert job["rowCount"] == 0

    s3_obj = s3.get_object(Bucket=REPORTS_BUCKET, Key=body["fileKey"])
    csv_content = s3_obj["Body"].read().decode()
    assert "orderId" in csv_content