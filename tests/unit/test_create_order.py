import json
import pytest
import boto3
from moto import mock_aws
from unittest.mock import patch
import os

# Set env vars sebelum import lambda
os.environ["ORDER_QUEUE_URL"] = "https://sqs.ap-southeast-1.amazonaws.com/123456789012/OrderQueue.fifo"
os.environ["ORDER_TABLE_NAME"] = "Order"

import sys
sys.path.insert(0, "lambda/CreateOrder")
from lambda_function import lambda_handler


@pytest.fixture
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-1"


@pytest.fixture
def sqs_queue(aws_credentials):
    with mock_aws():
        sqs = boto3.client("sqs", region_name="ap-southeast-1")
        queue = sqs.create_queue(
            QueueName="OrderQueue.fifo",
            Attributes={
                "FifoQueue": "true",
                "ContentBasedDeduplication": "false",
            },
        )
        os.environ["ORDER_QUEUE_URL"] = queue["QueueUrl"]
        yield sqs, queue["QueueUrl"]


@mock_aws
def test_create_order_success(sqs_queue):
    """Happy path - order berhasil masuk queue"""
    sqs, queue_url = sqs_queue

    event = {
        "body": json.dumps({
            "userId": "006",
            "items": [{"productId": "P001", "qty": 2}]
        })
    }

    response = lambda_handler(event, {})
    body = json.loads(response["body"])

    assert response["statusCode"] == 202
    assert body["message"] == "Order queued"
    assert body["userId"] == "006"
    assert "orderId" in body


@mock_aws
def test_create_order_missing_userId(sqs_queue):
    """Validasi - userId tidak ada"""
    event = {
        "body": json.dumps({
            "items": [{"productId": "P001", "qty": 1}]
        })
    }

    response = lambda_handler(event, {})
    body = json.loads(response["body"])

    assert response["statusCode"] == 400
    assert "required" in body["message"]


@mock_aws
def test_create_order_missing_items(sqs_queue):
    """Validasi - items tidak ada"""
    event = {
        "body": json.dumps({
            "userId": "006"
        })
    }

    response = lambda_handler(event, {})
    body = json.loads(response["body"])

    assert response["statusCode"] == 400
    assert "required" in body["message"]


@mock_aws
def test_create_order_empty_body(sqs_queue):
    """Edge case - body kosong"""
    event = {"body": None}

    response = lambda_handler(event, {})
    body = json.loads(response["body"])

    assert response["statusCode"] == 400