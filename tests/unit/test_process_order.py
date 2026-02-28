import json
import pytest
import boto3
from moto import mock_aws
import sys
import os

os.environ["ORDER_TABLE_NAME"] = "Order"
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-1"

sys.path.insert(0, "lambda/ProcessOrder")
from lambda_function import lambda_handler


@pytest.fixture
def order_table():
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-1")
        table = dynamodb.create_table(
            TableName="Order",
            KeySchema=[{"AttributeName": "orderId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "orderId", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield table


def test_process_order_success(order_table):
    event = {
        "Records": [
            {
                "body": json.dumps({
                    "orderId": "abc123",
                    "userId": "006",
                    "items": [{"productId": "P001", "qty": 2}],
                })
            }
        ]
    }
    lambda_handler(event, {})
    item = order_table.get_item(Key={"orderId": "abc123"}).get("Item")
    assert item is not None
    assert item["status"] == "PROCESSED"
    assert item["userId"] == "006"


def test_process_order_idempotent(order_table):
    order_table.put_item(Item={
        "orderId": "abc123",
        "userId": "006",
        "status": "PROCESSED",
    })
    event = {
        "Records": [
            {
                "body": json.dumps({
                    "orderId": "abc123",
                    "userId": "006",
                    "items": [{"productId": "P001", "qty": 2}],
                })
            }
        ]
    }
    lambda_handler(event, {})
    item = order_table.get_item(Key={"orderId": "abc123"}).get("Item")
    assert item["status"] == "PROCESSED"


def test_process_order_multiple_records(order_table):
    event = {
        "Records": [
            {"body": json.dumps({"orderId": "order1", "userId": "006", "items": []})},
            {"body": json.dumps({"orderId": "order2", "userId": "007", "items": []})},
        ]
    }
    lambda_handler(event, {})
    item1 = order_table.get_item(Key={"orderId": "order1"}).get("Item")
    item2 = order_table.get_item(Key={"orderId": "order2"}).get("Item")
    assert item1["status"] == "PROCESSED"
    assert item2["status"] == "PROCESSED"