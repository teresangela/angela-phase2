import json
import pytest
import boto3
from moto import mock_aws
from unittest.mock import patch, MagicMock
import os
import sys
import importlib.util
import urllib.error

os.environ["ORDER_TABLE_NAME"]   = "Order"
os.environ["STRIPE_SECRET_ARN"]  = "arn:aws:secretsmanager:ap-southeast-1:123456789012:secret:stripe/secret-key"
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
lambda_handler = _load(f"{BASE}/lambda/StripePayment/lambda_function.py", "stripe_payment_fn").lambda_handler

ORDER_TABLE_NAME  = "Order"
STRIPE_SECRET_ARN = "arn:aws:secretsmanager:ap-southeast-1:123456789012:secret:stripe/secret-key"
FAKE_STRIPE_KEY   = "sk_test_fake123"


def setup_aws():
    dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-1")
    table = dynamodb.create_table(
        TableName=ORDER_TABLE_NAME,
        KeySchema=[{"AttributeName": "orderId", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "orderId", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    secrets = boto3.client("secretsmanager", region_name="ap-southeast-1")
    secrets.create_secret(
        Name="stripe/secret-key",
        SecretString=FAKE_STRIPE_KEY,
    )

    return table


def make_event(order_id=None, amount=None, currency=None):
    body = {}
    if order_id:
        body["orderId"] = order_id
    if amount:
        body["amount"] = amount
    if currency:
        body["currency"] = currency
    return {"body": json.dumps(body)}


def mock_stripe_success(payment_intent_id="pi_test_123"):
    mock_response = MagicMock()
    mock_response.read.return_value = json.dumps({
        "id":     payment_intent_id,
        "status": "requires_payment_method",
        "amount": 100000,
    }).encode()
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = MagicMock(return_value=False)
    return mock_response


@mock_aws
def test_stripe_payment_success():
    """Happy path - order exists, PaymentIntent created, saved to DynamoDB."""
    table = setup_aws()

    table.put_item(Item={
        "orderId": "order-001",
        "userId":  "user-001",
        "status":  "PROCESSED",
    })

    with patch("urllib.request.urlopen", return_value=mock_stripe_success("pi_test_abc123")):
        response = lambda_handler(make_event("order-001", 100000, "idr"), {})

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["paymentIntentId"] == "pi_test_abc123"
    assert body["orderId"] == "order-001"
    assert body["currency"] == "idr"

    updated = table.get_item(Key={"orderId": "order-001"})["Item"]
    assert updated["paymentIntentId"] == "pi_test_abc123"
    assert "paymentStatus" in updated
    assert "paymentUpdatedAt" in updated


@mock_aws
def test_stripe_payment_order_not_found():
    """Order not found - returns 404."""
    setup_aws()

    with patch("urllib.request.urlopen", return_value=mock_stripe_success()):
        response = lambda_handler(make_event("non-existent-order", 100000, "idr"), {})

    assert response["statusCode"] == 404
    body = json.loads(response["body"])
    assert "not found" in body["message"]


@mock_aws
def test_stripe_payment_missing_fields():
    """Missing required fields - returns 400."""
    setup_aws()

    response = lambda_handler(make_event(order_id="order-001"), {})

    assert response["statusCode"] == 400
    body = json.loads(response["body"])
    assert "required" in body["message"]


@mock_aws
def test_stripe_payment_missing_order_id():
    """Missing orderId - returns 400."""
    setup_aws()

    response = lambda_handler(make_event(amount=100000, currency="idr"), {})

    assert response["statusCode"] == 400


@mock_aws
def test_stripe_payment_stripe_api_fails():
    """Stripe API returns error - returns 500."""
    table = setup_aws()

    table.put_item(Item={
        "orderId": "order-001",
        "userId":  "user-001",
        "status":  "PROCESSED",
    })

    http_error = urllib.error.HTTPError(
        url="https://api.stripe.com/v1/payment_intents",
        code=400,
        msg="Bad Request",
        hdrs={},
        fp=MagicMock(read=lambda: json.dumps({
            "error": {
                "type": "invalid_request_error",
                "message": "Invalid currency: xyz",
            }
        }).encode()),
    )

    with patch("urllib.request.urlopen", side_effect=http_error):
        response = lambda_handler(make_event("order-001", 100000, "xyz"), {})

    assert response["statusCode"] == 500
    body = json.loads(response["body"])
    assert "Stripe error" in body["message"]