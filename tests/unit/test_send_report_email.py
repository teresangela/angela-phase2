import json
import pytest
import boto3
from moto import mock_aws
from unittest.mock import patch
import os
import sys
import importlib.util

os.environ["SENDER_EMAIL"]       = "teresangelaa.rosa@gmail.com"
os.environ["RECIPIENT_EMAIL"]    = "teresangelaa.rosa@gmail.com"
os.environ["REPORTS_BUCKET"]     = "angel-phase2-reports"
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
lambda_handler = _load(f"{BASE}/lambda/SendReportEmail/lambda_function.py", "send_report_email_fn").lambda_handler

SENDER_EMAIL   = "teresangelaa.rosa@gmail.com"
RECIPIENT_EMAIL = "teresangelaa.rosa@gmail.com"
REPORTS_BUCKET  = "angel-phase2-reports"


def make_s3_event(bucket, key, size=606):
    return {
        "Records": [{
            "s3": {
                "bucket": {"name": bucket},
                "object": {"key": key, "size": size},
            }
        }]
    }


def setup_ses():
    ses = boto3.client("ses", region_name="ap-southeast-1")
    ses.verify_email_identity(EmailAddress=SENDER_EMAIL)
    return ses


@mock_aws
def test_send_report_email_success():
    """Happy path - S3 event triggers, email sent via SES."""
    setup_ses()

    event = make_s3_event(
        REPORTS_BUCKET,
        "reports/orders/2026/02/26/orders_report_20260226_080000.csv",
    )

    lambda_handler(event, {})

    ses = boto3.client("ses", region_name="ap-southeast-1")
    quota = ses.get_send_quota()
    assert quota["SentLast24Hours"] >= 1


@mock_aws
def test_send_report_email_multiple_records():
    """Multiple S3 records in one event - sends email for each."""
    setup_ses()

    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": REPORTS_BUCKET},
                    "object": {
                        "key": f"reports/orders/2026/02/26/orders_report_2026022608000{i}.csv",
                        "size": 606,
                    },
                }
            }
            for i in range(3)
        ]
    }

    lambda_handler(event, {})

    ses = boto3.client("ses", region_name="ap-southeast-1")
    quota = ses.get_send_quota()
    assert quota["SentLast24Hours"] >= 3