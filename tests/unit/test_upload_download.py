import json
import boto3
from moto import mock_aws
import os
import importlib.util

os.environ["USER_TABLE_NAME"] = "User"
os.environ["UPLOAD_BUCKET"] = "angel-phase2-uploads"
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-1"

def load_handler(module_name, file_path, handler_name="lambda_handler"):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, handler_name)

upload_handler = load_handler(
    "get_upload_url",
    os.path.join("lambda", "GetUploadUrl", "lambda_function.py"),
)

download_handler = load_handler(
    "get_download_url",
    os.path.join("lambda", "GetDownloadUrl", "lambda_function.py"),
)


def setup_aws():
    """Helper: buat S3 bucket + DynamoDB User table"""
    s3 = boto3.client("s3", region_name="ap-southeast-1")
    s3.create_bucket(
        Bucket="angel-phase2-uploads",
        CreateBucketConfiguration={"LocationConstraint": "ap-southeast-1"},
    )

    dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-1")
    table = dynamodb.create_table(
        TableName="User",
        KeySchema=[{"AttributeName": "userId", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "userId", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    return s3, table


def test_get_upload_url_success():
    """Happy path - dapat presigned URL untuk upload"""
    with mock_aws():
        _, table = setup_aws()

        # ✅ karena pilih opsi A: user harus ada dulu
        table.put_item(Item={"userId": "006"})

        event = {
            "pathParameters": {"userId": "006"},
            "body": json.dumps({"contentType": "image/png"}),
        }

        response = upload_handler(event, {})
        body = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert "uploadUrl" in body
        assert "key" in body
        assert body["contentType"] == "image/png"
        assert "users/006/" in body["key"]
        

def test_get_upload_url_invalid_content_type():
    """Validasi - contentType tidak allowed"""
    with mock_aws():
        setup_aws()

        event = {
            "pathParameters": {"userId": "006"},
            "body": json.dumps({"contentType": "video/mp4"}),
        }

        response = upload_handler(event, {})
        body = json.loads(response["body"])

        assert response["statusCode"] == 400
        assert "Invalid contentType" in body["message"]


def test_get_upload_url_missing_userId():
    """Validasi - userId tidak ada di path"""
    with mock_aws():
        setup_aws()

        event = {
            "pathParameters": {},
            "body": json.dumps({"contentType": "image/png"}),
        }

        response = upload_handler(event, {})
        body = json.loads(response["body"])

        assert response["statusCode"] == 400
        assert "userId is required" in body["message"]


def test_get_download_url_success():
    """Happy path - dapat presigned URL untuk download"""
    with mock_aws():
        _, table = setup_aws()

        table.put_item(Item={
            "userId": "006",
            "avatarKey": "users/006/abc123.png",
        })

        event = {"pathParameters": {"userId": "006"}}

        response = download_handler(event, {})
        body = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert "downloadUrl" in body
        assert body["key"] == "users/006/abc123.png"


def test_get_download_url_user_not_found():
    """User tidak exist di DynamoDB"""
    with mock_aws():
        setup_aws()

        event = {"pathParameters": {"userId": "999"}}

        response = download_handler(event, {})
        body = json.loads(response["body"])

        assert response["statusCode"] == 404
        assert "not found" in body["message"].lower()


def test_get_download_url_no_avatar():
    """User exist tapi belum upload foto"""
    with mock_aws():
        _, table = setup_aws()

        table.put_item(Item={"userId": "006", "name": "Angela"})

        event = {"pathParameters": {"userId": "006"}}

        response = download_handler(event, {})
        body = json.loads(response["body"])

        assert response["statusCode"] == 404
        assert "No file key" in body["message"]