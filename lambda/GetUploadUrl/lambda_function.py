import os, json, uuid
import boto3
from botocore.exceptions import ClientError


USER_TABLE_NAME = os.environ["USER_TABLE_NAME"]
UPLOAD_BUCKET = os.environ["UPLOAD_BUCKET"]

ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/png", "application/pdf"}

def _resp(code, body):
    return {
        "statusCode": code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }

def lambda_handler(event, context):
    dynamodb = boto3.resource("dynamodb")
    s3 = boto3.client("s3")
    try:
        user_id = (event.get("pathParameters") or {}).get("userId")
        if not user_id:
            return _resp(400, {"message": "userId is required in path"})

        body = json.loads(event.get("body") or "{}")
        content_type = body.get("contentType", "application/octet-stream")

        if content_type not in ALLOWED_CONTENT_TYPES:
            return _resp(400, {"message": f"Invalid contentType. Allowed: {sorted(ALLOWED_CONTENT_TYPES)}"})

        ext_map = {"image/jpeg": "jpg", "image/png": "png", "application/pdf": "pdf"}
        ext = ext_map[content_type]

        object_key = f"users/{user_id}/{uuid.uuid4().hex}.{ext}"

        upload_url = s3.generate_presigned_url(
            ClientMethod="put_object",
            Params={"Bucket": UPLOAD_BUCKET, "Key": object_key, "ContentType": content_type},
            ExpiresIn=300,
        )

        # simpan key ke DynamoDB record user
        table = dynamodb.Table(USER_TABLE_NAME)
        try:
            table.update_item(
                Key={"userId": user_id},
                UpdateExpression="SET avatarKey = :k",
                ExpressionAttributeValues={":k": object_key},
                ConditionExpression="attribute_exists(userId)",  # biar gak bikin user baru
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return _resp(404, {"message": "User not found (cannot store key)"})
            raise

        return _resp(200, {
            "uploadUrl": upload_url,
            "bucket": UPLOAD_BUCKET,
            "key": object_key,
            "expiresInSeconds": 300,
            "contentType": content_type,
        })

    except Exception as e:
        return _resp(500, {"message": "Internal server error", "error": str(e)})