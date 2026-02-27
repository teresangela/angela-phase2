import os, json
import boto3


USER_TABLE_NAME = os.environ["USER_TABLE_NAME"]
UPLOAD_BUCKET = os.environ["UPLOAD_BUCKET"]

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

        table = dynamodb.Table(USER_TABLE_NAME)
        item = table.get_item(Key={"userId": user_id}).get("Item")
        if not item:
            return _resp(404, {"message": "User not found"})

        key = item.get("avatarKey")
        if not key:
            return _resp(404, {"message": "No file key stored for this user"})

        download_url = s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": UPLOAD_BUCKET, "Key": key},
            ExpiresIn=300,
        )

        return _resp(200, {
            "downloadUrl": download_url,
            "bucket": UPLOAD_BUCKET,
            "key": key,
            "expiresInSeconds": 300,
        })

    except Exception as e:
        return _resp(500, {"message": "Internal server error", "error": str(e)})