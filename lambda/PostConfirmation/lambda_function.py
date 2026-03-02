import os
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
TABLE = os.environ["USER_TABLE_NAME"]

def lambda_handler(event, context):
    table   = dynamodb.Table(TABLE)
    user_id = event["userName"]
    attrs   = event["request"]["userAttributes"]
    email   = attrs.get("email", "")
    name    = attrs.get("name", "")

    try:
        table.put_item(
            Item={
                "userId":    user_id,
                "email":     email,
                "name":      name,
                "createdAt": datetime.now(timezone.utc).isoformat(),
                "source":    "cognito",
            },
            ConditionExpression="attribute_not_exists(userId)",
        )
        print(f"[PostConfirmation] Created user {user_id} / {email}")

    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            # User already exists — safe to ignore (idempotent)
            print(f"[PostConfirmation] User {user_id} already exists, skipping")
        else:
            # Real error — log it but still return event so Cognito doesn't block the user
            print(f"[PostConfirmation] ERROR saving user {user_id}: {e}")

    return event  # MUST return event back to Cognito