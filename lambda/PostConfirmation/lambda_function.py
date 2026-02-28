import os
import boto3
from datetime import datetime, timezone

dynamodb = boto3.resource("dynamodb")
TABLE = os.environ["USER_TABLE_NAME"]

def lambda_handler(event, context):
    table      = dynamodb.Table(TABLE)
    user_id    = event["userName"]
    attrs      = event["request"]["userAttributes"]
    email      = attrs.get("email", "")
    name       = attrs.get("name", "")

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
    return event  # MUST return event back to Cognito