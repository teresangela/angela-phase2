import json
import os
import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["USER_TABLE_NAME"])


def _resp(code, body):
    return {
        "statusCode": code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def handler(event, context):
    try:
        method = event.get("httpMethod", "")
        path_params = event.get("pathParameters") or {}
        user_id = path_params.get("userId")

        # POST /users — Create a new user
        if method == "POST":
            body = json.loads(event.get("body") or "{}")
            if "userId" not in body:
                return _resp(400, {"message": "userId is required"})
            try:
                table.put_item(
                    Item=body,
                    ConditionExpression="attribute_not_exists(userId)"
                )
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    return _resp(409, {"message": "User already exists"})
                raise
            return _resp(201, {"message": "User created", "item": body})

        # GET /users — List all users
        # GET /users/{userId} — Get a specific user
        if method == "GET":
            if not user_id:
                res = table.scan()
                return _resp(200, res.get("Items", []))
            res = table.get_item(Key={"userId": user_id})
            item = res.get("Item")
            if not item:
                return _resp(404, {"message": "User not found"})
            return _resp(200, item)

        # PUT /users/{userId} — Update a user (full replace)
        if method == "PUT":
            if not user_id:
                return _resp(400, {"message": "userId is required in path"})
            body = json.loads(event.get("body") or "{}")
            body["userId"] = user_id  # ensure userId is always set
            table.put_item(Item=body)
            return _resp(200, {"message": "User updated", "item": body})

        # DELETE /users/{userId} — Delete a user
        if method == "DELETE":
            if not user_id:
                return _resp(400, {"message": "userId is required in path"})
            table.delete_item(Key={"userId": user_id})
            return _resp(200, {"message": "User deleted", "userId": user_id})

        return _resp(405, {"message": "Method not allowed", "method": method})

    except Exception as e:
        return _resp(500, {"message": "Internal server error", "error": str(e)})