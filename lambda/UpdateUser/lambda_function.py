import json, os, boto3

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["USER_TABLE_NAME"])

def lambda_handler(event, context):
    try:
        path_params = event.get("pathParameters") or {}
        user_id = path_params.get("userId")
        if not user_id:
            return _resp(400, {"message": "userId is required in path"})
        body = json.loads(event.get("body") or "{}")
        body["userId"] = user_id
        table.put_item(Item=body)
        return _resp(200, {"message": "User updated", "item": body})
    except Exception as e:
        return _resp(500, {"message": "Internal server error", "error": str(e)})

def _resp(code, body):
    return {"statusCode": code, "headers": {"Content-Type": "application/json"}, "body": json.dumps(body)}