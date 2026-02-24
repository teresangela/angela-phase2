import json, os, boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["USER_TABLE_NAME"])

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal): return float(obj)
        return super().default(obj)

def lambda_handler(event, context):
    try:
        path_params = event.get("pathParameters") or {}
        user_id = path_params.get("userId")
        query_params = event.get("queryStringParameters") or {}
        email = query_params.get("email")

        # Query by email using GSI
        if email:
            res = table.query(
                IndexName="email-index",
                KeyConditionExpression=Key("email").eq(email)
            )
            items = res.get("Items", [])
            if not items:
                return _resp(404, {"message": "User not found"})
            return _resp(200, items[0])

        # Get all users
        if not user_id:
            res = table.scan()
            return _resp(200, res.get("Items", []))

        # Get by userId
        res = table.get_item(Key={"userId": user_id})
        item = res.get("Item")
        if not item:
            return _resp(404, {"message": "User not found"})
        return _resp(200, item)

    except Exception as e:
        return _resp(500, {"message": "Internal server error", "error": str(e)})

def _resp(code, body):
    return {"statusCode": code, "headers": {"Content-Type": "application/json"}, "body": json.dumps(body, cls=DecimalEncoder)}