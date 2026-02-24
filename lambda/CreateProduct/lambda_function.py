import json, os, boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["PRODUCT_TABLE_NAME"])

def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")
        if "productId" not in body:
            return _resp(400, {"message": "productId is required"})
        try:
            table.put_item(Item=body, ConditionExpression="attribute_not_exists(productId)")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return _resp(409, {"message": "Product already exists"})
            raise
        return _resp(201, {"message": "Product created", "item": body})
    except Exception as e:
        return _resp(500, {"message": "Internal server error", "error": str(e)})

def _resp(code, body):
    return {"statusCode": code, "headers": {"Content-Type": "application/json"}, "body": json.dumps(body)}