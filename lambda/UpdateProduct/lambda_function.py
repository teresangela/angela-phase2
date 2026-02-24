import json, os, boto3
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["PRODUCT_TABLE_NAME"])

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal): return float(obj)
        return super().default(obj)

def lambda_handler(event, context):
    try:
        path_params = event.get("pathParameters") or {}
        product_id = path_params.get("productId")
        if not product_id:
            return _resp(400, {"message": "productId is required in path"})
        body = json.loads(event.get("body") or "{}")
        body["productId"] = product_id
        table.put_item(Item=body)
        return _resp(200, {"message": "Product updated", "item": body})
    except Exception as e:
        return _resp(500, {"message": "Internal server error", "error": str(e)})

def _resp(code, body):
    return {"statusCode": code, "headers": {"Content-Type": "application/json"}, "body": json.dumps(body, cls=DecimalEncoder)}