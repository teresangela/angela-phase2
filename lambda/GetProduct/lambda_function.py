import json, os, boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key

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
        query_params = event.get("queryStringParameters") or {}
        category = query_params.get("category")

        # Query by category using GSI
        if category:
            res = table.query(
                IndexName="category-index",
                KeyConditionExpression=Key("category").eq(category)
            )
            items = res.get("Items", [])
            if not items:
                return _resp(404, {"message": "No products found in this category"})
            return _resp(200, items)

        # Get all products
        if not product_id:
            res = table.scan()
            return _resp(200, res.get("Items", []))

        # Get by productId
        res = table.get_item(Key={"productId": product_id})
        item = res.get("Item")
        if not item:
            return _resp(404, {"message": "Product not found"})
        return _resp(200, item)

    except Exception as e:
        return _resp(500, {"message": "Internal server error", "error": str(e)})

def _resp(code, body):
    return {"statusCode": code, "headers": {"Content-Type": "application/json"}, "body": json.dumps(body, cls=DecimalEncoder)}