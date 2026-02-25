import json, os, boto3, logging
from decimal import Decimal
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.ERROR)

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["PRODUCT_TABLE_NAME"])
cloudwatch = boto3.client("cloudwatch")

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal): return float(obj)
        return super().default(obj)

def put_metric(name, context, value=1):
    try:
        cloudwatch.put_metric_data(
            Namespace="HyPhase2App",
            MetricData=[{
                "MetricName": name,
                "Dimensions": [{"Name": "FunctionName", "Value": context.function_name}],
                "Unit": "Count",
                "Value": value
            }]
        )
    except Exception as e:
        logger.error(f"Failed to put metric {name}: {e}")

def lambda_handler(event, context):
    try:
        path_params = event.get("pathParameters") or {}
        product_id = path_params.get("productId")
        query_params = event.get("queryStringParameters") or {}
        category = query_params.get("category")

        if category:
            res = table.query(IndexName="category-index", KeyConditionExpression=Key("category").eq(category))
            items = res.get("Items", [])
            if not items:
                put_metric("HTTP4xx", context)
                return _resp(404, {"message": "No products found in this category"})
            return _resp(200, items)

        if not product_id:
            res = table.scan()
            return _resp(200, res.get("Items", []))

        res = table.get_item(Key={"productId": product_id})
        item = res.get("Item")
        if not item:
            put_metric("HTTP4xx", context)
            return _resp(404, {"message": "Product not found"})
        return _resp(200, item)

    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        put_metric("HTTP5xx", context)
        return _resp(500, {"message": "Internal server error"})

def _resp(code, body):
    return {"statusCode": code, "headers": {"Content-Type": "application/json"}, "body": json.dumps(body, cls=DecimalEncoder)}