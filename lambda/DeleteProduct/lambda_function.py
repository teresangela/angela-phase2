import json, os, boto3, logging

logger = logging.getLogger()
logger.setLevel(logging.ERROR)

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["PRODUCT_TABLE_NAME"])
cloudwatch = boto3.client("cloudwatch")

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
        if not product_id:
            put_metric("HTTP4xx", context)
            return _resp(400, {"message": "productId is required in path"})
        table.delete_item(Key={"productId": product_id})
        return _resp(200, {"message": "Product deleted", "productId": product_id})
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        put_metric("HTTP5xx", context)
        return _resp(500, {"message": "Internal server error"})

def _resp(code, body):
    return {"statusCode": code, "headers": {"Content-Type": "application/json"}, "body": json.dumps(body)}