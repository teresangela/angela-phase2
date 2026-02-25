import json, os, boto3, logging
from botocore.exceptions import ClientError

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
        body = json.loads(event.get("body") or "{}")
        if "productId" not in body:
            put_metric("HTTP4xx", context)
            return _resp(400, {"message": "productId is required"})
        try:
            table.put_item(Item=body, ConditionExpression="attribute_not_exists(productId)")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                put_metric("HTTP4xx", context)
                return _resp(409, {"message": "Product already exists"})
            logger.error(f"DynamoDB ClientError: {str(e)}")
            put_metric("HTTP5xx", context)
            return _resp(500, {"message": "Internal server error"})
        return _resp(201, {"message": "Product created", "item": body})
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        put_metric("HTTP5xx", context)
        return _resp(500, {"message": "Internal server error"})

def _resp(code, body):
    return {"statusCode": code, "headers": {"Content-Type": "application/json"}, "body": json.dumps(body)}