import json, os, boto3, logging
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.ERROR)

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["USER_TABLE_NAME"])
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
        user_id = path_params.get("userId")
        if not user_id:
            put_metric("HTTP4xx", context)
            return _resp(400, {"message": "userId is required in path"})

        body = json.loads(event.get("body") or "{}")
        body.pop("userId", None)  # prevent overwriting partition key

        if not body:
            put_metric("HTTP4xx", context)
            return _resp(400, {"message": "No fields to update"})

        # Dynamically build UpdateExpression from request body
        update_expr = "SET " + ", ".join(f"#k{i} = :v{i}" for i in range(len(body)))
        expr_names  = {f"#k{i}": k for i, k in enumerate(body.keys())}
        expr_values = {f":v{i}": v for i, v in enumerate(body.values())}

        table.update_item(
            Key={"userId": user_id},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values,
            ConditionExpression="attribute_exists(userId)",  # 404 if user doesn't exist
        )

        return _resp(200, {"message": "User updated", "userId": user_id})

    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            put_metric("HTTP4xx", context)
            return _resp(404, {"message": "User not found"})
        logger.error(f"DynamoDB error: {str(e)}")
        put_metric("HTTP5xx", context)
        return _resp(500, {"message": "Internal server error"})
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        put_metric("HTTP5xx", context)
        return _resp(500, {"message": "Internal server error"})

def _resp(code, body):
    return {"statusCode": code, "headers": {"Content-Type": "application/json"}, "body": json.dumps(body)}