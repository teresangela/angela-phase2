import json, os, boto3

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["PRODUCT_TABLE_NAME"])

def lambda_handler(event, context):
    try:
        path_params = event.get("pathParameters") or {}
        product_id = path_params.get("productId")
        if not product_id:
            return _resp(400, {"message": "productId is required in path"})
        table.delete_item(Key={"productId": product_id})
        return _resp(200, {"message": "Product deleted", "productId": product_id})
    except Exception as e:
        return _resp(500, {"message": "Internal server error", "error": str(e)})

def _resp(code, body):
    return {"statusCode": code, "headers": {"Content-Type": "application/json"}, "body": json.dumps(body)}