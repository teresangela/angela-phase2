import json
import os
import boto3
from decimal import Decimal
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["PRODUCT_TABLE_NAME"])


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def _resp(code, body):
    return {
        "statusCode": code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, cls=DecimalEncoder),
    }


def handler(event, context):
    try:
        method = event.get("httpMethod", "")
        path_params = event.get("pathParameters") or {}
        product_id = path_params.get("productId")

        # POST /products — Create a new product
        if method == "POST":
            body = json.loads(event.get("body") or "{}")
            if "productId" not in body:
                return _resp(400, {"message": "productId is required"})
            try:
                table.put_item(
                    Item=body,
                    ConditionExpression="attribute_not_exists(productId)"
                )
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    return _resp(409, {"message": "Product already exists"})
                raise
            return _resp(201, {"message": "Product created", "item": body})

        # GET /products — List all products
        # GET /products/{productId} — Get a specific product
        if method == "GET":
            if not product_id:
                res = table.scan()
                return _resp(200, res.get("Items", []))
            res = table.get_item(Key={"productId": product_id})
            item = res.get("Item")
            if not item:
                return _resp(404, {"message": "Product not found"})
            return _resp(200, item)

        # PUT /products/{productId} — Update a product (full replace)
        if method == "PUT":
            if not product_id:
                return _resp(400, {"message": "productId is required in path"})
            body = json.loads(event.get("body") or "{}")
            body["productId"] = product_id  # ensure productId is always set
            table.put_item(Item=body)
            return _resp(200, {"message": "Product updated", "item": body})

        # DELETE /products/{productId} — Delete a product
        if method == "DELETE":
            if not product_id:
                return _resp(400, {"message": "productId is required in path"})
            table.delete_item(Key={"productId": product_id})
            return _resp(200, {"message": "Product deleted", "productId": product_id})

        return _resp(405, {"message": "Method not allowed", "method": method})

    except Exception as e:
        return _resp(500, {"message": "Internal server error", "error": str(e)})