import os
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource("dynamodb")
ORDER_TABLE_NAME = os.environ["ORDER_TABLE_NAME"]

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Authorization,Content-Type",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
}


def lambda_handler(event, context):
    table = dynamodb.Table(ORDER_TABLE_NAME)
    params = event.get("queryStringParameters") or {}

    status    = params.get("status")
    from_date = params.get("from")
    to_date   = params.get("to")

    # Build filter expression
    filters = []
    if status:
        filters.append(Attr("status").eq(status))
    if from_date:
        filters.append(Attr("processedAt").gte(from_date))
    if to_date:
        filters.append(Attr("processedAt").lte(to_date))

    scan_kwargs = {}
    if filters:
        expr = filters[0]
        for f in filters[1:]:
            expr = expr & f
        scan_kwargs["FilterExpression"] = expr

    response = table.scan(**scan_kwargs)
    orders   = response.get("Items", [])

    # Handle pagination
    while "LastEvaluatedKey" in response:
        response = table.scan(
            ExclusiveStartKey=response["LastEvaluatedKey"],
            **scan_kwargs,
        )
        orders.extend(response.get("Items", []))

    return {
        "statusCode": 200,
        "headers": CORS_HEADERS,
        "body": json.dumps({
            "count": len(orders),
            "items": orders,
        }, default=str),
    }