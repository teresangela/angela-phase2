import os, json, uuid, boto3

sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")

ORDER_QUEUE_URL = os.environ["ORDER_QUEUE_URL"]
ORDER_TABLE_NAME = os.environ["ORDER_TABLE_NAME"]

def _resp(code, body):
    return {
        "statusCode": code,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps(body),
    }

def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")
        user_id = body.get("userId")
        items = body.get("items")

        if not user_id or not items:
            return _resp(400, {"message": "userId and items are required"})

        order_id = uuid.uuid4().hex

        # Kirim ke SQS FIFO
        sqs.send_message(
            QueueUrl=ORDER_QUEUE_URL,
            MessageBody=json.dumps({
                "orderId": order_id,
                "userId": user_id,
                "items": items,
            }),
            MessageGroupId=user_id,           # ordering per user
            MessageDeduplicationId=order_id,  # anti duplikat
        )

        return _resp(202, {
            "message": "Order queued",
            "orderId": order_id,
            "userId": user_id,
        })

    except Exception as e:
        return _resp(500, {"message": "Internal server error", "error": str(e)})