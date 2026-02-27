import os, json, boto3
from datetime import datetime, timezone

ORDER_TABLE_NAME = os.environ["ORDER_TABLE_NAME"]

def lambda_handler(event, context):
    dynamodb = boto3.resource("dynamodb", region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1"))
    table = dynamodb.Table(ORDER_TABLE_NAME)

    for record in event["Records"]:
        body = json.loads(record["body"])
        order_id = body["orderId"]
        user_id = body["userId"]
        items = body["items"]

        existing = table.get_item(Key={"orderId": order_id}).get("Item")
        if existing and existing.get("status") == "PROCESSED":
            print(f"[SKIP] orderId {order_id} already processed")
            continue

        table.put_item(Item={
            "orderId": order_id,
            "userId": user_id,
            "items": items,
            "status": "PROCESSED",
            "processedAt": datetime.now(timezone.utc).isoformat(),
        })

        print(f"[OK] Processed orderId={order_id} userId={user_id}")
