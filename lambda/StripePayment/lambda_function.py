import os
import json
import boto3
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone

dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")
)
secrets  = boto3.client("secretsmanager", region_name="ap-southeast-1")

ORDER_TABLE_NAME = os.environ["ORDER_TABLE_NAME"]
STRIPE_SECRET_ARN = os.environ["STRIPE_SECRET_ARN"]


def get_stripe_key():
    response = secrets.get_secret_value(SecretId=STRIPE_SECRET_ARN)
    return response["SecretString"]


def create_payment_intent(stripe_key, amount, currency, order_id, user_id):
    """
    Call Stripe API ONCE.
    Step Functions will handle retry.
    """
    url  = "https://api.stripe.com/v1/payment_intents"
    data = urllib.parse.urlencode({
        "amount": amount,
        "currency": currency,
        "metadata[orderId]": order_id,
        "metadata[userId]": user_id,
    }).encode()

    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Authorization", f"Bearer {stripe_key}")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode())

    except urllib.error.HTTPError as e:
        error_body = json.loads(e.read().decode())

        # 🔥 Important: Let Step Function retry this
        if e.code == 429:
            raise Exception("RateLimited")

        raise Exception(
            f"StripeHttpError_{e.code}: {error_body.get('error', {}).get('message')}"
        )


def lambda_handler(event, context):
   # Support both API Gateway (has "body") and Step Functions (raw JSON)
    if "body" in event:
        body = json.loads(event.get("body") or "{}")
    else:
        body = event  # ← Step Functions passes payload directly

    order_id = body.get("orderId")
    amount   = body.get("amount")
    currency = body.get("currency")

    if not order_id or not amount or not currency:
        return {"statusCode": 400, "body": json.dumps({"message": "orderId, amount, and currency are required"})}


    # Fetch order from DynamoDB
    table  = dynamodb.Table(ORDER_TABLE_NAME)
    result = table.get_item(Key={"orderId": order_id})
    order  = result.get("Item")

    if not order:
        return {"statusCode": 404, "body": json.dumps({"message": f"Order {order_id} not found"})}

    user_id = order.get("userId", "unknown")

    try:
        stripe_key     = get_stripe_key()
        payment_intent = create_payment_intent(stripe_key, amount, currency, order_id, user_id)

        payment_intent_id = payment_intent["id"]
        pi_status         = payment_intent["status"]

        print(f"[OK] PaymentIntent created: {payment_intent_id} status={pi_status} for orderId={order_id}")

        # Save paymentIntentId back to Order record
        table.update_item(
            Key={"orderId": order_id},
            UpdateExpression="SET paymentIntentId = :pi, paymentStatus = :ps, paymentUpdatedAt = :t",
            ExpressionAttributeValues={
                ":pi": payment_intent_id,
                ":ps": pi_status,
                ":t":  datetime.now(timezone.utc).isoformat(),
            },
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "orderId":         order_id,
                "paymentIntentId": payment_intent_id,
                "status":          pi_status,
                "amount":          amount,
                "currency":        currency,
            }),
        }

    except Exception as e:
        print(f"[ERROR] Stripe call failed for orderId={order_id}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(e)}),
        }


