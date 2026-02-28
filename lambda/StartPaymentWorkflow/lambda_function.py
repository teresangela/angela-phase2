import json, os, boto3
sfn = boto3.client("stepfunctions")

def handler(event, context):
    body = event.get("body") or "{}"
    payload = json.loads(body)

    resp = sfn.start_execution(
        stateMachineArn=os.environ["STATE_MACHINE_ARN"],
        input=json.dumps(payload),
    )
    return {
        "statusCode": 202,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"executionArn": resp["executionArn"]}),
    }