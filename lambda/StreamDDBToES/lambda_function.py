import os
import boto3
import requests
from urllib.parse import quote

from requests_aws4auth import AWS4Auth
from aws_lambda_powertools import Logger, Tracer
from boto3.dynamodb.types import TypeDeserializer

logger = Logger()
tracer = Tracer()

deserializer = TypeDeserializer()

ES_DOMAIN_ENDPOINT = os.environ["ES_DOMAIN_ENDPOINT"]  # e.g. search-xxxx.ap-southeast-1.es.amazonaws.com
ES_SERVICE = "es"  # still "es" for Elasticsearch/OpenSearch in AWS SigV4

HEADERS = {"Content-Type": "application/json"}
RETRIES = 3
TIMEOUT_SECONDS = 10


def _get_awsauth() -> AWS4Auth:
    """
    IMPORTANT: Build auth fresh to avoid expired creds on warm lambdas.
    """
    session = boto3.Session()
    creds = session.get_credentials()
    frozen = creds.get_frozen_credentials()

    region = os.environ.get("AWS_REGION", session.region_name or "ap-southeast-1")

    return AWS4Auth(
        frozen.access_key,
        frozen.secret_key,
        region,
        ES_SERVICE,
        session_token=frozen.token,
    )


def _ddb_item_to_dict(ddb_image: dict) -> dict:
    # ddb_image looks like {"field": {"S": "x"}, ...}
    return deserializer.deserialize({"M": ddb_image})


def _extract_pk_value(record: dict):
    """
    Extract partition key from record['dynamodb']['Keys'].
    Works for tables with a single PK (userId/productId).
    """
    keys = record["dynamodb"].get("Keys", {})
    if not keys:
        raise ValueError("No Keys found in DynamoDB stream record")

    pk_attr = next(iter(keys.keys()))
    pk_val_obj = keys[pk_attr]

    if "S" in pk_val_obj:
        return pk_attr, pk_val_obj["S"]
    if "N" in pk_val_obj:
        return pk_attr, pk_val_obj["N"]

    return pk_attr, deserializer.deserialize(pk_val_obj)


def _doc_url(index_name: str, doc_id: str) -> str:
    safe_id = quote(doc_id, safe="")  # URL safe
    return f"https://{ES_DOMAIN_ENDPOINT}/{index_name}/_doc/{safe_id}"


@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """
    DynamoDB Streams -> Elasticsearch on Amazon OpenSearch Service (ES 7.10 engine)
    - INSERT/MODIFY => PUT doc (upsert)
    - REMOVE        => DELETE doc
    """
    records = event.get("Records", [])
    if not records:
        logger.info("No records in event")
        return {"ok": True, "processed": 0}

    processed = 0

    for r in records:
        event_name = r.get("eventName")
        if event_name not in ("INSERT", "MODIFY", "REMOVE"):
            continue

        # ARN example: arn:aws:dynamodb:region:acct:table/User/stream/...
        stream_arn = r.get("eventSourceARN", "")
        table_name = stream_arn.split(":table/")[1].split("/")[0]  # "User" or "Product"
        index_name = table_name.lower()

        _, pk_value = _extract_pk_value(r)
        doc_id = str(pk_value)

        url = _doc_url(index_name, doc_id)

        if event_name == "REMOVE":
            _delete_with_retry(url)
            processed += 1
            continue

        new_image = r["dynamodb"].get("NewImage")
        if not new_image:
            logger.warning("No NewImage for INSERT/MODIFY")
            continue

        doc = _ddb_item_to_dict(new_image)

        _put_with_retry(url, doc)
        processed += 1

    return {"ok": True, "processed": processed}


def _put_with_retry(url: str, doc: dict):
    for attempt in range(1, RETRIES + 1):
        resp = requests.put(
            url,
            auth=_get_awsauth(),
            json=doc,
            headers=HEADERS,
            timeout=TIMEOUT_SECONDS,
        )
        if resp.status_code in (200, 201):
            return

        logger.warning(
            {"attempt": attempt, "status": resp.status_code, "body": _safe_text(resp)}
        )

    raise RuntimeError(f"Failed PUT after {RETRIES} retries: {url}")


def _delete_with_retry(url: str):
    for attempt in range(1, RETRIES + 1):
        resp = requests.delete(
            url,
            auth=_get_awsauth(),
            headers=HEADERS,
            timeout=TIMEOUT_SECONDS,
        )
        # 200/202 ok, 404 ok (already deleted)
        if resp.status_code in (200, 202, 404):
            return

        logger.warning(
            {"attempt": attempt, "status": resp.status_code, "body": _safe_text(resp)}
        )

    raise RuntimeError(f"Failed DELETE after {RETRIES} retries: {url}")


def _safe_text(resp):
    try:
        return resp.text
    except Exception:
        return "<no-text>"