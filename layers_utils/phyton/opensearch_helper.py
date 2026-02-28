import os
import boto3
import requests
from urllib.parse import quote
from requests_aws4auth import AWS4Auth

ES_SERVICE = "es"
HEADERS = {"Content-Type": "application/json"}


def get_domain_endpoint() -> str:
    # expects env: ES_DOMAIN_ENDPOINT (without https://)
    ep = os.environ.get("ES_DOMAIN_ENDPOINT")
    if not ep:
        raise ValueError("Missing env var ES_DOMAIN_ENDPOINT")
    # some people store with https://, normalize it
    return ep.replace("https://", "").replace("http://", "").strip("/")


def build_awsauth() -> AWS4Auth:
    """
    Build auth fresh to avoid expired creds on warm lambdas.
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


def doc_url(index_name: str, doc_id: str, endpoint: str | None = None) -> str:
    endpoint = endpoint or get_domain_endpoint()
    safe_id = quote(str(doc_id), safe="")
    return f"https://{endpoint}/{index_name}/_doc/{safe_id}"


def put_doc(
    url: str,
    doc: dict,
    timeout_seconds: int = 10,
    retries: int = 3,
):
    for attempt in range(1, retries + 1):
        resp = requests.put(
            url,
            auth=build_awsauth(),
            json=doc,
            headers=HEADERS,
            timeout=timeout_seconds,
        )
        if resp.status_code in (200, 201):
            return
        if attempt == retries:
            raise RuntimeError(f"Failed PUT after {retries} retries: {url} | {resp.status_code} | {safe_text(resp)}")


def delete_doc(
    url: str,
    timeout_seconds: int = 10,
    retries: int = 3,
):
    for attempt in range(1, retries + 1):
        resp = requests.delete(
            url,
            auth=build_awsauth(),
            headers=HEADERS,
            timeout=timeout_seconds,
        )
        # 200/202 ok, 404 ok (already deleted)
        if resp.status_code in (200, 202, 404):
            return
        if attempt == retries:
            raise RuntimeError(f"Failed DELETE after {retries} retries: {url} | {resp.status_code} | {safe_text(resp)}")


def safe_text(resp) -> str:
    try:
        return resp.text
    except Exception:
        return "<no-text>"