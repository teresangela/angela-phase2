from boto3.dynamodb.types import TypeDeserializer

_deserializer = TypeDeserializer()


def stream_table_name(record: dict) -> str:
    """
    ARN example: arn:aws:dynamodb:region:acct:table/User/stream/...
    """
    arn = record.get("eventSourceARN", "") or record.get("eventSourceArn", "")
    if ":table/" not in arn:
        raise ValueError(f"Invalid stream ARN: {arn}")
    return arn.split(":table/")[1].split("/")[0]


def index_name_from_table(table_name: str) -> str:
    # User -> user, Product -> product
    return table_name.lower()


def extract_pk(record: dict):
    """
    Extract partition key name + value from record['dynamodb']['Keys'].
    Works for single-PK tables: userId / productId.
    """
    keys = (record.get("dynamodb") or {}).get("Keys") or {}
    if not keys:
        raise ValueError("No Keys found in DynamoDB stream record")

    pk_attr = next(iter(keys.keys()))
    pk_val_obj = keys[pk_attr]

    if "S" in pk_val_obj:
        return pk_attr, pk_val_obj["S"]
    if "N" in pk_val_obj:
        return pk_attr, pk_val_obj["N"]

    return pk_attr, _deserializer.deserialize(pk_val_obj)


def ddb_image_to_dict(ddb_image: dict) -> dict:
    """
    ddb_image looks like {"field": {"S": "x"}, ...}
    """
    return _deserializer.deserialize({"M": ddb_image})


def event_name(record: dict) -> str:
    return record.get("eventName", "")