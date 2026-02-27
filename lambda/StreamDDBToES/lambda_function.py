from aws_lambda_powertools import Logger, Tracer

from ddb_stream_helper import (
    event_name,
    stream_table_name,
    index_name_from_table,
    extract_pk,
    ddb_image_to_dict,
)

from opensearch_helper import (
    doc_url,
    put_doc,
    delete_doc,
)

logger = Logger()
tracer = Tracer()


@tracer.capture_lambda_handler
def lambda_handler(event, context):
    records = event.get("Records", [])
    if not records:
        logger.info("No records in event")
        return {"ok": True, "processed": 0}

    processed = 0

    for r in records:
        ev = event_name(r)
        if ev not in ("INSERT", "MODIFY", "REMOVE"):
            continue

        table = stream_table_name(r)          # "User" / "Product"
        index = index_name_from_table(table)  # "user" / "product"

        _, pk_value = extract_pk(r)
        doc_id = str(pk_value)

        url = doc_url(index, doc_id)

        if ev == "REMOVE":
            delete_doc(url)
            processed += 1
            continue

        new_image = (r.get("dynamodb") or {}).get("NewImage")
        if not new_image:
            logger.warning("No NewImage for INSERT/MODIFY")
            continue

        doc = ddb_image_to_dict(new_image)
        put_doc(url, doc)
        processed += 1

    return {"ok": True, "processed": processed}