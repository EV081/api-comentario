import boto3
import uuid
import os
import json
from datetime import datetime, timezone

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

def _parse_body(event):
    """
    Soporta:
    - event["body"] como dict (mapping template / lambda integration)
    - event["body"] como str (lambda-proxy)
    - event completo como dict (invocación directa)
    """
    body = event.get("body", event)
    if isinstance(body, str):
        body = json.loads(body)
    return body

def lambda_handler(event, context):
    print("Evento recibido:", event)

    body = _parse_body(event)
    tenant_id = body["tenant_id"]
    texto = body["texto"]

    table_name = os.environ["TABLE_NAME"]
    ingest_bucket = os.environ["INGEST_BUCKET"]

    uuidv1 = str(uuid.uuid1())
    now = datetime.now(timezone.utc).isoformat()

    comentario = {
        "tenant_id": tenant_id,
        "uuid": uuidv1,
        "detalle": {
            "texto": texto
        },
        "created_at": now
    }

    table = dynamodb.Table(table_name)
    ddb_response = table.put_item(Item=comentario)

    date = datetime.now(timezone.utc)
    s3_key = f"tenant/{tenant_id}/{date:%Y/%m/%d}/{uuidv1}.json"

    s3.put_object(
        Bucket=ingest_bucket,
        Key=s3_key,
        Body=json.dumps(comentario, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
        Metadata={
            "tenant_id": tenant_id,
            "uuid": uuidv1
        }
    )

    print("Comentario guardado:", comentario)
    return {
        "statusCode": 200,
        "comentario": comentario,
        "dynamodb_response": ddb_response,
        "s3": {
            "bucket": ingest_bucket,
            "key": s3_key
        }
    }
