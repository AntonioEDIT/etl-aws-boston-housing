import urllib.parse
import boto3

glue = boto3.client("glue")
JOB_NAME = "Prueba ETL"


def lambda_handler(event, context):
    """Se ejecuta cuando S3 recibe un archivo nuevo en raw/ e inicia el job de Glue."""
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        if not key.lower().endswith(".csv"):
            continue

        ruta = f"s3://{bucket}/{key}"
        resp = glue.start_job_run(
            JobName=JOB_NAME,
            Arguments={"--ENTRADA": ruta}
        )
        print(f"Job iniciado: {resp['JobRunId']} | archivo: {ruta}")

    return {"statusCode": 200}
