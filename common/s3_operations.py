from contextlib import asynccontextmanager
import os
import io
import pandas as pd
import aioboto3
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

# ----------------------------------- Переменные окружения ---------------------------------------
S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET_DATASETS = os.getenv("S3_BUCKET_DATASETS", "datasets")
S3_BUCKET_RESULTS = os.getenv("S3_BUCKET_RESULTS", "results")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

S3_ENDPOINT_INTERNAL = os.environ.get("S3_ENDPOINT_URL", "http://minio:9000")
S3_ENDPOINT_EXTERNAL = os.environ.get(
    "S3_ENDPOINT_EXTERNAL_URL", "http://localhost:9000"
)
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
S3_BUCKET_RESULTS = os.environ.get("S3_BUCKET_RESULTS", "results")

# ----------------------------------- Глобальные переменные --------------------------------------
session = aioboto3.Session()

S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"

# ----------------------------------- Функции управления S3 --------------------------------------


# def get_s3_client():
#     return boto3.client(
#         "s3",
#         endpoint_url=S3_ENDPOINT,
#         aws_access_key_id=S3_ACCESS_KEY,
#         aws_secret_access_key=S3_SECRET_KEY,
#     )


def get_s3_client(external: bool = False):
    """Возвращает настроенный клиент S3 для MinIO."""
    endpoint = S3_ENDPOINT_EXTERNAL if external else S3_ENDPOINT_INTERNAL
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
        config=Config(s3={"addressing_style": "path"}),  # важно!
    )


def read_dataframe_from_s3(bucket: str, key: str) -> pd.DataFrame:
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=bucket, Key=key)
    data = response["Body"].read()
    return pd.read_csv(io.BytesIO(data))


def write_dataframe_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_client = get_s3_client()
    s3_client.put_object(
        Bucket=bucket,
        Key=f"{key}.csv",
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
