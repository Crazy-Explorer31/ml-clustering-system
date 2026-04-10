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

# ----------------------------------- Глобальные переменные --------------------------------------
session = aioboto3.Session()

s3_config = Config(
    s3={"addressing_style": "path"}, signature_version="s3v4"  # ← ключевое исправление
)

S3_CLIENT_KWARGS = {
    "endpoint_url": S3_ENDPOINT,
    "aws_access_key_id": S3_ACCESS_KEY,
    "aws_secret_access_key": S3_SECRET_KEY,
    "region_name": S3_REGION,
    "config": s3_config,
    "use_ssl": False,
    "verify": False,
}


@asynccontextmanager
async def get_s3_client():
    async with session.client("s3", **S3_CLIENT_KWARGS) as client:
        yield client


# ----------------------------------- Функции управления S3 -------------------------------------
# async def read_dataframe_from_s3_async(bucket: str, key: str) -> pd.DataFrame:
#     async with get_s3_client() as s3_client:
#         response = await s3_client.get_object(Bucket=bucket, Key=key)
#         data = await response["Body"].read()

#         return pd.read_csv(io.BytesIO(data))


# async def write_dataframe_to_s3_async(df: pd.DataFrame, bucket: str, key: str):
#     csv_buffer = io.StringIO()
#     df.to_csv(csv_buffer, index=False)

#     async with get_s3_client() as s3_client:
#         await s3_client.put_object(
#             Bucket=bucket,
#             Key="{key}.csv",
#             Body=csv_buffer.getvalue().encode("utf-8"),
#             ContentType="text/csv",
#         )


def read_dataframe_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Синхронное чтение CSV из S3 в pandas DataFrame."""
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    data = response["Body"].read()
    return pd.read_csv(io.BytesIO(data))


def write_dataframe_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    """Синхронная запись DataFrame в CSV на S3."""
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=f"{key}.csv",
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
