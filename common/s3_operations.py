import io
import pandas as pd
import boto3
from botocore.config import Config
from common.env_vars import (
    S3_ACCESS_KEY,
    S3_ENDPOINT_EXTERNAL,
    S3_ENDPOINT_INTERNAL,
    S3_REGION,
    S3_SECRET_KEY,
)

# ----------------------------------- Функции управления S3 --------------------------------------


def get_s3_client(external: bool = False):
    """Возвращает настроенный клиент S3 для MinIO."""
    endpoint = S3_ENDPOINT_EXTERNAL if external else S3_ENDPOINT_INTERNAL
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
        config=Config(s3={"addressing_style": "path"}),  # важно!
    )


def read_dataframe_from_s3(bucket: str, key: str) -> pd.DataFrame:
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=bucket, Key=key)
    data = response["Body"].read()
    return pd.read_csv(io.BytesIO(data))


def read_dataframe_from_s3_with_header(bucket: str, key: str) -> pd.DataFrame:
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=bucket, Key=key)
    data = response["Body"].read()
    return pd.read_csv(io.BytesIO(data), header=0)


def write_dataframe_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_client = get_s3_client()
    s3_client.put_object(
        Bucket=bucket,
        Key=f"{key}",
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
