import io
import json
import os

import pandas as pd

from redis import Redis

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")


# ----------------------------------- Функции управления Redis -------------------------------------
def write_dataframe_to_redis(df: pd.DataFrame, key: str, redis_conn: Redis):
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", compression="snappy")
    redis_conn.set(key, buffer.getvalue())


def read_dataframe_from_redis(key: str, redis_conn: Redis) -> pd.DataFrame:
    data = redis_conn.get(key)
    if data is None:
        raise KeyError(f"Key '{key}' not found in Redis")
    if len(data) == 0:
        raise ValueError(f"Data for key '{key}' is empty (0 bytes)")
    return pd.read_parquet(io.BytesIO(data), engine="pyarrow")


def save_job_state(jobs_pool, job_id: str, data: dict):
    """
    Сохранить/обновить полное состояние задачи.
    Все значения, являющиеся словарями или списками, сериализуются в JSON.
    """
    serialized_data = {}
    for key, value in data.items():
        if isinstance(value, (dict, list)):
            serialized_data[key] = json.dumps(value, ensure_ascii=False)
        else:
            serialized_data[key] = str(value) if value is not None else ""

    jobs_pool.hset(f"job:{job_id}", mapping=serialized_data)


def get_job_state(jobs_pool, job_id: str) -> dict:
    """
    Получить состояние задачи с десериализацией JSON-полей.
    Предполагается, что поля, изначально бывшие dict/list, сохранены как JSON-строки.
    """
    raw_data = jobs_pool.hgetall(f"job:{job_id}")
    if not raw_data:
        return {}

    # Список ключей, которые должны быть десериализованы из JSON
    json_fields = {"clustering_hyperparams", "embeddings_hyperparams"}

    result = {}
    for key, value in raw_data.items():
        if key in json_fields:
            try:
                result[key] = json.loads(value)
            except json.JSONDecodeError:
                # На случай, если поле не было корректно сериализовано
                result[key] = value
        else:
            # Попытка автоматического приведения числовых полей к int/float
            if value.isdigit():
                result[key] = int(value)
            elif value.replace(".", "", 1).isdigit() and value.count(".") < 2:
                result[key] = float(value)
            else:
                result[key] = value
    return result


def delete_job_state(jobs_pool, job_id: str):
    """Удалить состояние задачи."""
    jobs_pool.delete(f"job:{job_id}")


def update_job_status(jobs_pool, job_id: str, new_status: str):
    """Обновить только поле status."""
    jobs_pool.hset(f"job:{job_id}", "status", new_status)


def save_query(redis_conn: Redis, user_name: str, timestamp: str, query_info: dict):
    for key, value in query_info.items():
        if isinstance(value, dict):
            query_info[key] = json.dumps(value, indent=2, ensure_ascii=False)
        elif not isinstance(value, str):
            raise RuntimeError(f"save_query: dunno how to dump value: {value}")

    redis_conn.hset(
        f"{user_name}:{timestamp}",
        mapping=query_info,
    )
