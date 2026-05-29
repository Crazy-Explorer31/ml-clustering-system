import os

# ----------------------------------- Переменные окружения ---------------------------------------
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET_DATASETS = os.getenv("S3_BUCKET_DATASETS", "datasets")
S3_BUCKET_RESULTS = os.getenv("S3_BUCKET_RESULTS", "results")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

S3_ENDPOINT_INTERNAL = os.environ.get("S3_ENDPOINT_URL", "http://minio:9000")
S3_ENDPOINT_EXTERNAL = os.environ.get(
    "S3_ENDPOINT_EXTERNAL_URL", "http://108.165.32.182:9000"
)

AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "change-me-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")

REDIS_EMBEDDINGS_CACHE_ID = os.getenv("REDIS_EMBEDDINGS_CACHE_ID", "0")
REDIS_JOBS_POOL_ID = os.getenv("REDIS_JOBS_POOL_ID", "1")
REDIS_JOBS_QUEUE_ID = os.getenv("REDIS_JOBS_QUEUE_ID", "2")
REDIS_QUERIES_HISTORY_ID = os.getenv("REDIS_QUERIES_HISTORY_ID", "3")
REDIS_AUTHORISED_USERS_ID = os.getenv("REDIS_AUTHORISED_USERS_ID", "4")

JOBS_SERVER_URL = os.getenv("JOBS_SERVER_URL", "http://localhost:8001")
