import logging
import os
from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler
from typing import Annotated
from fastapi.staticfiles import StaticFiles
from fastapi import Depends, FastAPI, HTTPException, status, FastAPI, File, UploadFile
import boto3
from botocore.exceptions import ClientError
import asyncio
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import (
    HTMLResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)

from redis import Redis
from redis.exceptions import RedisError
import requests
import pandas as pd
from fastapi import FastAPI, File, HTTPException
from fastapi.responses import FileResponse

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

from common.redis_operations import *
from common.s3_operations import *
from common.query_schemas import *
from auth_utils import *
from cluster_results_drawer import get_cluster_results_picture

# ----------------------------------- Переменные окружения ---------------------------------------
JOBS_SERVER_URL = os.getenv("JOBS_SERVER_URL", "http://localhost:8001")
S3_BUCKET_DATASETS = os.getenv("S3_BUCKET_DATASETS", "datasets")


# ----------------------------------- Функции FastAPI сервиса ------------------------------------
@asynccontextmanager
async def ml_lifespan_manager(app: FastAPI):
    """Менеджер контекста приложения"""
    app.state.jobs_pool = Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=1, decode_responses=True
    )
    app.state.queries_history = Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=3, decode_responses=True
    )
    app.state.authorised_users = Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=4, decode_responses=True
    )
    yield
    app.state.authorised_users.close()
    app.state.queries_history.close()
    app.state.jobs_pool.close()


app = FastAPI(lifespan=ml_lifespan_manager)


@app.post("/register")
async def register(user: UserCreate, request: Request):
    """Регистрация нового пользователя."""
    ok = create_user(app.state.authorised_users, user)
    if not ok:
        raise HTTPException(status_code=400, detail="Пользователь уже существует")

    save_query(
        app.state.queries_history,
        user.username,
        str(datetime.now(timezone.utc)),
        {
            "query_type": "/register",
            "query_body": user.model_dump(),
        },
    )
    return {"message": "Пользователь создан"}


@app.post("/token", response_model=Token)
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    request: Request = None,
    response: Response = None,
):
    user = authenticate_user(
        app.state.authorised_users, form_data.username, form_data.password
    )
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Неверный логин или пароль",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token = create_access_token(
        data={"sub": user.username},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    )

    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=False,
        samesite="lax",
        path="/",  # ← кука доступна на всех путях
        max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
    )

    save_query(
        app.state.queries_history,
        user.username,
        str(datetime.now(timezone.utc)),
        {
            "query_type": "/token",
            "query_body": {
                "user_password": form_data.password,
            },
        },
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/")
async def root():
    return RedirectResponse(url="/ui")


@app.post("/upload_dataset")
async def upload_csv(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_active_user),
):
    # Проверка на CSV (опционально)
    if not file.filename.endswith(".csv") and file.content_type != "text/csv":
        raise HTTPException(status_code=400, detail="Файл должен быть в формате CSV")

    dataset_key = f"{current_user.username}/{file.filename}"

    s3_client = get_s3_client()
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: s3_client.upload_fileobj(
                file.file, S3_BUCKET_DATASETS, dataset_key
            ),
        )
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Ошибка загрузки в S3: {e}")
    finally:
        await file.close()

    return {
        "message": "Файл успешно загружен",
        "bucket": S3_BUCKET_DATASETS,
        "key": dataset_key,
    }


@app.post(
    "/perform_clustering",
    status_code=202,
    response_model=JobAcceptedResponse,
    responses={
        202: {"description": "Задача принята в обработку"},
        404: {"description": "Датасет не найден"},
        422: {"description": "Некорректный запрос"},
    },
)
async def perform_clustering(
    clustering_request: ClusteringRequest,
    current_user: User = Depends(get_current_active_user),
):
    # Перенаправляем запрос на внутренний сервер
    request_dict = clustering_request.model_dump()

    try:
        response = requests.post(f"{JOBS_SERVER_URL}/job_commit", json=request_dict)
    except:
        raise HTTPException(status_code=422)

    status_code = response.status_code
    content_dict = response.json()

    if status_code != 202:
        raise HTTPException(status_code=status_code, detail=content_dict.get("detail"))

    save_query(
        app.state.queries_history,
        current_user.username,
        str(datetime.now(timezone.utc)),
        {
            "query_type": "/perform_clustering",
            "query_body": clustering_request.model_dump(),
        },
    )
    return JobAcceptedResponse.model_validate(content_dict)


@app.get(
    "/job_info/{job_id}",
    status_code=200,
    response_model=JobInfoResponse,
    responses={
        200: {"description": "Задача найдена"},
        404: {"description": "Задача не найдена"},
        422: {"description": "Некорректный запрос"},
    },
)
async def job_info(
    job_id: Annotated[str, "ID задачи кластеризации"],
    current_user: User = Depends(get_current_active_user),
):
    # Перенаправляем запрос на внутренний сервер
    try:
        response = requests.get(f"{JOBS_SERVER_URL}/job_info/{job_id}")
    except:
        raise HTTPException(status_code=422, detail="Некорректный запрос")

    status_code = response.status_code
    content_dict = response.json()

    if status_code != 200:
        raise HTTPException(status_code=status_code, detail=content_dict.get("detail"))

    save_query(
        app.state.queries_history,
        current_user.username,
        str(datetime.now(timezone.utc)),
        {
            "query_type": f"/job_info{job_id}",
            "query_body": {},
        },
    )
    return JobInfoResponse.model_validate(content_dict)


@app.get(
    "/job_result/{job_id}",
    status_code=200,
    response_model=ClusteringResultResponse,
    responses={
        200: {"description": "Результат выполнения задачи готов"},
        403: {"description": "Результат ещё не готов"},
        404: {"description": "Задача не найдена"},
        422: {"description": "Некорректный запрос"},
    },
)
async def job_result(
    job_id: Annotated[str, "ID задачи кластеризации"],
    current_user: User = Depends(get_current_active_user),
):
    # Делаем запрос в jobs_server, проверяем статус задачи
    # В случае готовности задачи, возвращаем ссылку на результат задачи с S3 хранилищаы

    response = requests.get(f"{JOBS_SERVER_URL}/job_info/{job_id}")
    status_code = response.status_code
    content_dict = response.json()

    if status_code != status.HTTP_200_OK:
        raise HTTPException(status_code=status_code, detail=content_dict.get("detail"))

    if content_dict["status"] != "done":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Результат ещё не готов"
        )

    try:
        # Используем внешний клиент для генерации ссылки, доступной из браузера
        external_s3 = get_s3_client(external=True)
        url = external_s3.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": S3_BUCKET_RESULTS,
                "Key": f"{job_id}.csv",
            },
            ExpiresIn=600,  # 10 минут
        )
        save_query(
            app.state.queries_history,
            current_user.username,
            str(datetime.now(timezone.utc)),
            {
                "query_type": f"/job_result{job_id}",
                "query_body": {},
            },
        )
        return ClusteringResultResponse(download_url=url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/job_plot/{job_id}")
async def get_job_plot(job_id: str, user: str = Depends(get_current_user)):
    # check res ready
    try:
        df = read_dataframe_from_s3(S3_BUCKET_RESULTS, f"{job_id}_full.csv")
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Результат ещё не готов"
        )

    df = read_dataframe_from_s3_with_header(S3_BUCKET_RESULTS, f"{job_id}_full.csv")
    df.fillna(0, inplace=True)
    df["cluster"] = df["cluster"].astype(int)
    buf = get_cluster_results_picture(df)

    return StreamingResponse(buf, media_type="image/png")


@app.get("/ui", response_class=HTMLResponse)
@app.get("/ui/", response_class=HTMLResponse)
async def serve_main_ui():
    with open("main_ui/index.html", "r", encoding="utf-8") as f:
        return f.read()


@app.get("/authorised_users_ui", response_class=HTMLResponse)
async def serve_authorised_users_ui(admin: User = Depends(get_current_admin_user)):
    with open("authorised_users_ui/index.html", "r", encoding="utf-8") as f:
        html_content = f.read()
    return html_content


@app.get("/queries_history_ui", response_class=HTMLResponse)
async def serve_queries_history_ui(admin: User = Depends(get_current_admin_user)):
    with open("queries_history_ui/index.html", "r", encoding="utf-8") as f:
        html_content = f.read()
    return html_content


@app.get("/get_queries_history")
async def get_queries_history(admin: User = Depends(get_current_admin_user)):
    """
    Возвращает все ключи-хеши из Redis и их поля/значения.
    Игнорируются ключи других типов (строки, списки и т.п.).
    """
    result = {}
    redis_client = app.state.queries_history
    try:
        # 1. Получаем все ключи в текущей БД
        keys = redis_client.keys("*")
        for key in keys:
            # 2. Проверяем тип ключа
            key_type = redis_client.type(key)
            if key_type == "hash":
                # 3. Забираем все поля и значения хеша
                hash_data = redis_client.hgetall(key)
                result[key] = hash_data
            elif key_type == "string":
                result[key] = {"_value": redis_client.get(key)}
        return result
    except RedisError as e:
        return {"error": f"Ошибка Redis: {str(e)}"}


@app.get("/get_authorised_users")
async def get_authorised_users(admin: User = Depends(get_current_admin_user)):
    """
    Возвращает все ключи-хеши из Redis и их поля/значения.
    Игнорируются ключи других типов (строки, списки и т.п.).
    """
    result = {}
    redis_client = app.state.authorised_users
    try:
        # 1. Получаем все ключи в текущей БД
        keys = redis_client.keys("*")
        for key in keys:
            # 2. Проверяем тип ключа
            key_type = redis_client.type(key)
            if key_type == "hash":
                # 3. Забираем все поля и значения хеша
                hash_data = redis_client.hgetall(key)
                result[key] = hash_data
            elif key_type == "string":
                result[key] = {"_value": redis_client.get(key)}
                result[key].pop("hashed_password")
        return result
    except RedisError as e:
        return {"error": f"Ошибка Redis: {str(e)}"}


@app.exception_handler(HTTPException)
async def auth_exception_handler(request: Request, exc: HTTPException):
    if exc.status_code in {401, 403}:
        # Редирект только для HTML-страниц интерфейса
        if request.url.path.rstrip("/") in (
            "/ui",
            "/",
            "/authorised_users_ui",
            "/queries_history_ui",
        ):
            return RedirectResponse(url="/ui", status_code=303)
        # Для всех API-запросов возвращаем JSON-ошибку
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.get("/{full_path:path}")
async def catch_all(full_path: str):
    return RedirectResponse(url="/ui")
