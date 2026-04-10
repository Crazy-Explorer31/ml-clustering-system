import logging
import os
from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler
from typing import Annotated

import requests
import pandas as pd
from fastapi import FastAPI, File, HTTPException
from fastapi.responses import FileResponse

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

from common.s3_operations import *
from common.query_schemas import *

# ----------------------------------- Переменные окружения ---------------------------------------
JOBS_SERVER_URL = os.getenv("JOBS_SERVER_URL", "http://localhost:8001")


# ----------------------------------- Функции FastAPI сервиса ------------------------------------
@asynccontextmanager
async def ml_lifespan_manager(app: FastAPI):
    """Менеджер контекста приложения"""
    yield


app = FastAPI(lifespan=ml_lifespan_manager)


@app.get(
    "/",
    status_code=200,
    response_model=None,
    description="Корневая страница сервера-обработчика пользовательских запросов",
)
async def root() -> Annotated[dict, "Метаданные корневой страницы"]:
    return {
        "Name": "Система кластеризации медицинских документов",
        "Description": "Предоставляет возможности для загрузки своих задач кластеризации и получения результатов их выполнения",
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
async def perform_clustering(clustering_request: ClusteringRequest):
    # Перенаправляем запрос на внутренний сервер
    request_content = clustering_request.model_dump()

    try:
        response = requests.post(f"{JOBS_SERVER_URL}/job_commit", json=request_content)
    except:
        raise HTTPException(status_code=422)

    status_code = response.status_code
    if status_code != 202:
        raise HTTPException(status_code=status_code, detail=response.content["detail"])

    content = response.json()
    return JobAcceptedResponse.model_validate(content)


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
async def job_info(job_id: Annotated[str, "ID задачи кластеризации"]):
    # Перенаправляем запрос на внутренний сервер
    try:
        response = requests.get(f"{JOBS_SERVER_URL}/job_info/{job_id}")
    except:
        raise HTTPException(status_code=422, detail="Некорректный запрос")
    status_code = response.status_code
    content = response.json()

    if status_code != 200:
        raise HTTPException(status_code=status_code, detail=content.get("detail"))

    return JobInfoResponse.model_validate(content)


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
):
    # Делаем запрос в jobs_server, проверяем статус задачи
    # В случае готовности задачи, возвращаем ссылку на результат задачи с S3 хранилищаы

    response = requests.get(f"{JOBS_SERVER_URL}/job_info/{job_id}")

    status_code = response.status_code

    if status_code != status.HTTP_200_OK:
        raise HTTPException(status_code=status_code, detail=response.content["detail"])

    content = response.json()
    if content["status"] != "done":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Результат ещё не готов"
        )

    try:
        async with get_s3_client() as s3_client:
            url = s3_client.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": S3_BUCKET_RESULTS,
                    "Key": f"{job_id}.csv",
                },
                ExpiresIn=600,
            )
            if "minio" in url:
                url = url.replace("minio", "localhost")

            return ClusteringResultResponse(download_url=url)
        # TODO надо как-то удалять скачанные результаты из S3
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
