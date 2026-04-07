import logging
import os
from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler
from typing import Annotated

import requests
import pandas as pd
from fastapi import FastAPI, File, HTTPException
from fastapi.responses import FileResponse

from common.query_schemas import *

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

# ----------------------------------- Глобальные переменные --------------------------------------

job_info_example = {
    "status": "running",
    "dataset_id": "ds-12345",
    "clustering_algo": "k-means",
    "embeddings_method": "BERT",
    "clustering_hyperparams": {"n_clusters": 5},
    "embeddings_hyperparams": {"alpha": 0.3, "k": 10},
}

JOBS_SERVER_URL = "http://fastapi:8001"


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
    # Генерация ID задачи
    job_id = "abcde_123"

    # Передача задачи на jobs_server
    # В случае принятия задачи внутренним сервером возвращаем такой ответ:
    json_request = 
    response = requests.post(f"{JOBS_SERVER_URL}/commit_job", json=)

    status_code = status.HTTP_202_ACCEPTED

    return JSONResponse(status_code=status_code, content=response)


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
    # Делаем запрос в jobs_server, получаем данные о задаче
    status_code = status.HTTP_200_OK
    response = JobInfoResponse.model_validate(job_info_example)

    return JSONResponse(status_code=status_code, content=response.model_dump())


@app.get(
    "/job_result/{job_id}",
    status_code=200,
    responses={
        200: {
            "content": {"text/csv": {"schema": {"type": "string", "format": "binary"}}},
            "description": "Результат выполнения задачи готов",
        },
        403: {"description": "Результат ещё не готов"},
        404: {"description": "Задача не найдена"},
        422: {"description": "Некорректный запрос"},
    },
)
async def job_result(job_id: Annotated[str, "ID задачи кластеризации"]):
    # Делаем запрос в jobs_server, проверяем статус задачи
    # В случае готовности задачи, скачиваем задачу с S3 хранилища
    # Удаляем данные о задаче с jobs_server'а и хранилища

    return FileResponse(
        path="umath-validation-set-log.csv",
        media_type="text/csv",
        filename=f"umath-validation-set-log.csv",
        headers={"Content-Disposition": f"attachment; filename={job_id}.csv"},
    )
