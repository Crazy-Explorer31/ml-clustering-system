import logging
import os
from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler
from typing import Annotated

import joblib
import pandas as pd
from fastapi import FastAPI, File, HTTPException
from fastapi.responses import FileResponse

from common.query_schemas import *

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

from redis import Redis
from rq import Queue


def run_clustering(job_params: dict) -> pd.DataFrame:
    dataset = datasets[job_params["dataset_id"]] # load from S3
    clustering_func = ClusteringFabric(
        job_params["clustering_algo"], job_params["clustering_hyperparams"]
    )
    embeddings_func = EmbeddingsFabric(
        job_params["embeddings_method"], job_params["embeddings_hyperparams"]
    )
    
    if embeddings[job_params["dataset_id"] + job_params["embeddings_method"] + job_params["embeddings_hyperparams"]]


# ----------------------------------- Глобальные переменные --------------------------------------
tasks_queue = None


# ----------------------------------- Функции FastAPI сервиса ------------------------------------
@asynccontextmanager
async def ml_lifespan_manager(app: FastAPI):
    """Менеджер контекста приложения"""
    # создаем очередь, кэши.
    redis_conn = Redis(host="localhost", port=6379, db=0)
    tasks_queue = Queue(connection=redis_conn)
    yield
    # делаем dump этого всего в память


app = FastAPI(lifespan=ml_lifespan_manager)


@app.get(
    "/",
    status_code=200,
    response_model=None,
    description="Корневая страница внутреннего сервера выполнения задач",
)
async def root() -> Annotated[dict, "Метаданные корневой страницы"]:
    return {
        "Name": "Сервер выполнения задач кластеризации",
        "Description": "(Для внутреннего использования)",
    }


@app.post(
    "/job_commit",
    status_code=202,
    response_model=JobAcceptedResponse,
    responses={
        202: {"description": "Задача добавлена в очередь"},
        404: {"description": "Датасет не найден"},
        422: {"description": "Некорректный запрос"},
    },
)
async def job_commit(job_info: ClusteringRequest):
    # Валидируем задачу
    # Кладём задачу в очередь
    job_info.embeddings_hyperparams
    job_params = ClusteringRequestWithJobId.model_validate(job_info).model_dump()
    job = tasks_queue.enqueue(run_clustering, job_params)
    response = JobAcceptedResponse(job_id=job.id).model_dump()
    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response)


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
    # Ищем задачу в сохраненных, возвращаем о ней сведения
    return JSONResponse(JobInfoResponse().model_dump())


@app.delete(
    "/job_delete/{job_id}",
    status_code=status.HTTP_201_CREATED,
    response_model=None,
    responses={
        201: {"description": "Задача удалена"},
        404: {"description": "Задача не найдена"},
        422: {"description": "Некорректный запрос"},
    },
)
async def job_delete(job_id: Annotated[str, "ID задачи кластеризации"]):
    # Ищем задачу в сохраненных, в случае нахождения удаляем
    return JSONResponse(status_code=status.HTTP_201_CREATED, content=None)


@app.put(
    "/job_update/{job_id}",
    status_code=status.HTTP_201_CREATED,
    response_model=None,
    responses={
        201: {"description": "Статус задачи обновлен"},
        404: {"description": "Задача не найдена"},
        422: {"description": "Некорректный запрос"},
    },
)
async def job_update(job_update: JobUpdateRequest):
    # Ищем задачу в сохраненных, в случае нахождения обновляем
    return JSONResponse(status_code=status.HTTP_201_CREATED, content=None)
