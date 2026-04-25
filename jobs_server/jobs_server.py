from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler
from typing import Annotated
from pydantic import ValidationError
import aioboto3
from botocore.exceptions import ClientError
from botocore.config import Config
from fastapi import FastAPI, Response, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager
import pandas as pd
from fastapi import FastAPI
from fastapi.responses import FileResponse
import asyncio
from common.query_schemas import *

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

from redis import Redis
from rq import Queue
from rq import get_current_job

from managers import *


def run_clustering(job_params: dict):
    job_id = get_current_job().id
    # job_id = "123" # DEBUG
    update_job_status(jobs_pool, job_id, "running")

    embeddings_key = (
        job_params["dataset_id"],
        job_params["embeddings_method"],
        job_params["embeddings_hyperparams"],
    )
    embeddings_cache_manager.make_ready(embeddings_key, job_id)

    clustering_manager.find_clusters(
        embeddings_cache_manager.get(embeddings_key),
        job_params["clustering_algo"],
        job_params["clustering_hyperparams"],
        job_id,
    )

    update_job_status(jobs_pool, job_id, "done")


# ----------------------------------- Глобальные переменные --------------------------------------
jobs_pool = Redis(host=REDIS_HOST, port=REDIS_PORT, db=1, decode_responses=True)
jobs_queue = Queue(connection=Redis(host=REDIS_HOST, port=REDIS_PORT, db=2))
embeddings_cache_manager = EmbeddingsCacheManager()  # singleton?
clustering_manager = ClusteringManager()  # singleton?


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
    try:
        job_params = ClusteringRequest.model_validate(job_info).model_dump()
    except ValidationError:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, content=None
        )
    print(job_params)
    # Кладём задачу в очередь
    job = jobs_queue.enqueue(run_clustering, job_params)
    response = JobAcceptedResponse(job_id=job.id)
    response_dict = response.model_dump()
    # Сохраняем данные о задаче
    save_job_state(jobs_pool, job.id, job_params | {"status": "waiting"})

    # run_clustering(job_params) # DEBUG
    # response_dict = JobAcceptedResponse(job_id="123").model_dump() # DEBUG
    # save_job_state(jobs_pool, "123", job_params | {"status": "waiting"}) # DEBUG

    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=response_dict)


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
    job_state = get_job_state(jobs_pool, job_id)
    if job_state is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Задача не найдена"
        )
    return JobInfoResponse.model_validate(job_state)


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
    job_state = get_job_state(jobs_pool, job_id)
    if job_state is None:
        status_code = status.HTTP_404_NOT_FOUND
    else:
        status_code = status.HTTP_201_CREATED
        delete_job_state(jobs_pool, job_id)
    return JSONResponse(status_code=status_code, content=None)


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
async def job_update(
    job_id: Annotated[str, "ID задачи кластеризации"], job_update: JobUpdateRequest
):
    # Ищем задачу в сохраненных, в случае нахождения обновляем
    job_state = get_job_state(jobs_pool, job_id)
    if job_state is None:
        status_code = status.HTTP_404_NOT_FOUND
    else:
        status_code = status.HTTP_201_CREATED
        update_job_status(jobs_pool, job_id, job_update.new_status)
    return JSONResponse(status_code=status_code, content=None)
