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

# ----------------------------------- Глобальные переменные --------------------------------------


# ----------------------------------- Функции FastAPI сервиса ------------------------------------
@asynccontextmanager
async def ml_lifespan_manager(app: FastAPI):
    """Менеджер контекста приложения"""
    # создаем очередь, кэши.
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
    response_model=None,
    responses={
        202: {"description": "Задача добавлена в очередь"},
        404: {"description": "Датасет не найден"},
        422: {"description": "Некорректный запрос"},
    },
)
async def job_commit(job_info: ClusteringRequestWithJobId):
    # Валидируем задачу
    # Кладём задачу в очередь
    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=None)


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
