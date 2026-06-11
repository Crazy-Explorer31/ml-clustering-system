from contextlib import asynccontextmanager
from typing import Annotated
from pydantic import ValidationError
from fastapi import FastAPI, HTTPException
from common.query_schemas import (
    ClusteringRequest,
    JobAcceptedResponse,
    JobInfoResponse,
    JobUpdateRequest,
)

from fastapi import status
from fastapi.responses import JSONResponse

from redis import Redis
from rq import Queue
from rq import get_current_job

from common.redis_operations import (
    save_job_state,
    get_job_state,
    delete_job_state,
    update_job_status,
)
from managers import EmbeddingsCacheManager, ClusteringManager
from common.env_vars import (
    REDIS_HOST,
    REDIS_PORT,
    REDIS_JOBS_POOL_ID,
    REDIS_JOBS_QUEUE_ID,
)

# ----------------------------------- Глобальные переменные --------------------------------------
embeddings_cache_manager = EmbeddingsCacheManager()
clustering_manager = ClusteringManager()

# --------------------------------- Функция запуска кластеризации --------------------------------


def run_clustering(job_params: dict):
    current_job = get_current_job()
    job_id = current_job.id if current_job is not None else "-1"
    # job_id = "123" # DEBUG
    jobs_pool = Redis(
        host=REDIS_HOST,
        port=int(REDIS_PORT),
        db=int(REDIS_JOBS_POOL_ID),
        decode_responses=True,
    )
    update_job_status(jobs_pool, job_id, "running")

    embeddings_key = (
        job_params["dataset_id"],
        job_params["embeddings_method"],
        job_params["embeddings_hyperparams"],
    )
    embeddings_cache_manager.make_ready(embeddings_key, job_id)

    clustering_manager.find_clusters(
        embeddings_cache_manager.get(
            embeddings_key  # pyright: ignore[reportArgumentType]
        ),
        job_params["clustering_algo"],
        job_params["clustering_hyperparams"],
        job_id,
        job_params["dataset_id"],
    )

    update_job_status(jobs_pool, job_id, "done")

    jobs_pool.close()


# ----------------------------------- Функции FastAPI сервиса ------------------------------------
@asynccontextmanager
async def ml_lifespan_manager(app: FastAPI):
    """Менеджер контекста приложения"""
    app.state.jobs_pool = Redis(
        host=REDIS_HOST,
        port=int(REDIS_PORT),
        db=int(REDIS_JOBS_POOL_ID),
        decode_responses=True,
    )
    app.state.jobs_queue = Queue(
        connection=Redis(
            host=REDIS_HOST, port=int(REDIS_PORT), db=int(REDIS_JOBS_QUEUE_ID)
        )
    )
    yield
    app.state.jobs_queue.connection.close()
    app.state.jobs_pool.close()


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
    job = app.state.jobs_queue.enqueue(run_clustering, job_params)
    response = JobAcceptedResponse(job_id=job.id)
    response_dict = response.model_dump()

    # Сохраняем данные о задаче
    save_job_state(app.state.jobs_pool, job.id, job_params | {"status": "waiting"})

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
    job_state = get_job_state(app.state.jobs_pool, job_id)
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
    job_state = get_job_state(app.state.jobs_pool, job_id)
    if job_state is None:
        status_code = status.HTTP_404_NOT_FOUND
    else:
        status_code = status.HTTP_201_CREATED
        delete_job_state(app.state.jobs_pool, job_id)
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
    job_state = get_job_state(app.state.jobs_pool, job_id)
    if job_state is None:
        status_code = status.HTTP_404_NOT_FOUND
    else:
        status_code = status.HTTP_201_CREATED
        update_job_status(app.state.jobs_pool, job_id, job_update.new_status)
    return JSONResponse(status_code=status_code, content=None)
