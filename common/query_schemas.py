import datetime
from typing import Annotated, Any, Dict, List, Literal

from pydantic import BaseModel, Field


class ClusteringRequest(BaseModel):
    dataset_id: str = Field(description="ID датасета")
    clustering_algo: str = Field(description="Алгоритма кластеризации")
    embeddings_method: str = Field(description="Метод вычисления эмбеддингов")
    clustering_hyperparams: Dict[str, Any] = Field(
        description="Гиперпараметры алгоритма кластеризации"
    )
    embeddings_hyperparams: Dict[str, Any] = Field(
        description="Гиперпараметры эмбеддингов"
    )


class ClusteringRequestWithJobId(BaseModel):
    dataset_id: Annotated[str, "ID датасета"]
    clustering_algo: Annotated[str, "Алгоритма кластеризации"]
    embeddings_method: Annotated[str, "Метод вычисления эмбеддингов"]
    clustering_hyperparams: Annotated[dict, "Гиперпараметры алгоритма кластеризации"]
    embeddings_hyperparams: Annotated[dict, "Гиперпараметры эмбеддингов"]

    job_id: Annotated[str, "ID задачи кластеризации"]


class JobUpdateRequest(BaseModel):
    new_status: Annotated[
        Literal["waiting", "running", "done", "failed"], "Новый статус задачи"
    ] = "waiting"


class JobAcceptedResponse(BaseModel):
    job_id: Annotated[str, "ID задачи кластеризации"] = "abcde_123"


class JobInfoResponse(BaseModel):
    status: Annotated[
        Literal["waiting", "running", "done", "failed"], "Статус задачи"
    ] = "waiting"

    dataset_id: Annotated[str, "ID датасета"] = "ds_123"
    clustering_algo: Annotated[str, "Алгоритма кластеризации"] = "k-means"
    embeddings_method: Annotated[str, "Метод вычисления эмбеддингов"] = "bert"
    clustering_hyperparams: Annotated[
        dict, "Гиперпараметры алгоритма кластеризации"
    ] = {"n_clusters": 5}
    embeddings_hyperparams: Annotated[dict, "Гиперпараметры эмбеддингов"] = {
        "alpha": 0.1
    }


class ClusteringResultResponse(BaseModel):
    download_url: str


class ErrorResponse(BaseModel):
    message: Annotated[str, "Описание ошибки"]
