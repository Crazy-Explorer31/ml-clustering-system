from typing import Annotated, Any, Dict, Literal

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
    theme_length: int = Field(description="Длина названия темы для кластера")


class ClusteringRequestWithJobId(BaseModel):
    dataset_id: str = Field(description="ID датасета")
    clustering_algo: str = Field(description="Алгоритма кластеризации")
    embeddings_method: str = Field(description="Метод вычисления эмбеддингов")
    clustering_hyperparams: Dict[str, Any] = Field(
        description="Гиперпараметры алгоритма кластеризации"
    )
    embeddings_hyperparams: Dict[str, Any] = Field(
        description="Гиперпараметры эмбеддингов"
    )
    theme_length: int = Field(description="Длина названия темы для кластера")

    job_id: str = Field(description="ID задачи кластеризации")


class JobUpdateRequest(BaseModel):
    new_status: Annotated[
        Literal["waiting", "running", "done", "failed"], "Новый статус задачи"
    ] = "waiting"


class JobAcceptedResponse(BaseModel):
    job_id: str = Field(description="ID задачи кластеризации")


class JobInfoResponse(BaseModel):
    status: Annotated[
        Literal["waiting", "running", "done", "failed"], "Статус задачи"
    ] = "waiting"

    dataset_id: str = Field(description="ID датасета")
    clustering_algo: str = Field(description="Алгоритма кластеризации")
    embeddings_method: str = Field(description="Метод вычисления эмбеддингов")
    clustering_hyperparams: Dict[str, Any] = Field(
        description="Гиперпараметры алгоритма кластеризации"
    )
    embeddings_hyperparams: Dict[str, Any] = Field(
        description="Гиперпараметры эмбеддингов"
    )
    theme_length: int = Field(description="Длина названия темы для кластера")


class ClusteringResultResponse(BaseModel):
    download_url: str = Field(
        description="Ссылка на скачивание результата кластеризации"
    )


class ErrorResponse(BaseModel):
    message: str = Field(description="Сообщение об ошибке")
