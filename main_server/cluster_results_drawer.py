import io

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
from sklearn.metrics import davies_bouldin_score, calinski_harabasz_score, silhouette_score
from scipy.spatial.distance import pdist, cdist

from clusters_themes_modeling import get_clusters_themes


def get_cluster_results_picture(
    df_with_embeddings: pd.DataFrame,
    df_with_texts: pd.DataFrame,
    theme_length,
    feature_cols=None,
) -> io.BytesIO:
    """
    Визуализация распределения объектов по кластерам и ключевых метрик качества.

    Параметры
    ----------
    df_with_embeddings : pd.DataFrame
        Датафрейм, содержащий столбец 'cluster' с номерами кластеров и признаки.
    df_with_texts : pd.DataFrame
        Датафрейм с текстами для генерации тем кластеров.
    theme_length : int
        Длина темы (количество слов/токенов).
    feature_cols : list or None
        Список столбцов с признаками. Если None, используются все столбцы, кроме 'cluster'.
    """
    # Определяем признаки
    if feature_cols is None:
        feature_cols = [c for c in df_with_embeddings.columns if c != "cluster"]
    X = df_with_embeddings[feature_cols].values
    labels = df_with_embeddings["cluster"].values
    unique_labels = np.unique(
        labels  # pyright: ignore[reportArgumentType, reportCallIssue]
    )
    k = len(unique_labels)  # количество непустых кластеров

    clusters_themes = get_clusters_themes(df_with_texts, k, theme_length)

    # ------------------- Вычисление метрик -------------------
    # 1. Davies-Bouldin Index (чем меньше, тем лучше)
    db = davies_bouldin_score(X, labels)  # pyright: ignore[reportArgumentType]

    # 2. Calinski-Harabasz Index (чем больше, тем лучше)
    ch = calinski_harabasz_score(X, labels)  # pyright: ignore[reportArgumentType]

    # 3. Dunn Index (чем больше, тем лучше)
    diameters = []
    for lab in unique_labels:
        pts = X[labels == lab]
        if len(pts) < 2:
            diameters.append(0.0)
        else:
            diameters.append(np.max(pdist(pts)))
    max_diam = max(diameters) if diameters else 0

    inter_min = float("inf")
    for i in range(k):
        for j in range(i + 1, k):
            pts_i = X[labels == unique_labels[i]]
            pts_j = X[labels == unique_labels[j]]
            min_dist = np.min(cdist(pts_i, pts_j))
            if min_dist < inter_min:
                inter_min = min_dist
    if inter_min == float("inf"):
        inter_min = 0
    dunn = inter_min / max_diam if max_diam != 0 else 0
    
    # 4. Silhouette Score (чем ближе к 1, тем лучше)
    if k >= 2:
        sil = silhouette_score(X, labels)  # pyright: ignore[reportArgumentType]
    else:
        sil = np.nan

    # ------------------- Цветовое кодирование метрик -------------------
    def quality_db(val):
        if val <= 0.5:
            return 1.0
        elif val >= 2.0:
            return 0.0
        else:
            return 1.0 - (val - 0.5) / (2.0 - 0.5)

    def quality_ch(val):
        if val <= 10:
            return 0.0
        elif val >= 1000:
            return 1.0
        else:
            return (np.log10(val) - 1.0) / (3.0 - 1.0)

    def quality_dunn(val):
        if val <= 0.01:
            return 0.0
        elif val >= 1.0:
            return 1.0
        else:
            return (np.log10(val) - (-2)) / (0 - (-2))

    def quality_sil(val):
        if np.isnan(val):
            return 0.0
        return (val + 1.0) / 2.0

    cmap = plt.cm.RdYlGn  # pyright: ignore[reportAttributeAccessIssue]

    def quality_to_color(quality):
        return cmap(quality)

    # ------------------- Построение графиков -------------------
    fig = plt.figure(figsize=(15, 7))
    gs = GridSpec(2, 4, figure=fig, height_ratios=[3, 1], hspace=0.4, wspace=0.3)

    # Верхняя строка: гистограмма распределения по кластерам
    ax_hist = fig.add_subplot(gs[0, :])
    cluster_counts = df_with_embeddings["cluster"].value_counts().sort_index()
    bars = ax_hist.bar(
        cluster_counts.index.astype(str),
        cluster_counts.values,  # pyright: ignore[reportArgumentType]
        color="steelblue",
        edgecolor="black",
        alpha=0.85,
    )
    ax_hist.set_xlabel("Кластер")
    ax_hist.set_ylabel("Количество объектов")
    ax_hist.set_title("Распределение объектов по кластерам")

    # Подписи значений на столбцах
    for bar, count in zip(bars, cluster_counts.values):
        ax_hist.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 1,
            str(count),
            ha="center",
            va="bottom",
            fontsize=9,
        )

    # ---- ДОБАВЛЕННЫЙ БЛОК: подписи тем под столбцами с поворотом ----
    # Формируем метки: номер кластера + тема (перенос строки)
    labels_with_themes = []
    for label, theme in zip(cluster_counts.index, clusters_themes):
        # Ограничим длину темы для компактности (опционально)
        # if len(theme) > 40:
        #     theme = theme[:37] + "..."
        theme = theme.replace("_", "\n")
        labels_with_themes.append(f"{label}\n{theme}")

    ax_hist.set_xticks(range(len(cluster_counts)))
    ax_hist.set_xticklabels(labels_with_themes, ha="right", fontsize=8)

    # Небольшой дополнительный отступ снизу, чтобы текст не обрезался
    plt.subplots_adjust(bottom=0.2)
    # ----------------------------------------------------------------

    # Нижняя строка: три ячейки с метриками
    metrics = [
        ("Davies–Bouldin Index", db, "меньше → лучше", quality_db(db)),
        ("Calinski–Harabasz Index", ch, "больше → лучше", quality_ch(ch)),
        ("Dunn Index", dunn, "больше → лучше", quality_dunn(dunn)),
        ("Silhouette Score",        sil, "больше → лучше", quality_sil(sil)),
    ]

    for idx, (name, value, desc, qual) in enumerate(metrics):
        ax = fig.add_subplot(gs[1, idx])
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis("off")
        color = quality_to_color(qual)
        rect = plt.Rectangle(  # pyright: ignore[reportPrivateImportUsage]
            (0.05, 0.15),
            0.9,
            0.7,
            linewidth=2,
            edgecolor="black",
            facecolor=color,
            alpha=0.9,
        )
        ax.add_patch(rect)
        ax.text(
            0.5,
            0.6,
            f"{name}",
            ha="center",
            va="center",
            fontsize=10,
            fontweight="bold",
            transform=ax.transAxes,
        )
        ax.text(
            0.5,
            0.35,
            f"{value:.3f}",
            ha="center",
            va="center",
            fontsize=12,
            fontweight="bold",
            transform=ax.transAxes,
        )
        ax.text(
            0.5,
            0.1,
            f"({desc})",
            ha="center",
            va="center",
            fontsize=8,
            style="italic",
            transform=ax.transAxes,
        )

    fig.suptitle(
        "Оценка качества кластеризации", fontsize=14, fontweight="bold", y=1.02
    )

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf
