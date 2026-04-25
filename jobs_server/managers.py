import os
import sys

import pandas as pd
import numpy as np

from common.s3_operations import *
from common.redis_operations import *

from redis import Redis

from sklearn.feature_extraction.text import TfidfVectorizer
from gensim.models import Word2Vec
from nltk.tokenize import word_tokenize
import fasttext
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
from gensim.utils import simple_preprocess

from sklearn.cluster import KMeans, SpectralClustering

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")


# ----------------------------------- Функции вычисления эмбеддингов -------------------------------
def tfidf_vectorize(data, vectorize_params):
    vectorizer = TfidfVectorizer(
        **vectorize_params
    )  # FIXME: криво передаются min/max_df
    X_tfidf = vectorizer.fit_transform(data.text)
    return pd.DataFrame(X_tfidf.toarray(), columns=vectorizer.get_feature_names_out())


def word2vec_vectorize(data, vectorize_params):
    tokenized_data = data.text.apply(lambda x: word_tokenize(x.lower()))

    model = Word2Vec(sentences=tokenized_data, **vectorize_params)

    def get_doc_vector(text_tokens, model):
        vectors = []
        for word in text_tokens:
            if word in model.wv:
                vectors.append(model.wv[word])
        if vectors:
            return np.mean(vectors, axis=0)
        else:
            return np.zeros(model.vector_size)

    doc_vectors = tokenized_data.apply(lambda x: get_doc_vector(x, model))

    return pd.DataFrame(doc_vectors.tolist())


def fasttext_vectorize(data, vectorize_params):
    ft = fasttext.load_model("cc.en.300.bin")
    vectors = []
    for text in data.text:
        vector = ft.get_sentence_vector(text)
        vectors.append(vector)
    return pd.DataFrame(np.array(vectors))


def doc2vec_vectorize(data, vectorize_params):
    def prepare_documents(series):
        documents = []
        for i, text in enumerate(series):
            tokens = simple_preprocess(text)
            documents.append(TaggedDocument(tokens, [i]))
        return documents

    tagged_docs = prepare_documents(data.text)
    model = Doc2Vec(**vectorize_params)

    model.build_vocab(tagged_docs)
    model.train(tagged_docs, total_examples=model.corpus_count, epochs=model.epochs)

    def vectorize_series(series, model):
        vectors = []
        for text in series:
            tokens = simple_preprocess(text)
            vector = model.infer_vector(tokens)
            vectors.append(vector)
        return pd.DataFrame(np.array(vectors))

    return vectorize_series(data.text, model)


vectorizers = {
    "tfidf": tfidf_vectorize,
    "word2vec": word2vec_vectorize,
    "fasttext": fasttext_vectorize,
    "doc2vec": doc2vec_vectorize,
}


# ----------------------------------- Функции кластеризации --------------------------------------
def cluster_kmeans(data, cluster_params):
    kmeans_model = KMeans(**cluster_params)
    clusters = kmeans_model.fit_predict(data)

    clusters = pd.DataFrame({"cluster": clusters})

    return clusters


def cluster_spectral(data, cluster_params):
    spectral_model = SpectralClustering(**cluster_params)
    clusters = spectral_model.fit_predict(data)

    clusters = pd.DataFrame({"cluster": clusters})

    return clusters


clusterizers = {"kmeans": cluster_kmeans, "spectral": cluster_spectral}
# TODO
# графически изобразить
# выдавать картинку через s3
# нужен веб интерфейс
# просто страницу описать на js(react)

# TODO
# добавить авторизацию

# TODO
# ассоциировать responses с пользователями
#   отдельная база данных для пользовательских данных (можно хранить на main_server)
#   в эту базу логировать все пользовательские запросы (их действия)


# TODO
# для S3 хранилища сделать удаленный сервер
# ----------------------------------- Классы менеджеров ------------------------------------------
def get_hash(embeddings_key: tuple):
    return " ".join(map(str, embeddings_key))


class EmbeddingsCacheManager:
    embeddings_cache = None
    jobs_pool = None

    def __init__(self):
        self.embeddings_cache = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        self.jobs_pool = Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=1, decode_responses=True
        )

    def have_embeddings(self, embeddings_key: tuple) -> bool:
        embeddings_key_hash = get_hash(embeddings_key)
        data = self.embeddings_cache.get(embeddings_key_hash)
        return data is not None and len(data) > 0

    def make_ready(self, embeddings_key: tuple, job_id: str):
        if not self.have_embeddings(embeddings_key):
            self.calculate_embeddings(embeddings_key, job_id)

    def calculate_embeddings(self, embeddings_key: tuple, job_id: str):
        dataset_id, embeddings_method, embeddings_hyperparams = embeddings_key

        try:
            dataset = read_dataframe_from_s3(S3_BUCKET_DATASETS, f"{dataset_id}.csv")
            print(dataset.head())
            vectorizer = vectorizers[embeddings_method]
            dataset_vectorized = vectorizer(dataset, embeddings_hyperparams)

            embeddings_key_hash = get_hash(embeddings_key)
            print(f"write: {embeddings_key_hash}", flush=True)
            sys.stdout.flush()
            write_dataframe_to_redis(
                dataset_vectorized, embeddings_key_hash, self.embeddings_cache
            )
        except:
            update_job_status(self.jobs_pool, job_id, "failed (calculate_embeddings)")
            raise  # comment for release

    def get(self, embeddings_key: str) -> pd.DataFrame:
        embeddings_key_hash = get_hash(embeddings_key)
        print(f"get: {embeddings_key_hash}", flush=True)
        sys.stdout.flush()
        return read_dataframe_from_redis(embeddings_key_hash, self.embeddings_cache)


class ClusteringManager:
    jobs_pool = None

    def __init__(self):
        self.jobs_pool = Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=1, decode_responses=True
        )

    def find_clusters(
        self,
        data: pd.DataFrame,
        clustering_method: str,
        clustering_hyperparams: dict,
        job_id: str,
    ):
        clusterizer = clusterizers[clustering_method]
        print(data.head())
        try:
            data_clustered = clusterizer(data, clustering_hyperparams)

            write_dataframe_to_s3(data_clustered, S3_BUCKET_RESULTS, job_id)
        except:
            update_job_status(self.jobs_pool, job_id, "failed (find_clusters)")
