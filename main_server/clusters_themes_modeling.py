
from gensim import corpora
from gensim.models import LdaModel
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS

# import pyLDAvis.gensim_models as gensimvis
# import pyLDAvis

import warnings

warnings.filterwarnings("ignore")


def get_lda_model(documents, theme_length):
    stop_words = set(STOPWORDS)
    texts = [
        [word for word in simple_preprocess(doc) if word not in stop_words]
        for doc in documents
    ]
    dictionary = corpora.Dictionary(texts)
    corpus = [dictionary.doc2bow(text) for text in texts]

    # Обучение модели LDA
    lda_model = LdaModel(corpus, num_topics=theme_length, id2word=dictionary, passes=15)

    return lda_model


def get_cluster_theme(lda_model, theme_length):
    cluster_theme = []
    for i in range(theme_length):
        topics = lda_model.show_topic(i)
        topic_max = max(topics, key=lambda x: x[1])
        cluster_theme.append(topic_max[0])
    cluster_theme = list(set(cluster_theme))
    return "_".join(cluster_theme)


def get_clusters_themes_helper(df_splitted, clusters_count, theme_length):
    clusters_themes = []
    for i, df_splitted_part in enumerate(df_splitted):
        lda_model = get_lda_model(df_splitted_part, theme_length)
        cluster_theme = get_cluster_theme(lda_model, theme_length)
        clusters_themes.append(cluster_theme)
    return clusters_themes


def get_clusters_themes(df_with_texts, clusters_count, theme_length):
    df_splitted = [
        df_with_texts[df_with_texts["cluster"] == i].reset_index()["text"].tolist()
        for i in range(clusters_count)
    ]

    return get_clusters_themes_helper(df_splitted, clusters_count, theme_length)
