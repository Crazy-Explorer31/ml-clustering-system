import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import pandas as pd

nltk_downloaded = False


def get_preprocessed_texts(df: pd.DataFrame):
    """В df должна быть колонка `text` с документами в виде строк"""

    global nltk_downloaded
    if not nltk_downloaded:
        nltk.download("punkt")
        nltk.download("wordnet")
        nltk.download("stopwords")
        nltk.download("punkt_tab")
        nltk_downloaded = True

    stop_words = set(
        stopwords.words("english")
        + ["ha", "wa", "say", "said"]
        + "one two study three may usings used result case group without".split(" ")
    )
    stop_words.update({"patient", "treatment"})
    lemmatizer = WordNetLemmatizer()

    def preprocess(text):
        text = list(filter(str.isalpha, word_tokenize(text.lower())))
        text = list(lemmatizer.lemmatize(word) for word in text)
        text = list(word for word in text if word not in stop_words)
        return " ".join(text)

    df["text"] = df.apply(lambda i: preprocess(i.text), axis=1)

    return df
