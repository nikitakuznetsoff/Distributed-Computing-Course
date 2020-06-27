import glob
import os
from pathlib import Path
import json
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from time import process_time

# Word tokenization
def word_tokenize(word):
    import nltk
    word_lower = word.lower()
    return nltk.word_tokenize(word_lower)

# Removing Stop words
def remove_stop_words(data, spark=True):
    from nltk.corpus import stopwords
    stop_words = set(stopwords.words('english'))
    if spark:
        filtered_data = data.filter(lambda word : word[0] not in stop_words and word[0] != '')
        # filtered_data = data.filter(lambda word : word not in stop_words and word != '')
    else:
        filtered_data = list(filter(lambda word : word not in stop_words and word != '', data))
    return filtered_data

# Remove Punctuations from our data
def remove_punc(data, spark=True):
    import string
    puncts = list(string.punctuation)
    if spark:
        filtered_data = data.filter(lambda punct : punct not in puncts)
    else:
        filtered_data = list(filter(lambda punct : punct not in puncts, data))
    return filtered_data

# Lemmatization for Spark
def lemma(x):
    import nltk
    nltk.download('wordnet')
    from nltk.stem import WordNetLemmatizer
    lemmatizer = WordNetLemmatizer()
    return lemmatizer.lemmatize(x)

# Lemmatization for python methods
def lemma_python(words):
    import nltk
    nltk.download('wordnet')
    from nltk.stem import WordNetLemmatizer
    lemmatizer = WordNetLemmatizer()
    lem_words = list(map(lemmatizer.lemmatize, words))
    return lem_words


if __name__ == "__main__":

    list_of_files = glob.glob('media/*.txt')
    latest_file = max(list_of_files, key=os.path.getctime)

    f = open(latest_file, "r")
    str = f.read()
    f.close()

    sc = SparkContext()
    ssc = StreamingContext(sc, 1)

    data =  sc.textFile(latest_file)

    t1_spark = process_time()
    words = data.flatMap(word_tokenize)
    words_stop = remove_stop_words(words)
    words_punc = remove_punc(words_stop)
    lem_words = words_punc.map(lemma)
    t2_spark = process_time()
    print(lem_words.collect())


    t1_python = process_time()
    p_words = word_tokenize(str)
    p_words_stop = remove_stop_words(p_words, spark=False)
    p_words_punc = remove_punc(p_words_stop, spark=False)
    p_lem_words = lemma_python(p_words_punc)
    t2_python = process_time()
    print(p_lem_words)


    file_size = Path(latest_file).stat().st_size
    spark_time = t2_spark - t1_spark
    python_time = t2_python - t1_python


    str = {
        "python_time":python_time,
        "spark_time":spark_time,
        "file_size":file_size
    }
    f = open("media/ans_arg.txt", "w+")
    f.write(json.dumps(str))
    f.close()
