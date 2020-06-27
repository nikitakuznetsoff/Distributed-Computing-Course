from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import json


conf = SparkConf()
sc = SparkContext(conf=conf)
sess = SparkSession(sc)

data = sc.textFile("hdfs://localhost:9000/user/espero/files/part*").map(json.loads)

# WORDS
texts = data.map(lambda publication: publication['text'])
words = texts.flatMap(lambda x: x.split(" "))\
             .map(lambda x: (x, 1))
counts = words.reduceByKey(lambda a, b: a + b)
counts_arr = counts.collect()
counts_arr_sort = sorted(counts_arr, key=lambda count: count[1], reverse=True)

if counts_arr_sort[0][0] == '':
    counts_arr_sort = counts_arr_sort[1:]

publications_count = texts.count()
all_words_count = words.count()
different_words_count = counts.count()

# LIKES
likes = data.map(lambda publication: publication['likes']['count']).sum()
likes_mean = likes / publications_count

# COMMENTS
comments = data.map(lambda publication: publication['comments']['count']).sum()
comments_mean = comments / publications_count

# REPOSTS
reposts = data.map(lambda publication: publication['reposts']['count']).sum()
reposts_mean = reposts / publications_count

# VIEWS
def views_filter(dict):
    if 'views' not in dict:
        return False
    return True

views = data.filter(views_filter)
views = views.map(lambda publication: publication['views']['count']).sum()
views_mean = views / publications_count


print("PUBLICATIONS COUNT: ", publications_count)
print("ALL WORDS COUNT: ", all_words_count)
print("DIFFERENT WORDS COUNT: ", different_words_count, "\n")

print("TOP-10 WORDS: ")
for word in counts_arr_sort[:10]:
    print(word[0], end=' ')

print("\n\nMEAN LIKES COUNT: ", round(likes_mean, 2))
print("MEAN COMMENTS COUNT: ", round(comments_mean, 2))
print("MEAN REPOSTS COUNT: ", round(reposts_mean, 2))
print("MEAN VIEWS COUNT: ", round(views_mean, 2))
