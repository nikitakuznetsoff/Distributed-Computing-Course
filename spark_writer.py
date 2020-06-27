from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, SQLContext
import json


def text_to_lower(obj):
    str = ""
    for word in obj['text']:
        if word.isalpha():
            str += word.lower()
        else:
            str += " "
    obj['text'] = str
    return obj


def create_file(rdd):
    cont = SQLContext(rdd.context)
    if len(rdd.collect()) > 0:
        df = rdd.toDF()
        print(df.show(), '\n\n')
        df.write.mode('append').format('json')\
                .save("hdfs://localhost:9000/user/espero/files")


conf = SparkConf()
conf.setAppName("SparkVKAnalyser")

sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 2)
sess = SparkSession(sc)
data = ssc.socketTextStream("localhost", 8990)

records = data.map(json.loads).map(text_to_lower)

keywords = ['science', 'study', 'design', 'art', 'photo', 'computer', 'theory', 'business']
def keywords_filter(obj):
    for word in keywords:
        if word in obj['text']:
            return True
    return False

filtered_records = records.filter(keywords_filter)
filtered_records.foreachRDD(create_file)

ssc.start()
ssc.awaitTermination()
ssc.stop()
