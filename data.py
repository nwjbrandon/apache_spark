from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)


def print_rdd(rdd, n=5):
    entries = rdd.take(n)
    for entry in entries:
        print(entry)


def load_ml_100k_data():
    # user id | item id | rating | timestamp
    lines = sc.textFile("data/ml-100k/u.data")
    rdd = lines.map(lambda x: x.split())
    return rdd
