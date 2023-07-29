from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession

conf = SparkConf().setMaster("local").setAppName("MySpark")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName("MySpark").getOrCreate()


def print_df(df, n=5):
    df.show(n)


def print_rdd(rdd, n=5):
    entries = rdd.take(n)
    for entry in entries:
        print(entry)


def load_ml_100k_data():
    # user id | item id | rating | timestamp
    lines = sc.textFile("data/ml-100k/u.data")
    rdd = lines.map(lambda x: x.split())
    return rdd


def load_fakefriends_data(use_df=False):
    # user id | name | age | number of friends
    if use_df:
        lines = spark.sparkContext.textFile("data/fakefriends.csv")
        rdd = lines.map(lambda x: x.split(","))
        rdd = rdd.map(
            lambda x: Row(
                user_id=int(x[0]),
                name=str(x[1].encode("utf-8")),
                age=int(x[2]),
                n_friends=int(x[3]),
            )
        )
        schema = spark.createDataFrame(rdd).cache()
        schema.createOrReplaceTempView("fakefriends")
        return schema
    else:
        lines = sc.textFile("data/fakefriends.csv")
        rdd = lines.map(lambda x: x.split(","))
        return rdd


def load_temperature_1800_data():
    # station id | time | temperature type | temperature
    lines = sc.textFile("data/1800.csv")
    rdd = lines.map(lambda x: x.split(","))
    rdd = rdd.map(
        lambda x: (
            x[0],
            int(x[1]),
            x[2],
            float(x[3]) * 0.1 * (9.0 / 5.0) + 32.0,
        )
    )
    return rdd


def load_book_data():
    # sentences
    rdd = sc.textFile("data/book.txt")
    return rdd


def load_customer_orders_data():
    lines = sc.textFile("data/customer-orders.csv")
    rdd = lines.map(lambda x: x.split(","))
    rdd = rdd.map(lambda x: (x[0], x[1], float(x[2])))
    return rdd
