from pyspark import SparkConf, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import FloatType, IntegerType, LongType, StringType, StructField, StructType

conf = SparkConf().setMaster("local").setAppName("MySpark")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName("MySpark").getOrCreate()


def print_df(df, n=5):
    df.show(n)


def print_rdd(rdd, n=5):
    entries = rdd.take(n)
    for entry in entries:
        print(entry)


def load_ml_100k_data(use_df=False):
    # user id | item id | rating | timestamp
    if use_df:
        schema = StructType(
            [
                StructField("userID", IntegerType(), True),
                StructField("movieID", IntegerType(), True),
                StructField("rating", IntegerType(), True),
                StructField("timestamp", LongType(), True),
            ]
        )
        return (
            spark.read.option("sep", "\t")
            .schema(schema)
            .csv("data/ml-100k/u.data")
        )
    else:
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
                friends=int(x[3]),
            )
        )
        schema = spark.createDataFrame(rdd).cache()
        schema.createOrReplaceTempView("fakefriends")
        return schema
    else:
        lines = sc.textFile("data/fakefriends.csv")
        rdd = lines.map(lambda x: x.split(","))
        return rdd


def load_fakefriends_headers_data(use_df=False):
    # user id | name | age | number of friends
    if use_df:
        schema = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv("data/fakefriends-header.csv")
        )
        return schema
    else:
        raise NotImplementedError


def load_temperature_1800_data(use_df=False):
    # station id | time | temperature type | temperature
    if use_df:
        schema = StructType(
            [
                StructField("stationID", StringType(), True),
                StructField("date", IntegerType(), True),
                StructField("measure_type", StringType(), True),
                StructField("temperature", FloatType(), True),
            ]
        )
        return spark.read.schema(schema).csv("data/1800.csv")
    else:
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


def load_book_data(use_df=False):
    # sentences
    if use_df:
        schema = spark.read.text("data/book.txt")
        return schema
    else:
        rdd = sc.textFile("data/book.txt")
        return rdd


def load_customer_orders_data(use_df=False):
    if use_df:
        schema = StructType(
            [
                StructField("cust_id", IntegerType(), True),
                StructField("item_id", IntegerType(), True),
                StructField("amount_spent", FloatType(), True),
            ]
        )
        return spark.read.schema(schema).csv("data/customer-orders.csv")
    else:
        lines = sc.textFile("data/customer-orders.csv")
        rdd = lines.map(lambda x: x.split(","))
        rdd = rdd.map(lambda x: (x[0], x[1], float(x[2])))
        return rdd
