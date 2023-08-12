import collections
import re

from pyspark.sql import functions as func
from pyspark.sql.functions import col

from data import load_book_data, print_df, print_rdd


def count_word_occurences(rdd):
    print("Get word occurences")
    rdd_ = rdd.flatMap(lambda x: x.split())
    word_counts = rdd_.countByValue()
    word_counts = collections.OrderedDict(sorted(word_counts.items()))
    for key, value in list(word_counts.items())[:10]:
        key = key.encode("ascii", "ignore")
        print("Word: %s | Occurences: %i" % (key, value))


def count_word_occurences_after_preprocessing(rdd):
    print("Get word occurences after preprocessing")
    rdd_ = rdd.flatMap(
        lambda x: re.compile(r"\W+", re.UNICODE).split(x.lower())
    )
    word_counts = rdd_.countByValue()
    word_counts = collections.OrderedDict(sorted(word_counts.items()))
    for key, value in list(word_counts.items())[:10]:
        key = key.encode("ascii", "ignore")
        print("Word: %s | Occurences: %i" % (key, value))


def count_word_occurences_after_preprocessing_and_sorting(rdd):
    print("Get sorted word occurences after preprocessing")
    rdd_ = rdd.flatMap(
        lambda x: re.compile(r"\W+", re.UNICODE).split(x.lower())
    )
    word_counts = rdd_.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    word_counts = (
        word_counts.map(lambda x: (x[1], x[0]))
        .sortByKey()
        .map(lambda x: (x[1], x[0]))
    )
    for key, value in list(word_counts.collect())[:10]:
        key = key.encode("ascii", "ignore")
        print("Word: %s | Occurences: %i" % (key, value))


def count_word_occurences_after_preprocessing_and_sorting_spark(schema):
    print("Get sorted word occurences after preprocessing")
    words = schema.select(
        func.explode(func.split(schema.value, "\\W+")).alias("word")
    )
    words.filter(words.word != "")
    lowercase_words = words.select(func.lower(words.word).alias("word"))
    word_counts = lowercase_words.groupBy("word").count()
    word_counts_sorted = word_counts.sort(col("count").desc())
    word_counts_sorted.show()


def main():
    rdd = load_book_data()
    print_rdd(rdd)
    print()

    # Get word occurences
    count_word_occurences(rdd)
    print()

    # Get word occurences after preprocessing
    count_word_occurences_after_preprocessing(rdd)
    print()

    # Get word occurences after preprocessing and sorting
    count_word_occurences_after_preprocessing_and_sorting(rdd)
    print()

    schema = load_book_data(use_df=True)
    print_df(schema)
    print()

    # Get word occurences after preprocessing and sorting with spark
    count_word_occurences_after_preprocessing_and_sorting_spark(schema)
    print()


if __name__ == "__main__":
    main()
