import collections
import re

from data import load_book_data, print_rdd


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


if __name__ == "__main__":
    main()
