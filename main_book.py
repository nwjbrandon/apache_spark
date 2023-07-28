import collections
import re
from data import load_book_data, print_rdd


def count_word_occurences(rdd):
    print("Get word occurences")
    rdd_ = rdd.flatMap(lambda x: x.split())
    word_counts = rdd_.countByValue()
    word_counts = collections.OrderedDict(sorted(word_counts.items()))
    for key, value in word_counts.items():
        key = key.encode("ascii", "ignore")
        print("Word: %s | Occurences: %i" % (key, value))

def count_word_occurences_after_preprocessing(rdd):
    print("Get word occurences after preprocessing")
    rdd_ = rdd.flatMap(lambda x: re.compile(r'\W+', re.UNICODE).split(x.lower()))
    word_counts = rdd_.countByValue()
    word_counts = collections.OrderedDict(sorted(word_counts.items()))
    for key, value in word_counts.items():
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

if __name__ == "__main__":
    main()
