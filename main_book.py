import collections

from data import load_book_data, print_rdd


def count_word_occurences(rdd):
    print("Get word occurences")
    rdd_ = rdd.flatMap(lambda x: x.split())
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


if __name__ == "__main__":
    main()
