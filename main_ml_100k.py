import collections

from data import load_ml_100k_data, print_rdd


def count_ratings(rdd):
    print("Count ratings")
    ratings = rdd.map(lambda x: x[2])
    rating_counts = ratings.countByValue()
    rating_counts = collections.OrderedDict(sorted(rating_counts.items()))
    for key, value in rating_counts.items():
        print("Rating: %s | Count: %i" % (key, value))


def main():
    rdd = load_ml_100k_data()
    print_rdd(rdd)
    print()

    # Get ratings count
    count_ratings(rdd)
    print()


if __name__ == "__main__":
    main()
