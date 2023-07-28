import collections

from data import load_fakefriends_data, print_rdd


def count_average_friends_by_age(rdd):
    print("Count average friends by age")
    rdd_ = rdd.map(lambda x: (int(x[2]), int(x[3])))
    totals_by_age = rdd_.mapValues(lambda x: (x, 1)).reduceByKey(
        lambda x, y: (x[0] + y[0], x[1] + y[1])
    )
    avg_by_age = totals_by_age.mapValues(lambda x: x[0] / x[1])
    avg_by_age = collections.OrderedDict(sorted(avg_by_age.collect()))
    for key, value in avg_by_age.items():
        print("Age: %s | Friends: %i" % (key, value))


def main():
    rdd = load_fakefriends_data()
    print_rdd(rdd)
    print()

    # Get average friends by age
    count_average_friends_by_age(rdd)
    print()


if __name__ == "__main__":
    main()
