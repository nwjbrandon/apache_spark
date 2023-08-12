import collections

from pyspark.sql import functions as func

from data import load_fakefriends_data, load_fakefriends_headers_data, print_df, print_rdd


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


def count_average_friends_by_age_spark(schema):
    df = (
        schema.groupBy("age")
        .agg(func.round(func.avg("friends"), 2).alias("friends_avg"))
        .sort("age")
    )
    df.show()


def main():
    rdd = load_fakefriends_data()
    print_rdd(rdd)
    print()

    # Get average friends by age
    count_average_friends_by_age(rdd)
    print()

    schema = load_fakefriends_data(use_df=True)
    print_df(schema)
    print()

    # Get average friends by age with spark
    count_average_friends_by_age_spark(schema)
    print()

    schema = load_fakefriends_headers_data(use_df=True)
    print_df(schema)
    print()

    # Get average friends by age with spark
    count_average_friends_by_age_spark(schema)
    print()


if __name__ == "__main__":
    main()
