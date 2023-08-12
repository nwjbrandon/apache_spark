import collections

from pyspark.sql import functions as func

from data import load_ml_100k_data, print_df, print_rdd


def count_ratings(rdd):
    print("Count ratings")
    ratings = rdd.map(lambda x: x[2])
    rating_counts = ratings.countByValue()
    rating_counts = collections.OrderedDict(sorted(rating_counts.items()))
    for key, value in rating_counts.items():
        print("Rating: %s | Count: %i" % (key, value))


def get_top_k_popular_movies(schema, k=10):
    print("Get top k popular movies")
    top_movies = schema.groupBy("movieID").count().orderBy(func.desc("count"))
    top_movies.show(k)


def main():
    rdd = load_ml_100k_data()
    print_rdd(rdd)
    print()

    # Get ratings count
    count_ratings(rdd)
    print()

    schema = load_ml_100k_data(use_df=True)
    print_df(schema)
    print()

    # Get top k popular movies
    get_top_k_popular_movies(schema)


if __name__ == "__main__":
    main()
