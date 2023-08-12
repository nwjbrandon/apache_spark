from pyspark.sql import functions as func
from pyspark.sql.functions import col

from data import load_customer_orders_data, print_df, print_rdd


def get_customer_spending(rdd):
    rdd_ = rdd.map(lambda x: (x[0], x[2]))
    spending = rdd_.reduceByKey(lambda x, y: x + y)
    spending = spending.map(lambda x: (x[1], x[0]))
    spending = spending.sortByKey(ascending=False)
    spending = spending.map(lambda x: (x[1], x[0]))
    for key, value in list(spending.collect())[:10]:
        print("Customer: %s | Spending: %i" % (key, value))


def get_customer_spending_spark(schema):
    total_by_customer = schema.groupBy("cust_id").agg(
        func.round(func.sum("amount_spent"), 2).alias("total_spent")
    )
    total_by_customer_sorted = total_by_customer.sort(col("total_spent").desc())
    total_by_customer_sorted.show()


def main():
    rdd = load_customer_orders_data()
    print_rdd(rdd)
    print()

    # Get customer spending
    get_customer_spending(rdd)
    print()

    schema = load_customer_orders_data(use_df=True)
    print_df(schema)
    print()

    # Get customer spending
    get_customer_spending_spark(schema)
    print()


if __name__ == "__main__":
    main()
