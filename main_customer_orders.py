from data import load_customer_orders_data, print_rdd


def get_customer_spending(rdd):
    rdd_ = rdd.map(lambda x: (x[0], x[2]))
    spending = rdd_.reduceByKey(lambda x, y: x + y)
    spending = spending.map(lambda x: (x[1], x[0]))
    spending = spending.sortByKey(ascending=False)
    spending = spending.map(lambda x: (x[1], x[0]))
    for key, value in list(spending.collect())[:10]:
        print("Customer: %s | Spending: %i" % (key, value))


def main():
    rdd = load_customer_orders_data()
    print_rdd(rdd)
    print()

    # Get customer spending
    get_customer_spending(rdd)
    print()


if __name__ == "__main__":
    main()
