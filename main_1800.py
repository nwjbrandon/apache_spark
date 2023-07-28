import collections

from data import load_temperature_1800_data, print_rdd


def get_min_temperature_by_stations(rdd):
    print("Get minimum temperature by stations")
    min_temperatures = rdd.filter(lambda x: "TMIN" in x[2])
    min_temperatures = min_temperatures.map(lambda x: (x[0], x[3]))
    min_temperatures = min_temperatures.reduceByKey(lambda x, y: min(x, y))
    min_temperatures = collections.OrderedDict(
        sorted(min_temperatures.collect())
    )
    for key, value in min_temperatures.items():
        print("Station: %s | Min Temperature: %f" % (key, value))


def main():
    rdd = load_temperature_1800_data()
    print_rdd(rdd)
    print()

    # Get minimum temperatue by station
    get_min_temperature_by_stations(rdd)
    print()


if __name__ == "__main__":
    main()
