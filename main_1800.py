import collections

from pyspark.sql import functions as func

from data import load_temperature_1800_data, print_df, print_rdd


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


def get_max_temperature_by_stations(rdd):
    print("Get maximum temperature by stations")
    min_temperatures = rdd.filter(lambda x: "TMAX" in x[2])
    min_temperatures = min_temperatures.map(lambda x: (x[0], x[3]))
    min_temperatures = min_temperatures.reduceByKey(lambda x, y: max(x, y))
    min_temperatures = collections.OrderedDict(
        sorted(min_temperatures.collect())
    )
    for key, value in min_temperatures.items():
        print("Station: %s | Max Temperature: %f" % (key, value))


def get_min_temperature_by_stations_spark(schema):
    print("Get minimum temperature by stations")
    min_temps = schema.filter(schema.measure_type == "TMIN")
    station_temps = min_temps.select("stationID", "temperature")
    min_temps_by_station = station_temps.groupBy("stationID").min("temperature")
    min_temps_by_station_f = (
        min_temps_by_station.withColumn(
            "temperature",
            func.round(
                func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2
            ),
        )
        .select("stationID", "temperature")
        .sort("temperature")
    )
    min_temps_by_station_f.show()


def main():
    rdd = load_temperature_1800_data()
    print_rdd(rdd)
    print()

    # Get minimum temperatue by station
    get_min_temperature_by_stations(rdd)
    print()

    # Get maximum temperatue by station
    get_max_temperature_by_stations(rdd)
    print()

    schema = load_temperature_1800_data(use_df=True)
    print_df(schema)
    print()

    # Get minimum temperatue by station
    get_min_temperature_by_stations_spark(schema)
    print()


if __name__ == "__main__":
    main()
