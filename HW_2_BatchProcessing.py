from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import datediff, to_date, lit, to_timestamp, col, hour


def join_look_up_with_cities(df, lookup_df):
    """
    Join the taxi dataset with the lookup table to enrich location data.
    :param df: Taxi dataframe
    :param lookup_df: Lookup dataframe
    :return: Joined dataframe
    """
    joined_df = df.join(
        lookup_df.withColumnRenamed("LocationID", "PU_LocationID"),
        df["PULocationID"] == col("PU_LocationID"),
        "left"
    ).withColumnRenamed("Zone", "PU_Zone") \
     .withColumnRenamed("Borough", "PU_Borough") \
     .drop("PU_LocationID") \
     .join(
        lookup_df.withColumnRenamed("LocationID", "DO_LocationID"),
        df["DOLocationID"] == col("DO_LocationID"),
        "left"
    ).withColumnRenamed("Zone", "DO_Zone") \
     .withColumnRenamed("Borough", "DO_Borough") \
     .drop("DO_LocationID")
    
    return joined_df



def clean_data(df):
    """
    Clean the taxi dataset by filtering out invalid or extreme values.
    :param df: Dataframe to be cleaned
    :return: Cleaned dataframe
    """
    cleaned_df = df.filter((col("fare_amount") > 0) &
                           (col("total_amount") > 0) &
                           (col("trip_distance") > 0) &
                           (col("trip_distance") < 1000) &
                           (col("passenger_count") > 0))
    return cleaned_df

def get_most_expensive_route(df):
    """
    Find the most expensive route (highest fare amount) for each dataset.
    :param df: Dataframe
    """
    most_expensive_route = df.orderBy(col("fare_amount").desc()).select(
        "PU_Zone", "DO_Zone", "fare_amount"
    ).first()

    print(f"Most Expensive Route: {most_expensive_route['PU_Zone']} to {most_expensive_route['DO_Zone']} - ${most_expensive_route['fare_amount']}")

def get_top_5_busiest_area(df):
    """
    Find the top 5 busiest pickup zones for each dataset.
    :param df: Dataframe
    """
    top_5_busiest_zones = df.groupBy("PU_Zone") \
        .count() \
        .orderBy(col("count").desc()) \
        .limit(5)
    
    print("Top 5 Busiest Pickup Zones:")
    top_5_busiest_zones.show()

def get_longest_trips(df):
    """
    Find the longest trip (by trip distance) in the dataset.
    :param df: Dataframe
    """
    longest_trip = df.orderBy(col("trip_distance").desc()).select(
        "PU_Zone", "DO_Zone", "trip_distance"
    ).first()
    
    print(f"Longest Trip: {longest_trip['PU_Zone']} to {longest_trip['DO_Zone']} - {longest_trip['trip_distance']} miles")

def get_crowded_places_per_hour(df, pickup_col, dropoff_col):
    """
    Find the most crowded Pickup and Drop-off zones for each hour.
    :param df: Dataframe
    :param pickup_col: Pickup datetime column
    :param dropoff_col: Drop-off datetime column
    """
    crowded_pickup = df.groupBy(hour(col(pickup_col)).alias("Hour"), "PU_Zone") \
        .count() \
        .orderBy(col("Hour"), col("count").desc())
    
    print("Most Crowded Pickup Zones Per Hour:")
    crowded_pickup.show(10, truncate=False)

    crowded_dropoff = df.groupBy(hour(col(dropoff_col)).alias("Hour"), "DO_Zone") \
        .count() \
        .orderBy(col("Hour"), col("count").desc())
    
    print("Most Crowded Drop-off Zones Per Hour:")
    crowded_dropoff.show(10, truncate=False)

def get_hourly_pickup_dropoff_counts(df, pickup_col, dropoff_col):
    """
    Calculate hourly Pickup and Drop-off counts and return the dataframes.
    :param df: Dataframe
    :param pickup_col: Pickup datetime column
    :param dropoff_col: Drop-off datetime column
    :return: Two dataframes for hourly pickup and drop-off counts
    """
    hourly_pickup = df.groupBy(hour(col(pickup_col)).alias("Hour")) \
        .count() \
        .withColumnRenamed("count", "Pickup_Count") \
        .orderBy("Hour")

    hourly_dropoff = df.groupBy(hour(col(dropoff_col)).alias("Hour")) \
        .count() \
        .withColumnRenamed("count", "Dropoff_Count") \
        .orderBy("Hour")
    
    return hourly_pickup, hourly_dropoff

def calculate_tip_correlations(df):
    """
    Calculate correlations between tip_amount and other numerical columns.
    :param df: Dataframe
    """
    numeric_columns = ["trip_distance", "fare_amount", "total_amount", "passenger_count"]
    correlations = {}
    
    for col_name in numeric_columns:
        correlation = df.stat.corr("tip_amount", col_name)
        correlations[col_name] = correlation
    
    print("Correlations with tip_amount:")
    for key, value in correlations.items():
        print(f"{key}: {value}")
    
    return correlations

# Visualization Functions

# Graph of Hourly Pickup and Drop-off Numbers

import matplotlib.pyplot as plt

def plot_hourly_counts(hourly_pickup, hourly_dropoff, title, output_path=None):
    """
    Plot hourly Pickup and Drop-off counts.
    :param hourly_pickup: Dataframe of hourly pickup counts
    :param hourly_dropoff: Dataframe of hourly drop-off counts
    :param title: Title for the plot
    :param output_path: Path to save the plot as an image (optional)
    """
    pickup_pd = hourly_pickup.toPandas()
    dropoff_pd = hourly_dropoff.toPandas()

    plt.figure(figsize=(12, 6))
    plt.plot(pickup_pd["Hour"], pickup_pd["Pickup_Count"], label="Pickup", marker="o")
    plt.plot(dropoff_pd["Hour"], dropoff_pd["Dropoff_Count"], label="Drop-off", marker="o")
    plt.title(title)
    plt.xlabel("Hour")
    plt.ylabel("Count")
    plt.legend()
    plt.grid(True)
    if output_path:
        plt.savefig(output_path)
    plt.show()

# Graph of Tip Correlations
def plot_correlations(correlations, title, output_path=None):
    """
    Plot correlations between tip_amount and other numerical columns.
    :param correlations: Dictionary of correlations
    :param title: Title for the plot
    :param output_path: Path to save the plot as an image (optional)
    """
    plt.figure(figsize=(10, 6))
    keys = list(correlations.keys())
    values = list(correlations.values())
    plt.bar(keys, values, color='skyblue')
    plt.title(title)
    plt.ylabel("Correlation")
    plt.grid(axis='y')
    if output_path:
        plt.savefig(output_path)
    plt.show()

    
if __name__ == '__main__':
    spark = SparkSession.builder.appName("TaxiAnalysis").getOrCreate()

    # Load data
    yellow_taxi_df = spark.read.parquet("yellow_tripdata_2021-03.parquet")
    green_taxi_df = spark.read.parquet("green_tripdata_2021-03.parquet")
    lookup_df = spark.read.csv("taxi+_zone_lookup.csv", header=True, inferSchema=True)
    
    # Join lookup data
    yellow_taxi_enriched = join_look_up_with_cities(yellow_taxi_df, lookup_df)
    green_taxi_enriched = join_look_up_with_cities(green_taxi_df, lookup_df)
    
    # Clean data
    yellow_taxi_cleaned = clean_data(yellow_taxi_enriched)
    green_taxi_cleaned = clean_data(green_taxi_enriched)
    
    # Analysis
    print("Yellow Taxi Most Expensive Route:")
    get_most_expensive_route(yellow_taxi_cleaned)
    
    print("Green Taxi Most Expensive Route:")
    get_most_expensive_route(green_taxi_cleaned)

    print("Yellow Taxi Top 5 Busiest Zones:")
    get_top_5_busiest_area(yellow_taxi_cleaned)
    
    print("Green Taxi Top 5 Busiest Zones:")
    get_top_5_busiest_area(green_taxi_cleaned)
    
    print("Yellow Taxi Longest Trip:")
    get_longest_trips(yellow_taxi_cleaned)
    
    print("Green Taxi Longest Trip:")
    get_longest_trips(green_taxi_cleaned)

    print("Yellow Taxi Crowded Places Per Hour:")
    get_crowded_places_per_hour(yellow_taxi_cleaned, "tpep_pickup_datetime", "tpep_dropoff_datetime")
    
    print("Green Taxi Crowded Places Per Hour:")
    get_crowded_places_per_hour(green_taxi_cleaned, "lpep_pickup_datetime", "lpep_dropoff_datetime")
    
    print("Yellow Taxi Hourly Pickup and Drop-off Counts:")
    yellow_hourly_pickup, yellow_hourly_dropoff = get_hourly_pickup_dropoff_counts(
        yellow_taxi_cleaned, "tpep_pickup_datetime", "tpep_dropoff_datetime"
    )
    yellow_hourly_pickup.show()
    yellow_hourly_dropoff.show()
    
    plot_hourly_counts(
        yellow_hourly_pickup,
        yellow_hourly_dropoff,
        "Yellow Taxi Hourly Counts",
        "yellow_taxi_hourly_counts.png"
    )    

    print("Green Taxi Hourly Pickup and Drop-off Counts:")
    green_hourly_pickup, green_hourly_dropoff = get_hourly_pickup_dropoff_counts(
        green_taxi_cleaned, "lpep_pickup_datetime", "lpep_dropoff_datetime"
    )
    green_hourly_pickup.show()
    green_hourly_dropoff.show()
    
    plot_hourly_counts(
        green_hourly_pickup,
        green_hourly_dropoff,
        "Green Taxi Hourly Counts",
        "green_taxi_hourly_counts.png"
    )

    
        # Tip Correlation Graphs
    print("Yellow Taxi Tip Correlations:")
    yellow_tip_correlations = calculate_tip_correlations(yellow_taxi_cleaned)
    plot_correlations(
        yellow_tip_correlations,
        "Yellow Taxi Tip Correlations",
        "yellow_taxi_tip_correlations.png"
    )
    
    print("Green Taxi Tip Correlations:")
    green_tip_correlations = calculate_tip_correlations(green_taxi_cleaned)
    plot_correlations(
        green_tip_correlations,
        "Green Taxi Tip Correlations",
        "green_taxi_tip_correlations.png"
    )
    
