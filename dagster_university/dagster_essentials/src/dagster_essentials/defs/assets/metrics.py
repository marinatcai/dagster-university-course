from datetime import datetime, timedelta

import dagster as dg

import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd

import duckdb
import os

from dagster_essentials.defs.assets import constants
from dagster._utils.backoff import backoff


@dg.asset
def metrics(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...


@dg.asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats() -> None:
    """
      Generate and save some basic statistics and visualizations for Manhattan taxi trips
    """
    # Query to get trip counts by zone
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """
    #Executes that query against the same DuckDB database that you ingested data into in the other assets.
    conn = duckdb.connect(os.getenv("DUCKDB_DATABASE"))
    
    #  Stores as a regular pandas DataFrame, not GeoPandas!!
    trips_by_zone = conn.execute(query).fetch_df()

    # Use GeoPandas to turn messy coordinates into GeoPandas format: 

    # Converts ONLY the geometry column from text format (WKT string like "POLYGON((x1 y1, x2 y2...))") into actual geographic shape objects that GeoPandas understands
    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    
    # converts the pandas DataFrame into a GeoDataFrame.
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    # Opens (or creates) a file at the path specified in constants.MANHATTAN_STATS_FILE_PATH and writes the trip statistics as JSON to that file.
    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())


# src/dagster_essentials/defs/assets/metrics.py
@dg.asset(
    deps=["manhattan_stats"],
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black")
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim(-74.05, -73.90)  # Adjust longitude range
    ax.set_ylim(40.70, 40.82)  # Adjust latitude range
    
    # Save the image
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)

@dg.asset(
    deps=["taxi_trips"]
)
def trips_by_week() -> None:
    """
      Generate and save some basic statistics for weekly taxi trips as CSV file 
    """
   # retry connecting to DuckDB up to 10 times
    conn = backoff(
        fn=duckdb.connect,
        retry_on=(RuntimeError, duckdb.IOException),
        kwargs={
            "database": os.getenv("DUCKDB_DATABASE"),
        },
        max_retries=10,
    )
    # Date Range Setup
    current_date = datetime.strptime("2023-03-01", constants.DATE_FORMAT)
    end_date = datetime.strptime("2023-04-01", constants.DATE_FORMAT)

    # Initialize an empty DataFrame to hold results
    result = pd.DataFrame()
    
    # Loop through each week in the date range
    while current_date < end_date:
        # Format the current date as a string to match SQL date format
        current_date_str = current_date.strftime(constants.DATE_FORMAT)
        # SQL Query to aggregate data for the current week
        query = f"""
            select
                vendor_id, total_amount, trip_distance, passenger_count
            from trips
            where date_trunc('week', pickup_datetime) = date_trunc('week', '{current_date_str}'::date)
        """
        # fetch the data for the week as dataframe
        data_for_week = conn.execute(query).fetch_df()

        #  Perform aggregation
        aggregate = data_for_week.agg({
            "vendor_id": "count",
            "total_amount": "sum",
            "trip_distance": "sum",
            "passenger_count": "sum"
        }).rename({"vendor_id": "num_trips"}).to_frame().T # type: ignore

        # Add week's start date as period column
        aggregate["period"] = current_date
        # Append this week's data to results
        result = pd.concat([result, aggregate])

        #Move to next week (+7 days)
        current_date += timedelta(days=7)

    # clean up the formatting of the dataframe
    result['num_trips'] = result['num_trips'].astype(int)
    result['passenger_count'] = result['passenger_count'].astype(int)
    result['total_amount'] = result['total_amount'].round(2).astype(float)
    result['trip_distance'] = result['trip_distance'].round(2).astype(float)
    result = result[["period", "num_trips", "total_amount", "trip_distance", "passenger_count"]]
    result = result.sort_values(by="period")

    # Save the result as CSV: Writes final weekly summary to file path from constants
    result.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)