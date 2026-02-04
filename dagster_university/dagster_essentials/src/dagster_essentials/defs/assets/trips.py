# src/dagster_essentials/defs/assets/trips.py
from dagster_duckdb import DuckDBResource
import dagster as dg
import requests
from dagster_essentials.defs.assets import constants
from dagster_essentials.defs.partitions import monthly_partition

import duckdb
import os


@dg.asset(
    partitions_def=monthly_partition
)
def taxi_trips_file(context: dg.AssetExecutionContext) -> None:
    """
      The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """

    # Initially, we were downloading the raw files directly from the source system.
    #hardcoded logic for month = '2023-03'
    #month_to_fetch = '2023-03'

    #raw_trips = requests.get(
        #f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    #)
    #with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        #output_file.write(raw_trips.content)


    # In this option we will load the data directly into DuckDB from the source system without saving a local copy of the raw file.

    # get the partition key from the context
    partition_date_str = context.partition_key
    #context.partition_key supplies the materializing partition’s date as a string in the YYYY-MM-DD format
    # NYC OpenData source system, the taxi trip files are structured in a YYYY-MM format. 
    # So we only need the year and month portion of the string. We slice the string to make it match the format expected by our source system.
    month_to_fetch = partition_date_str[:-3]

    query = f"""
    create table if not exists trips (
      vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
      rate_code_id double, payment_type integer, dropoff_datetime timestamp,
      pickup_datetime timestamp, trip_distance double, passenger_count double,
      total_amount double, partition_date varchar
    );

    delete from trips where partition_date = '{month_to_fetch}';

    insert into trips
    select
      VendorID, PULocationID, DOLocationID, RatecodeID, payment_type, tpep_dropoff_datetime,
      tpep_pickup_datetime, trip_distance, passenger_count, total_amount, '{month_to_fetch}' as partition_date
    from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
  """
    
    with database.get_connection() as conn:
        conn.execute(query)

@dg.asset
def taxi_zones_file() -> None:
    """
      The raw parquet files for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    raw_trips = requests.get(
        f"https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_trips.content)

# src/dagster_essentials/defs/assets/trips.py
@dg.asset(
    deps=["taxi_trips_file"]
)
def taxi_trips(database: DuckDBResource) -> None: # added database resource as parameter
    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """
    query = """
        create or replace table trips as (
          select
            VendorID as vendor_id,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            RatecodeID as rate_code_id,
            payment_type as payment_type,
            tpep_dropoff_datetime as dropoff_datetime,
            tpep_pickup_datetime as pickup_datetime,
            trip_distance as trip_distance,
            passenger_count as passenger_count,
            total_amount as total_amount
          from 'data/raw/taxi_trips_2023-03.parquet'
        );
    """
    #   we no longer need to use the backoff function. The Dagster DuckDBResource handles this functionality for us.

#    conn = backoff(
#        fn=duckdb.connect,
#        retry_on=(RuntimeError, duckdb.IOException),
#        kwargs={
#            "database": os.getenv("DUCKDB_DATABASE"),
#        },
#        max_retries=10,
#    )

    with database.get_connection() as conn:
        conn.execute(query)


    # src/dagster_essentials/defs/assets/trips.py
@dg.asset(
    deps=["taxi_zones_file"]
)
def taxi_zones(database: DuckDBResource) -> None:
    """
      The raw taxi zones dataset, loaded into a DuckDB database
    """
    query = f"""
        create or replace table zones as (
          select
            LocationID as zone_id,
            borough,
            zone,    
            the_geom as geometry
          from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """

    with database.get_connection() as conn:
        conn.execute(query) 