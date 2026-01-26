import dagster as dg

import matplotlib.pyplot as plt
import geopandas as gpd

import duckdb
import os

from dagster_essentials.defs.assets import constants


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