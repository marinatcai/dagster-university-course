# src/dagster_essentials/defs/jobs.py
import dagster as dg
import dagster_essentials.defs.partitions as monthly_partition

# create a job named trips_by_week and use the AssetSelection utility to reference a single asset
trips_by_week = dg.AssetSelection.assets(["trips_by_week"])
weekly_update_job = dg.define_asset_job(
    name="weekly_update_job",
    partitions_def=monthly_partition, # partitions added here
    selection=trips_by_week,
)

# create a job named trip_update_job that selects all assets using AssetSelection.all() and then omit trips_by_week by subtracting its selection:
trip_update_job = dg.define_asset_job(
    name="trip_update_job",
    partitions_def=monthly_partition, # partitions added here
    selection=dg.AssetSelection.all() - trips_by_week
)
