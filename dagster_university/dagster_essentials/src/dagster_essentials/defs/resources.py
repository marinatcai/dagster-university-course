import dagster as dg

# src/dagster_essentials/defs/resources.py
from dagster_duckdb import DuckDBResource


database_resource = DuckDBResource(
    database=dg.EnvVar("DUCKDB_DATABASE")  # replaced with environment variable instead of hardcoded path> database="data/staging/data.duckdb"
)

@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={"database": database_resource}) # This tells Dagster how to map the resources to specific key names> database_resource just defined is mapped to the key name database.