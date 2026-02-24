# module to export results of simulation to a PostgreSQL database
"""
Postgres loader utilities for the Production Plant Simulation.

Author
------
Patrick Ortiz

Purpose
-------
Provide small helpers to persist simulation result DataFrames into a
PostgreSQL database using SQLAlchemy. These helpers are used by the
simulation export/load workflow to move generated tables into a database
schema for further analytics.

Key details / conventions
-------------------------
- Database connection parameters are read from environment variables:
  `PG_USER`, `PG_PASSWORD`, `PG_HOST`, `PG_PORT`, `PG_DATABASE`.
  These are typically set via the project's `.env` file and loaded with
  `python-dotenv`.
- The connection engine uses the `psycopg2` dialect via SQLAlchemy.
- `load_dataframe_to_postgres` uses `DataFrame.to_sql()` with:
    - `if_exists` behavior controlled by the `exists_action` parameter.
    - `method='multi'` and `chunksize=1000` to perform batched multi-row
      INSERTs where possible.
- `load_run_to_postgres` provides a convenience routine to load the
  canonical set of simulation tables into a chosen schema (default:
  `"analytics"`).

Notes
-----
- These helpers do not perform transactional grouping across multiple
  tables; if atomic multi-table behavior is required wrap calls in an
  explicit transaction using SQLAlchemy connection/transaction primitives.
- Ensure the configured DB user has permission to create/append to the
  target tables and to write to the target schema.
"""

####################################################################
## Required Setup ##
####################################################################

from dotenv import load_dotenv
from sqlalchemy import create_engine
import os
####################################################################

# Load environment variables from .env file (if present) to configure database connection
load_dotenv()

# Build SQLAlchemy engine from environment variables.
# Expected env vars: PG_USER, PG_PASSWORD, PG_HOST, PG_PORT, PG_DATABASE
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}"
)

def load_dataframe_to_postgres(df, table_name, schema_name, exists_action="append"):
    """
    Load a pandas DataFrame to a PostgreSQL table.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to persist to the database.
    table_name : str
        Target table name in the database.
    schema_name : str
        Target schema name (e.g., "analytics").
    exists_action : str, optional
        Behavior when the target table exists. Passed to `if_exists` in
        `DataFrame.to_sql`. Common values: "append" (default), "replace",
        "fail".

    Behavior
    --------
    Uses `pandas.DataFrame.to_sql()` with:
      - SQLAlchemy engine created at module import time
      - `index=False` to avoid writing the DataFrame index as a column
      - `method='multi'` and `chunksize=1000` to improve insert throughput
    """
    print(f"Loading {table_name} to PostgreSQL, table contains {len(df)} records\n")

    df.to_sql(
        table_name, 
        engine,
        schema = schema_name,
        if_exists=exists_action, 
        index=False,
        method='multi',
        chunksize=1000
    )

def load_run_to_postgres(tables, schema_selection="testing"):
    """
    Convenience helper to load a standard set of simulation tables.

    Parameters
    ----------
    tables : dict
        Mapping of table name (str) -> pandas.DataFrame. Expected keys:
        "dim_scenario", "dim_machine", "dim_product", "dim_process", "dim_process_route",
        "fact_work_order", "fact_production_event", "fact_downtime_event",
        "fact_quality_event".
    schema_selection : str, optional
        Target schema name in the database (default: "testing").

    Notes
    -----
    - This function delegates to `load_dataframe_to_postgres` for each
      table. It currently uses the default `exists_action="append"`.
    - If you need to change behavior per-table (e.g., replace dimension
      tables), call `load_dataframe_to_postgres` individually with the
      desired `exists_action`.
    """
    for table in [
        "dim_scenario",
        "dim_machine",
        "dim_product",
        "dim_process",
        "dim_process_route",
        "fact_work_order",
        "fact_production_event",
        "fact_downtime_event",
        "fact_quality_event"
    ]:
        load_dataframe_to_postgres(tables[table], table, schema_name=schema_selection)



