# module to export results of simulation to a specified folder
"""
Export utilities for simulation run artifacts.

Author
------
Patrick Ortiz

Purpose
-------
Provide functions to persist simulation result DataFrames to a structured
directory on disk and to produce a manifest describing the run. The module
supports optional gzip compression for CSV outputs and writes a manifest.json
that lists table filenames and row counts for downstream ingestion or auditing.

Key Behavior / Conventions
--------------------------
- `export_tables()`:
    * Creates a run-specific directory under `BASE_DIR / data / <folder_pointer> / <scenario_id> / <safe_run_id>`.
    * Iterates a canonical table list and delegates per-table writes to `export_table`.
    * Accepts `compress_data` to toggle writing `.csv` vs `.csv.gz` files.

- `export_table()`:
    * Ensures destination directory exists before writing.
    * Writes fact tables in chunked mode (`chunksize=100_000`) to reduce memory usage.
    * Uses `pandas.DataFrame.to_csv()` with `compression='gzip'` when `compress=True`.
    * Catches and re-raises exceptions while printing diagnostics (target path + repr of exception).

- `write_manifest()`:
    * Builds a JSON manifest containing run metadata, a config summary, and per-table row counts and file names.
    * Inspects the run directory to prefer `.csv.gz` entries when present, otherwise falls back to `.csv`.
    * Writes `manifest.json` into the same run folder.

Notes / Conventions
-------------------
- File naming: the manifest records the actual filename written for each table (preferring `.csv.gz` when present).
- Time units: unrelated to file I/O, but other modules use seconds; filenames and manifest fields do not imply any unit conversion.
- Directory creation: functions call `Path.mkdir(parents=True, exist_ok=True)` before writing to avoid PermissionError when possible; ensure the process has OS write permission to `BASE_DIR/data/...`.
- Compression and chunking: chunked writes reduce peak memory but will still write a single compressed file per table when compression is enabled.
- Failure modes to check:
    * Permissions / locks (OneDrive, antivirus, or an editor holding files).
    * Path construction errors that produce unexpected filenames (ensure `file_path` is a path to a file, not a directory string).
    * Long/invalid Windows paths or invalid characters (safe names are produced via regex for run_id).
- Manifest consistency: manifest entries are derived from the same location where files are written; if you change where files are written update `write_manifest()` accordingly.

Usage
-----
Call `export_tables(tables, scenario_id, run_id, compress_data=False, folder_pointer='unspecified_runs')`
then `write_manifest(export_dir=BASE_DIR / 'data', run_id=run_id, scenario_id=scenario_id, run_specs=..., config=..., tables=..., random_seed=...)`.

"""

####################################################################
## Required Setup ##
####################################################################

import json
from math import e
import re
from datetime import datetime, timezone
from pathlib import Path
import pandas

BASE_DIR = Path(__file__).resolve().parent

####################################################################

def export_tables(tables: dict, scenario_id: str, run_id: str, compress_data: bool=False, folder_pointer='unspecificed_runs'):
    """
    Export the canonical set of simulation tables to a run-specific directory.

    Parameters
    ----------
    tables : dict
        Mapping of table name (str) -> pandas.DataFrame.
        Expected keys: "dim_scenario", "dim_machine", "dim_product", "dim_process",
        "dim_process_route", "fact_work_order", "fact_production_event",
        "fact_downtime_event", "fact_quality_event".
    scenario_id : str
        Logical scenario identifier used as a subfolder.
    scenario_tag : str
        Description identifiying the user provided scenario characteristics.
    run_id : str
        Run identifier (will be sanitized for filesystem use).
    compress_data : bool, optional
        If True, write files as gzipped CSV (`.csv.gz`). Default False.
    folder_pointer : str, optional
        Top-level folder under `BASE_DIR/data` where runs are stored.
    """
    # Build the run directory and ensure it exists
    MAIN_DIR = BASE_DIR / 'data' / folder_pointer
    safe_run_id = re.sub(r'[<>:"/\\|?*]', '-', run_id).strip().rstrip('.')
    base_path = MAIN_DIR / scenario_id / safe_run_id
    base_path.mkdir(parents=True, exist_ok=True)

    # Iterate the canonical table list and export each one
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
        export_table(table, tables[table], base_path, table_type=table.split('_')[0], compress=compress_data)
        print(f"Data export for {table} to archive was successful.\n")
    
    # Final confirmation with the absolute path used
    print(f"Data was exported to folder: {base_path}\n")

def export_table(table_name, df, path, table_type, compress=False):
    """
    Export a single DataFrame to CSV (optionally gzip-compressed).

    Parameters
    ----------
    table_name : str
        Logical table name used to build the filename.
    df : pandas.DataFrame
        DataFrame to write.
    path : pathlib.Path
        Directory where the file should be written. Directory will be created if missing.
    table_type : str
        'fact' or 'dim' (used to decide whether to chunk large tables).
    compress : bool, optional
        If True write gzipped CSV (`.csv.gz`), otherwise plain `.csv`.
    """
    # Print brief progress to the console
    print(f"Exporting {len(df):,} records from {table_name}...")
    # Ensure target directory exists (safe no-op if already present)
    path.mkdir(parents=True, exist_ok=True)
    
    # Determine filename and full path
    filename = f"{table_name}.csv.gz" if compress else f"{table_name}.csv"
    file_path = path / filename

    # Export with appropriate settings based on table type and compression, with error handling
    try:
        # For fact tables use chunked writes to reduce memory pressure
        if table_type == 'fact':
            chunk_size = 100_000
            if compress:
                # pandas handles streaming + compression
                df.to_csv(file_path, index=False, compression='gzip', chunksize=chunk_size)
                print(f"Exported {table_name} in chunks of {chunk_size} with compression.")
            else:
                df.to_csv(file_path, index=False, chunksize=chunk_size)
                print(f"Exported {table_name} in chunks of {chunk_size} without compression.")
        else:
            # Dimension tables are typically smaller (write in one shot)
            if compress:
                df.to_csv(file_path, index=False, compression='gzip')
                print(f"Exported {table_name} with compression.")
            else:
                df.to_csv(file_path, index=False)
                print(f"Exported {table_name} without compression.")
    except Exception as exc:
        # Provide diagnostic information and re-raise so calling code can handle and log
        print(f"Failed to export {table_name}. Target path: {file_path}")
        print("Exception:", repr(exc))
        raise

def write_manifest(
    export_dir,
    run_id,
    scenario_id,
    scenario_tag,
    run_specs,
    config,
    tables,
    random_seed="not_specified"
):
    """
    Create and write a manifest.json describing the run and exported files.

    The manifest includes:
      - run identifiers and creation timestamp
      - a small summary of the run_specs and config
      - per-table row counts and the actual filename present on disk

    The function inspects the expected run directory and prefers `.csv.gz`
    entries when present for each table; if neither `.csv.gz` nor `.csv` are
    found an explicit `file_not_found_<table>` placeholder is recorded.

    Parameters
    ----------
    export_dir : pathlib.Path
        Base export directory (typically `BASE_DIR / 'data'`).
    run_id : str
        Run identifier (unsanitized; will be sanitized internally).
    scenario_id : str
        Scenario identifier used as a subfolder.
    scenario_tag : str
        Scenario description used for identification.
    run_specs : dict
        Run specification dictionary (e.g., run_mode, num_machines, ...).
    config : dict
        Full configuration dictionary used for the run (used to populate summary).
    tables : dict
        Mapping of table name -> DataFrame used to populate row counts in the manifest.
    random_seed : str|int, optional
        Random seed value used for the run (default: "not_specified").

    Returns
    -------
    pathlib.Path
        The directory where the manifest was written.
    """
    # Base manifest structure with run metadata and a compact config summary
    manifest = {
        "run_id": run_id,
        "scenario_id": scenario_id,
        "scenario_name": scenario_tag,
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),

        "run_specs": {
            "run_mode": run_specs.get('run_mode'),
            "sim_horizon_days": run_specs.get('sim_horizon_days'),
            "num_machines": run_specs.get('num_machines'),
            "num_products": run_specs.get('num_products'),
            "num_work_orders": run_specs.get('num_work_orders'),
            "wip_limit": run_specs.get('wip_limit'),
            "wip_poll_interval": run_specs.get('wip_poll_interval')
        },

        random_seed: random_seed,

        "config_summary": {
            "cycle_time_distribution": config['cycle_times']['type'],
            "interarrival_distribution": config['work_order_interarrival']['type'],
            "failure_distribution": config['time_to_failure']['type'],
            "repair_distribution": config['repair_behavior']['type'],
            "quality_enabled": "quality" in config,
        },

        "tables": {}
    }

    # Sanitize run id and build manifest directory path (matches export_tables)
    safe_run_id = re.sub(r'[<>:"/\\|?*]', '-', run_id).strip().rstrip('.')
    manifest_dir = export_dir / scenario_id / safe_run_id 

    # For each table choose the filename that actually exists on disk
    for table_name, df in tables.items():
        csv_gz = manifest_dir / f"{table_name}.csv.gz"
        csv_plain = manifest_dir / f"{table_name}.csv"
        if csv_gz.exists():
            file_name = f"{table_name}.csv.gz"
        elif csv_plain.exists():
            file_name = f"{table_name}.csv"
        else:
            # Record that the expected file was not found
            file_name = f"file_not_found_{table_name}"

        # Populate manifest entry with rows and file name (relative file name)
        manifest["tables"][table_name] = {
            "rows": int(len(df)),
            "file": file_name
        }
    
    # Ensure directory exists and write manifest.json
    manifest_path = manifest_dir / "manifest.json"
    manifest_dir.mkdir(parents=True, exist_ok=True)

    # Write the manifest to disk with pretty formatting
    with open(manifest_path, "w", encoding="utf-8") as f:
         json.dump(manifest, f, indent=2)
    
    # Output
    return manifest_dir
    