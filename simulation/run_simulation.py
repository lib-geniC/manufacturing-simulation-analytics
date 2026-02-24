# Primary entry point for the Manufacturing Plant Simulation
"""
Run script for the Production Line KPI simulation.

Author
------
Patrick Ortiz

Purpose
-------
Drive an interactive simulation run using the project's data-generation and
SimPy-based Plant model. This script collects run parameters from the user,
configures RNGs and simulation settings, instantiates the simulation Plant,
runs the simulation, collects results, exports artifacts to disk, writes a
manifest, and (optionally) loads results into PostgreSQL.

Key Behavior / Organization
---------------------------
- Interactive prompts collect run_mode (time or volume), machine/product counts,
  optional random seed, and export folder.
- RNGs are derived from a master RNG to produce independent streams for
  different aspects of the simulation (arrival, processing, failure, quality,
  structure).
- The `config` dictionary centralizes stochastic settings used throughout the
  codebase and is passed into the `Plant` constructor.
- Results returned from the Plant are exported via `export_to_folder` and a
  manifest is written describing the produced files.
- PostgreSQL load is available via `load_to_postgres` but is commented out by
  default to avoid accidental writes during testing.

Notes / Conventions
-------------------
- Time units: the simulation uses seconds internally (where documented). The
  user-facing `sim_horizon_days` value is stored in days; caller code must
  convert to seconds if needed by the Plant implementation.
- Input conversion: helper functions from `helper_functions` (e.g. `safe_int`,
  `safe_float`) are used to validate conversions from raw `input()` values.
- Filenames: export folder names and run IDs are sanitized to remove characters
  invalid for file systems.
- If you change `export_to_folder` behavior, ensure `write_manifest()` remains
  consistent with the produced filenames.
"""
####################################################################
## Required Setup ##
####################################################################

from pathlib import Path

import pandas
import numpy as np
import export_to_folder as etf
import load_to_postgres as ltp
from datetime import datetime, timezone
import numpy as np
import re
import helper_functions as hf

####################################################################

def main():
    """
    Interactive entry point for running a single simulation.

    Behavior
    --------
    - Prompts the user for an action to start or exit.
    - Collects run configuration (run mode, horizon/work-order volume, machine/product counts,
      scenario id, optional random seed, and export folder).
    - Builds RNG streams, a `config` dictionary, instantiates the SimPy environment and Plant,
      executes the simulation, and exports results.
    - Writes a manifest describing the exported files and (optionally) loads data to PostgreSQL.

    Caution
    -------
    - The function performs many user-facing conversions (string -> int/float). Invalid inputs
      will raise `ValueError` from the `hf.safe_int`/`hf.safe_float` helpers. Those helpers
      provide clearer error messages than direct casts.
    """
    print('Manufacturing Plant Simulation has started. Before starting, ensure all necessary sql scripts have been run for PostgreSQL database and schema initialization.\n')
    
    start_bool = input('Type Y to continue or N to exit: ')

    if start_bool.lower() == 'y':
        print('\nPlease enter the following information to begin...\n')
        import simpy
        from Plant import Plant

        counter = 0
        # Ask user to pick a run mode: time-driven or volume-driven
        while True:
            run_mode_spec = input('Select a Run Mode (time/volume): ')
            if run_mode_spec.lower() == 'time':
                sim_horizon_spec = input('Enter Simulation Horizon in Days (e.g. 1 for 24 hours): ')
                work_order_volume_spec = None
                break
            elif run_mode_spec.lower() == 'volume':
                work_order_volume_spec = input('Enter Number of Work Orders to Process (e.g. 1000): ')
                sim_horizon_spec = None
                break
            else:
                # Allow a few attempts before aborting
                print("Run mode not recognized. Please try again.\n")
                counter += 1
                if counter >= 3:
                    print("Multiple invalid attempts detected. Exiting simulation setup.\n")
                    return
                continue

        # Collect counts and identifiers
        machine_count_spec = input('Enter Number of Machines in Plant (e.g. 10): ')
        product_count_spec = input('Enter Number of Products on Plant Portfolio (e.g. 10): ')

        # Collect work order limits
        wip_spec = input('\nOptional - Max concurrent Work-In-Progress (enter for no limit): ')
        poll_interval_spec = input('Optional - WIP poll interval in seconds (enter for default 60): ')

        scenario_val = input('\nEnter Scenario ID (e.g. SS1010TD50WSPS5060SE67R1): ')
        scenario_name = input('Enter Scenario Name (e.g. Small_Scale_Time_Driven): ')
        # run_id includes scenario and an ISO timestamp (UTC)
        run_val = f"{scenario_val}_{datetime.now(timezone.utc).isoformat()}"
        seed_check = input('\nInclude a Random Seed? (Y/N): ')
        if seed_check.lower() == 'y':
            seed_val = int(input('Enter Random Seed: '))
            master_rng = np.random.default_rng(seed_val)
        elif seed_check.lower() == 'n':
            seed_val = None
            master_rng = np.random.default_rng()
        else:
            print("Invalid response given, seed will be set to none")
            seed_val = None
            master_rng = np.random.default_rng()

        # Request export folder and sanitize; default to 'unspecified_runs' if blank
        while True:
            folder_spec = input('\nSpecify folder for data export (or press Enter for default "unspecified_runs"): ')

            if folder_spec.strip() == '':
                print("Specified folder is empty. Using default 'unspecified_runs'.\n")
                folder_spec = 'unspecified_runs'
                break
            else:
                # Replace characters invalid on Windows/other filesystems with underscore
                safe_folder_spec = re.sub(r'[<>:"/\\|?*]', '_', folder_spec).strip().rstrip('.')
                print(f"Data export folder set to: {safe_folder_spec}\n")
                folder_spec = safe_folder_spec
                break

        # Request export folder and sanitize; default to 'unspecified_runs' if blank
        while True:
            schema_spec = input('\nSpecify schema name for database import (or press Enter for default "testing"): ')

            if schema_spec.strip() == '':
                print("Specified schema is empty. Using default 'testing'.\n")
                schema_spec = 'testing'
                break
            else:
                # Replace characters invalid on Windows/other filesystems with underscore
                print(f"Data export folder set to: {schema_spec}\n")
                break

        print("Time scale for simulation has been initialized in seconds.\n")
        print("----------------------------------------------------------------------------------------------------------------------\n\n")
        print("Setting up configuration values for run...")

        # Create independent RNG streams seeded from the master RNG for reproducibility
        rngs = {
            'arrival': np.random.default_rng(master_rng.integers(1e9)),
            'processing': np.random.default_rng(master_rng.integers(1e9)),
            'failure': np.random.default_rng(master_rng.integers(1e9)),
            'quality': np.random.default_rng(master_rng.integers(1e9)),
            'structure': np.random.default_rng(master_rng.integers(1e9))
        }

        # Configuration dictionary passed into Plant - keep units documented (seconds/minutes)
        config = {
            "cycle_times": {
            "type": "discrete_uniform",
            "time_range": (600, 5400),  # seconds
        },
        "process_noise": {
            "type": "normal clipped",
            "mean_val": 1.0,
            "var_val": 0.1,
            "min_val": 0.85,
            "max_val": 1.20,
        },
        "time_to_failure": {
            "type": "exponential",
            "low_range": 14400,   # seconds
            "high_range": 288000, # seconds
        },
        "repair_behavior": {
            "type": "lognormal_clipped",
            "mean_val": 30,    # minutes
            "var_val": 0.6,
            "min_bound": 300,   # seconds
            "max_bound": 86400, # seconds
        },
        "product_family_weights": {
            "Logic Weight": 0.4,
            "Memory Wight": 0.3,
            "Analog Weight": 0.2,
            "Power Weight": 0.1,
        },
        "step_bounds": {
            "type": "discrete_uniform",
            "min_steps": 5,
            "max_steps": 9,
        },
        "work_order_interarrival": {
            "type": "gamma",
            "shape": 3.0,
            "scale": 400.0, # mean = shape*scale
        },
        "batch_sizes": {
            "type": "poisson_clipped",
            "lambda": 5,
            "min_val": 1,
            "max_val": 12,
        },
        "quality": {
            "interrupt_penalty": 0.02,
            "min_yield": 0.85,
        },
            'run_specs':{
                # Use helper conversions to validate user-provided inputs
                'run_mode': run_mode_spec.lower(),
                'num_machines': hf.safe_int(machine_count_spec),
                'num_products': hf.safe_int(product_count_spec),
                'num_work_orders': hf.safe_int(work_order_volume_spec),
                'wip_limit': 0 if wip_spec == '' else hf.safe_int(wip_spec),
                'wip_poll_interval': 60 if poll_interval_spec == '' else hf.safe_int(poll_interval_spec),
                'sim_horizon_days': hf.safe_float(sim_horizon_spec) #days
            }
        }

        print('Configuration setup complete.\n\n Initializing simulation environment...')

        # Instantiate SimPy environment and Plant model
        env = simpy.Environment()
        plant = Plant(env, scenario_val, scenario_name, config, rngs)

        print('Simulation environment initialized.\n') 
        print('----------------------------------------------------------------------------------------------------------------------\n\n')
        print('Beginning simulation run:\n\n')
        env.process(plant.run())

        # Run the environment until the Plant sets `done`
        env.run(until=plant.done)
        print('Simulation run complete.\n Collecting results...')

        results = plant.collect_results()

        print(f'Results collected.\n')
        print('----------------------------------------------------------------------------------------------------------------------\n\n')
       
        print('Results tranfer initiating...\n')

        # Unpack results into the canonical tables expected by exporters/loaders
        dim_machine = results[0]
        dim_product = results[1]
        dim_process = results[2]
        dim_process_route = results[3]
        fact_work_order = results[4]
        fact_production_event = results[5]
        fact_downtime_event = results[6]
        fact_quality_event = results[7]
        dim_scenario = results[8]

        tables = {
            "dim_scenario": dim_scenario,
            "dim_machine": dim_machine,
            "dim_product": dim_product,
            "dim_process": dim_process,
            "dim_process_route": dim_process_route,
            "fact_work_order": fact_work_order,
            "fact_production_event": fact_production_event,
            "fact_downtime_event": fact_downtime_event,
            "fact_quality_event": fact_quality_event
        }

        # Export DataFrames to disk
        print('Exporting data to specified folder...\n')
        etf.export_tables(tables, scenario_id=scenario_val, run_id=run_val, folder_pointer=folder_spec)
        
        # Build export_dir base used by write_manifest (must match export_tables layout)
        export_dir = Path(__file__).resolve().parent / 'data' / folder_spec
        
        # Write manifest that records filenames and row counts
        print('Writing manifest file for data export...\n')
        manifest_dir = etf.write_manifest(
                            export_dir=export_dir,
                            run_id=run_val,
                            scenario_id=scenario_val,
                            scenario_tag=scenario_name,
                            run_specs=config['run_specs'],
                            config=config,
                            tables=tables,
                            random_seed=seed_val
                        )
        print(f"Manifest file written to {manifest_dir}\n")

        print('Data export complete.\n Loading data to PostgreSQL...\n')

        ltp.load_run_to_postgres(tables, schema_selection=schema_spec)

        print('Data load to PostgreSQL complete.\n')
        print('----------------------------------------------------------------------------------------------------------------------\n\n')
        print('Simulation process finished successfully.\n')
        print('See specified directories and databases for output. Thank you for using the Manufacturing Plant Simulation.\n')

    else:
        # User chose not to continue
        print('Exiting simulation setup. Please rerun when ready.')
        return

if __name__ == '__main__':
    main()

"""
    # Example configuration presets (formatted for easy editing)

    # A - Large time-driven run with WIP limit enabled
    config_A = {
        "cycle_times": {
            "type": "discrete_uniform",
            "time_range": (600, 5400),  # seconds
        },
        "process_noise": {
            "type": "normal clipped",
            "mean_val": 1.0,
            "var_val": 0.1,
            "min_val": 0.85,
            "max_val": 1.20,
        },
        "time_to_failure": {
            "type": "exponential",
            "low_range": 14400,   # seconds
            "high_range": 288000, # seconds
        },
        "repair_behavior": {
            "type": "lognormal_clipped",
            "mean_val": 30,    # minutes
            "var_val": 0.6,
            "min_bound": 300,   # seconds
            "max_bound": 86400, # seconds
        },
        "product_family_weights": {
            "Logic Weight": 0.25,
            "Memory Wight": 0.25,
            "Analog Weight": 0.25,
            "Power Weight": 0.25,
        },
        "step_bounds": {
            "type": "discrete_uniform",
            "min_steps": 3,
            "max_steps": 6,
        },
        "work_order_interarrival": {
            "type": "gamma",
            "shape": 4.0,
            "scale": 1200.0, mean = 3600s
        },
        "batch_sizes": {
            "type": "poisson_clipped",
            "lambda": 5,
            "min_val": 1,
            "max_val": 12,
        },
        "quality": {
            "interrupt_penalty": 0.02,
            "min_yield": 0.85,
        },
        "run_specs": {
            "run_mode": "time",
            "num_machines": 100,
            "num_products": 50,
            "num_work_orders": 0,        # not required in time mode
            "wip_limit": 125,
            "wip_poll_interval": 60,     # seconds
            "sim_horizon_days": 180,     # days
        },
    }

    # B - Time-driven with increased interarrival (throttled arrivals), no WIP limit
    config_B = {
        "cycle_times": {
            "type": "discrete_uniform",
            "time_range": (600, 5400),
        },
        "process_noise": {
            "type": "normal clipped",
            "mean_val": 1.0,
            "var_val": 0.1,
            "min_val": 0.85,
            "max_val": 1.20,
        },
        "time_to_failure": {
            "type": "exponential",
            "low_range": 14400,
            "high_range": 288000,
        },
        "repair_behavior": {
            "type": "lognormal_clipped",
            "mean_val": 30,
            "var_val": 0.6,
            "min_bound": 300,
            "max_bound": 86400,
        },
        "product_family_weights": {
            "Logic Weight": 0.25,
            "Memory Wight": 0.25,
            "Analog Weight": 0.25,
            "Power Weight": 0.25,
        },
        "step_bounds": {
            "type": "discrete_uniform",
            "min_steps": 3,
            "max_steps": 6,
        },
        "work_order_interarrival": {
            "type": "gamma",
            "shape": 4.0,
            "scale": 13.75,  
        },
        "batch_sizes": {
            "type": "poisson_clipped",
            "lambda": 5,
            "min_val": 1,
            "max_val": 12,
        },
        "quality": {
            "interrupt_penalty": 0.02,
            "min_yield": 0.85,
        },
        "run_specs": {
            "run_mode": "time",
            "num_machines": 100,
            "num_products": 50,
            "num_work_orders": 0,
            "wip_limit": 5000,            # 0 or None = no limit
            "wip_poll_interval": 60,    # ignored when wip_limit is 0
            "sim_horizon_days": 30,
        },
    }

    # C - Time-driven with modest WIP cap (recommended realistic preset)
    config_C = {
        "cycle_times": {
            "type": "discrete_uniform",
            "time_range": (600, 5400),
        },
        "process_noise": {
            "type": "normal clipped",
            "mean_val": 1.0,
            "var_val": 0.1,
            "min_val": 0.85,
            "max_val": 1.20,
        },
        "time_to_failure": {
            "type": "exponential",
            "low_range": 14400,
            "high_range": 288000,
        },
        "repair_behavior": {
            "type": "lognormal_clipped",
            "mean_val": 30,
            "var_val": 0.6,
            "min_bound": 300,
            "max_bound": 86400,
        },
        "product_family_weights": {
            "Logic Weight": 0.25,
            "Memory Wight": 0.25,
            "Analog Weight": 0.25,
            "Power Weight": 0.25,
        },
        "step_bounds": {
            "type": "discrete_uniform",
            "min_steps": 3,
            "max_steps": 6,
        },
        "work_order_interarrival": {
            "type": "gamma",
            "shape": 4.0,
            "scale": 10.0,
        },
        "batch_sizes": {
            "type": "poisson_clipped",
            "lambda": 5,
            "min_val": 1,
            "max_val": 12,
        },
        "quality": {
            "interrupt_penalty": 0.02,
            "min_yield": 0.85,
        },
        "run_specs": {
            "run_mode": "time",
            "num_machines": 100,
            "num_products": 50,
            "num_work_orders": 0,
            "wip_limit": 80,           # modest WIP cap
            "wip_poll_interval": 60,   # seconds
            "sim_horizon_days": 180,
        },
    }
"""