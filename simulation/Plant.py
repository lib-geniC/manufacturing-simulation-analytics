# Plant Class Definition for Production Plant Simulation
"""
Plant orchestration and run logic for the Production Plant Simulation.

Author
------
Patrick Ortiz

Purpose
-------
Coordinate high-level simulation behavior: build machine objects and machine
type resources, generate products/processes/work orders, dispatch work orders,
drive the SimPy environment, and collect results into tabular outputs that
are exported by the surrounding run harness.

Notes / Conventions
-------------------
- Time units: cycle times, interarrival times and repair durations are
  expressed in seconds unless otherwise documented.
- Methods that perform SimPy activity (yielding/timeouts/process creation)
  expect to run inside the SimPy environment and therefore return generators
  or yield SimPy events.
- Docstrings follow numpy-style conventions used across the codebase.
- RNG injection pattern: `rngs` is passed to allow reproducible independent
  streams for arrivals, processing, failures, quality and structural sampling.
"""

####################################################################
## Required Setup ##
####################################################################

# Import necessary modules
from Machine import Machine
from MachineType import MachineType

import pandas as pd
import numpy as np
import data_generators as dg
import helper_functions as hf

import simpy
import warnings

####################################################################

## Plant Class Definition ##
class Plant(object):
    """
    High level plant model used to run production simulations.

    The Plant is responsible for:
      - building machine instances and machine-type resources
      - generating product/process/process-route and work-order tables
      - releasing work orders (time- or volume-driven)
      - dispatching and executing work orders by creating per-work-order
        processes that run on Machines
      - collecting logs from Machines and assembling final fact/dimension tables

    Attributes
    ----------
    config : dict
        Full run configuration (stochastic settings, distributions, run_specs).
    run_specs : dict
        Run-specific parameters (run_mode, num_machines, num_products, num_work_orders, wip_limit).
    rngs : dict
        Random number generator streams (arrival, processing, failure, quality, structure).
    env : simpy.Environment
        Simulation environment used by the Plant and machines.
    Maches : list
        Instantiated `Machine` objects used by the Plant.
    machine_types : dict
        Mapping machine_type -> MachineType wrapper containing simpy.Resource and machines list.
    active_work_orders : list
        List of simpy Process objects representing in-flight work orders.
    final_tables : list
        List of final tables produced by the run for export.
    """
    def __init__(self, env, scenario_val, scenario_name, config, rngs):
        """
        Initialize a Plant instance.

        Parameters
        ----------
        env : simpy.Environment
            Simulation environment used to schedule processes and timeouts.
        scenario_val : str
            String containing a set of letter and number identifiers for scenario currently in progress.
        scenario_name : str
            String containing a textual description of the scenario currently in progress.
        config : dict
            Full configuration dictionary (includes run_specs and stochastic parameters).
        rngs : dict
            RNG streams. Expected keys: 'arrival', 'processing', 'failure', 'quality', 'structure'.
        """
        # Store config
        self.scenario_id = scenario_val
        self.scenario_name = scenario_name
        self.config = config
        self.run_specs = self.config['run_specs']

        # Set random value generators to set seeds if required
        self.rngs = rngs
        self.arrival_rng = rngs['arrival']
        self.structure_rng = rngs['structure']

        # Interpret run mode and number of work orders
        if self.run_specs['num_work_orders'] in [None, 0]:
            self.num_work_orders = None
        else:
            self.num_work_orders = self.run_specs['num_work_orders']

        if self.run_specs['run_mode'] == 'volume':
            if self.num_work_orders is None:
                raise ValueError("num_work_orders required for volume-driven run")
        elif self.run_specs['run_mode'] == 'time':
            if self.num_work_orders is not None:
                warnings.warn("num_work_orders ignored in time-driven mode", category=RuntimeWarning)
                self.num_work_orders = 1
            else:
                self.num_work_orders = 1
        else:
            raise ValueError(f"Run mode provided can only be defined as either 'time' or 'volume', not {self.run_specs['run_mode']}.")

        # Attribute definitions
        self.num_machines = self.run_specs['num_machines']
        self.num_products = self.run_specs['num_products']
        self.Machines = []
        self.machine_types = {}
        self.work_order_start_times = {}
        self.work_order_end_times = {}
        self.work_order_counter = 0
        self.active_work_orders = []
        
        # Create simulation environment
        self.env = env
        self.done = env.event()

        # Initialize empty output tables
        self.dim_machine = []
        self.dim_product = []
        self.dim_process = []
        self.dim_process_route = []
        self.work_order_staging = None
        self.fact_work_order = []
        self.final_tables = []

    # ---------------------------------------------------------------------
    # Initialization helpers
    # ---------------------------------------------------------------------
    def initialize_simulation(self):
        """
        Build initial plant state: machines, machine-type resources,
        products/processes/process-routes and initial work orders.

        This prepares the Plant to start the release processes (time- or
        volume-driven) in the SimPy environment.
        """
        print("Plant initialization started...\n")
        self.dim_machine = self.build_machines()
        print("Machine objects built.\n")
        self.build_machine_type_resources()
        print("Machine type resources built.\n")

        (self.dim_product,
         self.dim_process,
         self.dim_process_route
        ) = self.generate_products_and_processes(
             self.dim_machine, 
             self.config['step_bounds']
             )

        print("Products and process routes generated.\n")
        
        print("Generating initial work orders...")
        self.work_order_staging = self.generate_work_orders(self.dim_product)
        print("Initial work orders generated.\n")

    def build_machines(self):
        """
        Instantiate `Machine` objects and start their background failure processes.

        Returns
        -------
        pandas.DataFrame
            The dimension table used to create the `Machine` objects (from data_generators).
        """
        print("Building machine objects...")
        # Instantiate machine objects
        dim_machine = dg.create_machines(self.num_machines, self.rngs)

        i = 1
        for row in dim_machine.itertuples(index=True, name='Plant_Machine_'+str(i)):
            machine = Machine(
                env=self.env,
                dim_machine_row=row,
                work_order=[],
                rngs=self.rngs
                )

            self.Machines.append(machine)
            i +=1
            # start background failure processes
            machine.start_failure_process(self.config['time_to_failure'])
        
        return dim_machine

    def build_machine_type_resources(self):
        """
        Create MachineType wrappers (simpy.Resource per machine type) and
        populate them with machine instances.

        This groups machines by `machine_type` so dispatching can request a
        resource by type and then pick an idle concrete machine.
        """
        print("Building machine type resources...")
        type_list = []
        for machine in self.Machines:
            type_list.append(machine.machine_type)
        
        unique_values, counts = np.unique(type_list, return_counts=True)

        machine_counts = dict(zip(unique_values, counts))

        for item in unique_values:
            self.machine_types[item] = MachineType(self.env, item, machine_counts[item])
            for machine in self.Machines:
                if machine.machine_type == item:
                    self.machine_types[item].machines.append(machine)

    def generate_products_and_processes(self, dim_machine, step_bounds):
        """
        Create dim_product, dim_process and dim_process_route tables.

        Parameters
        ----------
        dim_machine : pandas.DataFrame
            Machine dimension used to derive step-level cycle times and types.
        step_bounds : dict
            Dictionary with 'min_steps' and 'max_steps'.

        Returns
        -------
        tuple of pandas.DataFrame
            (dim_product, dim_process, dim_process_route)
        """
        print("Generating products and process routes...")
        # Evaluate product weights
        p_key = list(self.config['product_family_weights'].values())

        # Select products and build records
        min_steps = step_bounds['min_steps']
        max_steps = step_bounds['max_steps']
        dim_product, dim_process, dim_process_route = dg.create_products_with_processes(dim_machine, self.num_products, min_steps, max_steps, p_key, self.rngs)
        return dim_product, dim_process, dim_process_route

    # ---------------------------------------------------------------------
    # Work order generation & dispatch
    # ---------------------------------------------------------------------
    def generate_work_orders(self, dim_product):
        """
        Produce a batch of new work orders.

        Behavior
        --------
        - On the first call (work_order_staging is None) the function generates
          a complete set of work orders determined by `self.num_work_orders`.
        - On subsequent calls it generates a group determined by `batch_sizes`
          configuration using `helper_functions.generate_batch_group_size()`.

        Parameters
        ----------
        dim_product : pandas.DataFrame
            Product table used for sampling product ids.

        Returns
        -------
        pandas.DataFrame
            fact_work_order table with new/updated work orders.
        """
        # Create work order records
        if self.work_order_staging is None:
            fact_work_order, _ = dg.create_work_orders(dim_product, num_work_orders=self.num_work_orders, rngs=self.rngs)
        else:
            poisson_params = self.config['batch_sizes']
            lam = poisson_params['lambda']
            min_val = poisson_params['min_val']
            max_val = poisson_params['max_val']
            num_val = hf.generate_batch_group_size(lam, min_val, max_val, self.arrival_rng)
            fact_work_order, _ = dg.create_work_orders(dim_product, self.work_order_staging, num_work_orders=num_val, rngs=self.rngs)

        return fact_work_order

    def dispatch_work_order(self, work_order_df):
        """
        Map a work order's route steps to MachineType resources.

        Parameters
        ----------
        work_order_df : pandas.DataFrame
            Rows for a single work order describing process_route steps.

        Returns
        -------
        tuple
            (machine_pool, work_order_steps) where machine_pool is a list of
            MachineType objects (one per step) and work_order_steps is a list
            of step tuples for execution.
        """
        # Route work orders to machines
        work_order_steps = []
        machine_pool = []
        for step in work_order_df.itertuples(index=False, name='Process_Step'):
            machine_type = step.machine_type
            work_order_steps.append(step)
            machine_pool.append(self.machine_types[machine_type])

        return machine_pool, work_order_steps

    def _cleanup_active_work_orders(self) -> int:
        """
        Trim completed Processes from `self.active_work_orders` and return the
        current WIP (count of in-flight work order processes).

        Returns
        -------
        int
            Number of active (not-yet-triggered) work-order processes.
        """
        # Keep only processes that have NOT yet been triggered (i.e., still running)
        self.active_work_orders = [p for p in self.active_work_orders if not getattr(p, "triggered", False)]
        return len(self.active_work_orders)

    # ---------------------------------------------------------------------
    # Per-work-order execution
    # ---------------------------------------------------------------------
    def run_work_order(self, work_order_steps, machine_pool, planned_quantity):
        """
        SimPy process that executes all steps for a single work order.

        It obtains a resource by machine type, selects a concrete machine,
        runs the machine's `process_order` (which yields to env.timeout and
        may be interrupted by failures), and logs start/end times for the work order.

        Parameters
        ----------
        work_order_steps : list
            Ordered list of step objects for the work order.
        machine_pool : list
            List of `MachineType` objects aligned with steps.
        planned_quantity : int
            Initial unit count for the work order.

        Notes
        -----
        This method yields to the SimPy environment while waiting for resources
        and while machines execute `process_order`. It appends start/end timestamps
        to `self.work_order_start_times` and `self.work_order_end_times`.
        """
        current_quantity = planned_quantity

        for idx, step in enumerate(work_order_steps):
            assert idx < len(machine_pool), (
                f"Step index {idx} out of range for machine pool"
            )
            work_order_id = step.work_order_id
            step_number = step.step_number
            num_steps = len(machine_pool)
            process_id = step.process_id
            process_route_id = step.process_route_id
            target_yield = step.target_yield_rate
            
            # Run processes for machine environments
            with machine_pool[idx].resource.request() as type_req:
                if work_order_id not in self.work_order_start_times:
                    self.work_order_start_times[work_order_id] = self.env.now
                
                yield type_req
                machine = machine_pool[idx].select_machine()
                machine.start_quantity = current_quantity

                if machine is None:
                    raise RuntimeError(f"No idle machine found for {step.machine_type}")  
                
                yield self.env.process(machine.process_order(self.env, work_order_id, step_number, num_steps, process_id, process_route_id, target_yield, current_quantity, self.config["process_noise"], self.config["repair_behavior"], self.config["quality"]))
                current_quantity = machine.end_quantity
                machine.start_quantity = 0
                machine.end_quantity = 0

        assert work_order_id not in self.work_order_end_times,(
            f"Duplicate completion detected for WO {work_order_id}"
        )
        work_order_end_time = self.env.now
        self.work_order_end_times[work_order_id] = float(work_order_end_time)

    # ---------------------------------------------------------------------
    # Release processes (time- and volume-driven)
    # ---------------------------------------------------------------------
    def work_order_release_process(self, sim_horizon):
        """
        Time-driven release process.

        This process runs until `sim_horizon` and on each interarrival:
          - generates a batch of new work orders (via `generate_work_orders`)
          - prioritizes and groups them
          - releases them to the plant (subject to optional WIP limiter)

        Parameters
        ----------
        sim_horizon : float
            Simulation horizon in seconds (time-driven mode).
        """
        work_order_batch_lengths = [0]
        release_times = []
        work_order_ids = []
        while self.env.now < sim_horizon:
            interarrival = hf.generate_interarrival(self.config['work_order_interarrival'], self.arrival_rng)
            yield self.env.timeout(interarrival)
            
            initial_length = len(self.work_order_staging.index)
            if initial_length == 1:
                initial_length = 0

            self.work_order_staging = self.generate_work_orders(self.dim_product)

            final_length = len(self.work_order_staging.index)

            num_work_orders_added = final_length - initial_length
            work_order_batch_lengths.append(num_work_orders_added)
            
            work_orders_remaining = num_work_orders_added
            print('New work orders added: ', num_work_orders_added)
            while work_orders_remaining > 0:
                new_work_orders_df = self.work_order_staging.iloc[initial_length:final_length+1]
                work_order_df_ready = hf.prioritize_dispatches(new_work_orders_df, self.dim_process, self.dim_process_route)
                new_work_orders_grouped = work_order_df_ready.groupby('work_order_id', sort=False)
                for work_order_id, work_order_set in new_work_orders_grouped:
                    wip_limit = self.run_specs.get('wip_limit')
                    poll_interval = self.run_specs.get('wip_poll_interval', 60)
                    if wip_limit and int(wip_limit) > 0:
                        current_wip = self._cleanup_active_work_orders()
                        while current_wip >= int(wip_limit):
                            # wait and re-check
                            yield self.env.timeout(poll_interval)
                            current_wip = self._cleanup_active_work_orders()
                            print('\n\n\n\nWIP Limit Hit....Work Order Release Halted\n\n\n\n')
                    machine_pool, work_order_steps = self.dispatch_work_order(work_order_set)
                    print(f'Releasing work order {work_order_id} at time {self.env.now}')
                    work_order_row = self.work_order_staging.loc[self.work_order_staging['work_order_id'] == work_order_id, ['planned_quantity']]
                    planned_quantity = work_order_row['planned_quantity'].values[0]
                    proc = self.env.process(self.run_work_order(work_order_steps, machine_pool, planned_quantity))
                    self.active_work_orders.append(proc)
                    
                    yield self.env.timeout(0)

                    release_time = self.env.now
                    work_order_ids.append(work_order_id)
                    release_times.append(release_time)

                    work_orders_remaining -= 1
                
                print('Work orders remaining in batch: ', work_orders_remaining)
            
        yield self.env.timeout(0)
        assert len(self.active_work_orders) > 0, "No work orders were scheduled"
        print('\nWaiting for all work orders to complete...\n')

        # Trim completed processes and wait for the remainder to finish
        self._cleanup_active_work_orders()
        if self.active_work_orders:
            print("Active Work Orders:", len(self.active_work_orders))
            print(f"Unfinished: {sum(not p.triggered for p in self.active_work_orders)}\n")
            assert all(isinstance(p, simpy.events.Process) for p in self.active_work_orders), "Active work orders list contains invalid processes"
            yield simpy.events.AllOf(self.env, self.active_work_orders)
            assert all(p.triggered for p in self.active_work_orders), "Simulation advanced before all work orders completed"
        
        print('\nAll work orders completed\n')
        
        # Set final work order table and finalize outputs
        self.fact_work_order = self.work_order_staging

        self.final_tables = [
            self.dim_machine, 
            self.dim_product, 
            self.dim_process, 
            self.dim_process_route, 
            self.fact_work_order
        ]
        
        self.done.succeed()
        assert all(p.triggered for p in self.active_work_orders), \
            "Simulation ended with unfinished work orders"

    def run(self):
        """
        Entry generator that initializes the plant and delegates to the
        appropriate run mode (time- or volume-driven).
        """
        self.initialize_simulation()

        if self.run_specs['run_mode'] == 'time':
            yield from self.run_time_driven()

        elif self.run_specs['run_mode'] == 'volume':
            yield from self.run_volume_driven()

        else:
            raise ValueError('Invalid run_mode')

    def run_time_driven(self):
        """
        Start a time-driven simulation run by launching the time-based release
        process for the configured simulation horizon.
        """
        print("Starting time-driven simulation...\n")
        sim_horizon = self.run_specs['sim_horizon_days'] * 24 * 3600
        yield self.env.process(self.work_order_release_process(sim_horizon))
    
    def run_volume_driven(self):
        """
        Volume-driven run that releases a fixed total number of work orders.

        The function consumes batches produced by `hf.get_work_order_sets` and
        releases each work order (subject to optional WIP throttling).
        """
        print("Starting volume-driven simulation...\n")
        final_work_orders = hf.prioritize_dispatches(self.work_order_staging, self.dim_process, self.dim_process_route)

        release_times = []
        work_order_ids = []
        work_orders_remaining = self.num_work_orders

        while work_orders_remaining > 0:
            print(f'Work orders remaining at start of processing: {work_orders_remaining}')
            for batch in hf.get_work_order_sets(final_work_orders, self.config['batch_sizes'], self.arrival_rng):  
                print('Work order batch size: ', len(batch))
                interarrival_time = hf.generate_interarrival(self.config['work_order_interarrival'], self.arrival_rng)
                yield self.env.timeout(interarrival_time)

                for work_order_df in batch:
                    wip_limit = self.run_specs.get('wip_limit')
                    poll_interval = self.run_specs.get('wip_poll_interval', 60)
                    if wip_limit and int(wip_limit) > 0:
                        current_wip = self._cleanup_active_work_orders()
                        while current_wip >= int(wip_limit):
                            yield self.env.timeout(poll_interval)
                            current_wip = self._cleanup_active_work_orders()
                    machine_pool, work_order_steps = self.dispatch_work_order(work_order_df)
                    print(f'Releasing work order {work_order_df["work_order_id"].iloc[0]} at time {self.env.now}\n')
                    work_order_row = self.work_order_staging.loc[self.work_order_staging['work_order_id'] == work_order_df['work_order_id'].iloc[0], ['planned_quantity']]
                    planned_quantity = work_order_row['planned_quantity'].values[0]
                    proc = self.env.process(self.run_work_order(work_order_steps, machine_pool, planned_quantity))
                    self.active_work_orders.append(proc)
                    release_time = self.env.now
                    wo_id = str(work_order_df['work_order_id'].iloc[0])
                    work_order_ids.append(wo_id)
                    release_times.append(release_time)

                work_orders_remaining -= len(batch)
            
            print(f'Work orders remaining at end of processing: {work_orders_remaining}\n')
        
        yield self.env.timeout(0)
        print('\nWaiting for all work orders to complete...\n')

        # Trim completed processes and wait for the remainder to finish
        self._cleanup_active_work_orders()
        if self.active_work_orders:
            print("Active Work Orders:", len(self.active_work_orders))
            print("Unfinished:", sum(not p.triggered for p in self.active_work_orders))
            assert all(isinstance(p, simpy.events.Process) for p in self.active_work_orders), "Active work orders list contains invalid processes"
            yield simpy.events.AllOf(self.env, self.active_work_orders)
            assert all(p.triggered for p in self.active_work_orders), "Simulation advanced before all work orders completed"
        
        print('\nAll work orders completed\n')
        
        # Set final work order table and finalize outputs
        self.fact_work_order = self.work_order_staging

        self.final_tables = [
            self.dim_machine, 
            self.dim_product, 
            self.dim_process, 
            self.dim_process_route, 
            self.fact_work_order
        ]
        
        self.done.succeed()
    
    # ---------------------------------------------------------------------
    # Results collection
    # ---------------------------------------------------------------------
    def collect_results(self):
        """
        Gather logs from Machines and create final pandas DataFrames.

        Returns
        -------
        tuple
            (dim_machine, dim_product, dim_process, dim_process_route,
             fact_work_order, fact_production_event, fact_downtime_event, fact_quality_event)
        """
        
        wo_int_arr = self.config["work_order_interarrival"]
        int_mean = wo_int_arr["shape"] * wo_int_arr["scale"]

        dim_scenario = pd.DataFrame({
                'scenario_id': self.scenario_id,
                'scenario_name': self.scenario_name,
                'interarrival_mean_sec': int_mean,
                'wip_limit': self.run_specs["wip_limit"],
                'wip_poll_interval': self.run_specs["wip_poll_interval"],
                'sim_horizon_days': self.run_specs["sim_horizon_days"]
             }, index=[0])
        
        
        # Merge machine logs
        agg_production_logs = []
        for machine in self.Machines:
            for item in machine.production_log:
                agg_production_logs.append(item)

        fact_production_event = pd.DataFrame(agg_production_logs)
        fact_production_event = fact_production_event.sort_values('process_start', ascending=True)
        fact_production_event = fact_production_event.reset_index(drop=True)
        fact_production_event['scenario_id'] = self.scenario_id

        agg_downtime_logs = []
        for machine in self.Machines:
            for item in machine.downtime_log:
                agg_downtime_logs.append(item)

        fact_downtime_event = pd.DataFrame(agg_downtime_logs)
        fact_downtime_event['scenario_id'] = self.scenario_id

        agg_quality_logs = []
        for machine in self.Machines:
            for item in machine.quality_log:
                agg_quality_logs.append(item)

        fact_quality_event = pd.DataFrame(agg_quality_logs)
        fact_quality_event = fact_quality_event.sort_values('event_time', ascending=True)
        fact_quality_event = fact_quality_event.reset_index(drop=True)
        fact_quality_event['scenario_id'] = self.scenario_id


        dim_machine = self.final_tables[0]
        dim_machine['scenario_id'] = self.scenario_id
        dim_product = self.final_tables[1]
        dim_product['scenario_id'] = self.scenario_id
        dim_process = self.final_tables[2]
        dim_process['scenario_id'] = self.scenario_id
        dim_process_route = self.final_tables[3]
        dim_process_route['scenario_id'] = self.scenario_id
        fact_work_order = self.final_tables[4]

        work_order_ids_start, start_times = zip(*self.work_order_start_times.items())
        work_order_ids_end, end_times = zip(*self.work_order_end_times.items())
        work_order_starts = pd.DataFrame({
                'work_order_id': work_order_ids_start,
                'work_order_start_time': start_times
            })
        work_order_ends = pd.DataFrame({
                'work_order_id': work_order_ids_end,
                'work_order_end_time': end_times
            })

        work_order_times = work_order_starts.merge(work_order_ends[['work_order_id', 'work_order_end_time']], on='work_order_id', how='left')
        fact_work_order = fact_work_order.merge(work_order_times[['work_order_id', 'work_order_start_time', 'work_order_end_time']], on='work_order_id', how='left')
        fact_work_order['scenario_id'] = self.scenario_id

        # Remove non-required columns and perform sanity checks before returning
        dim_machine.drop(columns=['machine_status'], inplace=True)

        # Output final tables
        assert fact_work_order["work_order_end_time"].notna().all(), (
            "Some work orders never completed"
        )
        assert fact_production_event["process_start"].le(
            fact_production_event["process_end"]
        ).all(), "Invalid process timing detected"
        assert fact_production_event["machine_id"].notna().all(), (
            "Production event missing machine assignment"
        )
        return dim_machine, dim_product, dim_process, dim_process_route, fact_work_order, fact_production_event, fact_downtime_event, fact_quality_event, dim_scenario




