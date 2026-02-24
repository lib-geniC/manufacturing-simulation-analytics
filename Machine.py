### Machine Class Definition for Production Plant Simulation ###
"""
Machine class for Production Plant Simulation.

Author
------
Patrick Ortiz

Purpose
-------
Representation of a physical machine in the production plant simulation.

This class encapsulates machine state (operational, busy, logs) and behavior
(processing a work order, handling failures, logging production/downtime/quality
events). The class is designed to be used inside a SimPy environment; many
methods interact with `env` and yield simpy events.

Notes / Conventions
-------------------
- `ideal_cycle_time` is expected to be provided in the same time units used
    by the simulation environment (the project uses seconds elsewhere).
- Several RNGs are injected via `rngs` to keep sampling deterministic when a
    seed is provided by the caller.
- Methods that use `env` assume they are run inside a SimPy process (i.e.,
    they may `yield env.timeout(...)` or rely on `env.now`).
"""
####################################################################
## Required Setup ##
####################################################################

# Import necessary modules
import helper_functions as hf
import numpy as np
import simpy
from itertools import count

# Local RNG for non-seeded randomness
local_rng = np.random.default_rng()

# Module-level monotonic counters to guarantee unique numeric IDs across all Machine instances
# Using module-level counters ensures uniqueness across machines in a single process / run.
_event_id_counter = count(start=1)
_batch_id_counter = count(start=1)
_downtime_id_counter = count(start=1)
_quality_id_counter = count(start=1)

####################################################################

## Machine Class Definition ##
class Machine(object):
    def __init__(self, env, dim_machine_row, work_order, rngs):
        """
        Initialize a Machine instance.

        Parameters
        ----------
        env : simpy.Environment
            Simulation environment.
        dim_machine_row : pandas.Series-like
            Row from the machine dimension (expects attributes `machine_id`,
            `machine_type`, and `ideal_cycle_time`).
        work_order : any
            Initial work order id/context (may be None).
        rngs : dict
            Dictionary containing RNGs used by the machine. Expected keys:
            - 'processing', 'failure', 'quality'
        """
        # Simulation environment reference
        self.env = env
        # A simpy.Resource local to this machine (not the shared MachineType resource)
        self.machine = simpy.Resource(env)
        # Identifiers and static configuration
        self.machine_id = dim_machine_row.machine_id
        self.machine_type = dim_machine_row.machine_type
        self.ideal_cycle_time = dim_machine_row.ideal_cycle_time

        # Dynamic state
        self.is_operational = True            # whether the machine can operate (not failed)
        self.current_work_order = work_order  # id of work order currently assigned
        self.production_log = []              # list of production event dicts
        self.downtime_log = []                # list of downtime event dicts
        self.quality_log = []                 # list of quality check event dicts
        self.current_process = None           # reference to the active SimPy process, if any
        self.start_quantity = 0               # units at process start
        self.end_quantity = 0                 # units after processing
        self.is_busy = False                  # convenience flag for local scheduling

        # Random number generators for reproducible stochastic behavior
        self.processing_rng = rngs['processing']
        self.failure_rng = rngs['failure']
        self.quality_rng = rngs['quality']

    def vary_cycle_time(self, process_noise):
        """
        Compute an actual cycle time from nominal `ideal_cycle_time` with noise.

        Parameters
        ----------
        process_noise : dict
            Noise configuration with keys:
            - 'mean_val', 'var_val', 'min_val', 'max_val'

        Returns
        -------
        float
            The rounded actual cycle time (same units as `ideal_cycle_time`).
        """
        # Base nominal cycle time
        nominal = self.ideal_cycle_time

        # Extract noise parameters
        mean_val =  process_noise['mean_val']
        var_val = process_noise['var_val']
        min_val = process_noise['min_val']
        max_val = process_noise['max_val']

        # Sample noise and compute actual cycle time
        noise = np.clip(local_rng.normal(mean_val, var_val), min_val, max_val)
        actual_cycle_time = nominal * noise
        # Return rounded actual cycle time
        return round(actual_cycle_time, 0)

    def log_production_event(self,
                                process_id,
                                process_route_id,
                                step_number,
                                process_start,
                                process_end,
                                actual_cycle_time,
                                event_status
    ):
        """
        Log a production event for the current work order.

        The method generates unique `event_id` and `batch_id` values using the
        machine's processing RNG and appends an event dictionary to
        `self.production_log`.

        Parameters
        ----------
        process_id : int|str
            Identifier of the process being executed.
        process_route_id : int|str
            Identifier for the process route.
        step_number : int
            Step index within the route.
        process_start : numeric
            Timestamp (env.now) when processing started.
        process_end : numeric
            Timestamp (env.now) when processing ended.
        actual_cycle_time : numeric
            Observed cycle time for the step.
        event_status : str
            Status string (e.g. "completed", "interrupted", "failed").

        Returns
        -------
        int
            The generated `batch_id` for this production event.
        """
        
        # # Build sets of existing IDs to avoid collisions
        # if not self.production_log:
        #     curr_event_ids = [1.1]
        #     curr_batch_ids = [1.1]
        # else:
        #     curr_event_ids = []
        #     curr_batch_ids = []
        #     temp_prod_log = self.production_log
        #     for item in temp_prod_log:
        #         event_id_temp = item['event_id']
        #         batch_id_temp = item['batch_id']
        #         curr_event_ids.append(event_id_temp)
        #         curr_batch_ids.append(batch_id_temp)
        
        # # Generate unique event_id
        # while True:
        #     event_id = self.processing_rng.integers(1e9)
        #     if event_id in curr_event_ids:
        #         continue
        #     else:
        #         break
        # # Generate unique batch_id    
        # while True:
        #     batch_id = self.processing_rng.integers(1e9)
        #     if batch_id in curr_batch_ids:
        #         continue
        #     else:
        #         break

        # Generate globally unique IDs using module-level counters (fast, deterministic)
        event_id = next(_event_id_counter)
        batch_id = next(_batch_id_counter)

        # Append production event record
        self.production_log.append({
            'event_id': event_id,
            'work_order_id': self.current_work_order,
            'machine_id': self.machine_id,
            'process_id': process_id,
            'process_route_id': process_route_id,
            'step_number': step_number,
            'batch_id': batch_id,
            'process_start': process_start,
            'process_end': process_end,
            'ideal_cycle_time': self.ideal_cycle_time,
            'actual_cycle_time': actual_cycle_time,
            'event_status': event_status
                
        })  
        return batch_id


    def log_downtime_event(self,
                            process_id,
                            process_route_id,
                            failure_type,
                            usage_duration,
                            failure_start,
                            failure_end
    ):
        """
        Log a downtime event for the current work order / machine.

        Parameters
        ----------
        process_id : int|str
        process_route_id : int|str
        failure_type : str
            Human-readable failure description.
        usage_duration : numeric
            Time the machine had been processing before the failure segment.
        failure_start : numeric
            env.now timestamp when failure started.
        failure_end : numeric
            env.now timestamp when failure ended.
        """
        # # Collect existing downtime ids to avoid collisions
        # if not self.downtime_log:
        #     curr_downtime_ids = [1.1]
        # else:
        #     curr_downtime_ids = []
        #     for item in self.downtime_log:
        #         downtime_id_temp = item['downtime_id']
        #         curr_downtime_ids.append(downtime_id_temp)

        # # Generate unique downtime_id
        # while True:
        #     downtime_id = self.processing_rng.integers(1e9)
        #     if downtime_id in curr_downtime_ids:
        #         continue
        #     else:
        #         break

        downtime_id = next(_downtime_id_counter)

        # Append downtime record
        self.downtime_log.append({
            'downtime_id': downtime_id,
            'work_order_id': self.current_work_order,
            'process_id': process_id,
            'process_route_id': process_route_id,
            'machine_id': self.machine_id,
            'failure_type': failure_type,
            'usage_duration': usage_duration,
            'failure_start': failure_start,
            'failure_end': failure_end
        })

    def log_quality_event(self, env,
                          process_id,
                          process_route_id,
                          step_number,
                          batch_id,
                          initial_quantity,
                          good_units,
                          scrap_units
                          
    ):
        """
        Record the quality inspection result for a batch/step.

        Parameters
        ----------
        env : simpy.Environment
            Used to capture the current timestamp via `env.now`.
        process_id, process_route_id, step_number, batch_id : identifiers
        initial_quantity : int
            Number of units presented for inspection.
        good_units : int
            Units that passed inspection.
        scrap_units : int
            Units that failed inspection.
        """
        # # Collect existing quality ids to avoid collisions
        # if not self.quality_log:
        #     curr_quality_ids = [1.1]
        # else:
        #     curr_quality_ids = []
        #     for item in self.quality_log:
        #         quality_id_temp = item['quality_id']
        #         curr_quality_ids.append(quality_id_temp)

        # # Generate unique quality_id
        # while True:
        #     quality_id = self.processing_rng.integers(1e9)
        #     if quality_id in curr_quality_ids:
        #         continue
        #     else:
        #         break

        quality_id = next(_quality_id_counter)

        # Append quality event record
        self.quality_log.append({
            'quality_id': quality_id,
            'work_order_id': self.current_work_order,
            'machine_id': self.machine_id,
            'process_id': process_id,
            'process_route_id': process_route_id,
            'step_number': step_number,
            'batch_id': batch_id,
            'initial_quantity': initial_quantity,
            'units_approved': good_units,
            'units_scrapped': scrap_units,
            'event_time': env.now
        })
    
    def start_failure_process(self, time_to_failure):
        """
        Launch the background failure generator process for this machine.

        Parameters
        ----------
        time_to_failure : dict
            Configuration used by `cause_failure`.
        """

        # Start the failure process
        self.env.process(self.cause_failure(time_to_failure))
        
    def process_order(self, env, work_order_id, step_number, num_steps, process_id, process_route_id, target_yield, current_quantity, process_noise, repair_behavior, quality):
        """
        SimPy process to execute a work order step on this machine.

        This method is intended to be used as a process body (i.e., called by
        `env.process(machine.process_order(...))` or yielded by another process).
        It handles:
         - checking & setting busy state
         - applying variable cycle time
         - yielding to env.timeout and catching simpy.Interrupt for failures
         - applying repair delays and logging downtime
         - computing quality outcome and logging production/quality events

        Parameters
        ----------
        env : simpy.Environment
        work_order_id : int|str
        step_number : int
        num_steps : int
            Total number of steps in the route (used to distribute target yield).
        process_id, process_route_id : identifiers for logging
        target_yield : float
            Overall target yield for the work order (0..1).
        current_quantity : int
            Units entering this step.
        process_noise : dict
            Passed to `vary_cycle_time`.
        repair_behavior : dict
            Passed to `hf.vary_repair_time` when interrupted.
        quality : dict
            Configuration values such as 'interrupt_penalty' and 'min_yield'.

        Raises
        ------
        AssertionError
            If the machine is already processing or if timestamps/quantities are invalid.
        """    

        # Ensure machine is not already busy
        assert self.current_process is None, (
            f"Machine {self.machine_id} started a new process while still busy"
        )

        # Mark machine as busy
        self.is_busy = True
        print(f"Work order processing started for WO: {work_order_id} on Machine: {self.machine_id}")

        # Set current work order and starting quantity
        self.current_work_order = work_order_id
        self.start_quantity = current_quantity

        # Determine actual cycle time with noise
        actual_cycle_time = self.vary_cycle_time(process_noise)

        # Validate actual cycle time
        assert actual_cycle_time > 0, (
            f"Non-positive cycle time on machine {self.machine_id}: {actual_cycle_time}"
        )
        
        # Reference to the active process for interruption handling
        self.current_process = env.active_process

        # Process the work order step, handling interruptions for failures
        remaining_time = actual_cycle_time
        counter = 0
        loop_guard = 0
        process_start = env.now
        process_end = None
        batch_id = 0

        # Main processing loop with interruption handling
        while remaining_time > 0:
            # Validate remaining time has not increased
            assert remaining_time <= actual_cycle_time, (
                f"Remaining time increased unexpectedly on machine {self.machine_id}"
            )
            # Infinite loop guard
            loop_guard +=1
            # Break if loop guard exceeds threshold of 1000 iterations
            if loop_guard > 1000:
                # Log error and break to avoid infinite loop
                print(
                        f"[ERROR] Infinite loop detected\n"
                        f"Machine: {self.machine_id}\n"
                        f"WO: {self.current_work_order}\n"
                        f"Remaining: {remaining_time}\n"
                        f"Now: {env.now}\n"
                        f"Operational: {self.is_operational}"
                    )
                counter = -1
                break
            # Attempt to process the remaining time segment
            try:
                # Record segment start time
                segment_start = env.now
                # Yield timeout for remaining processing time
                yield env.timeout(remaining_time)
                # If completed without interruption, set remaining time to zero
                remaining_time = 0
            # Handle interruptions due to failures
            except simpy.Interrupt as downtime:
                # Mark machine as non-operational during failure
                self.is_operational = False
                counter += 1
                # Record failure start time and type
                failure_start = env.now
                failure_type = downtime.cause
                # Compute elapsed processing time before interruption
                elapsed = env.now - segment_start
                if elapsed <= 0:
                    elapsed = min(1e-6, remaining_time)
                # Update remaining time
                remaining_time -= elapsed
                if remaining_time < 0:
                    remaining_time = 0
                # Simulate repair time
                repair_time = hf.vary_repair_time(repair_behavior)
                yield env.timeout(repair_time) 
                failure_end = env.now
                # Log the downtime event
                self.log_downtime_event(process_id, process_route_id, failure_type, elapsed, failure_start, failure_end)
                # Mark machine as operational again
                self.is_operational = True
        # End of processing loop
        process_end = env.now
        self.current_process = None
        self.is_busy = False
        # Compute quality outcome
        interrupt_penalty = counter * quality["interrupt_penalty"]
        distributed_target = target_yield ** (1 / num_steps)
        step_yield = max(quality["min_yield"], distributed_target - interrupt_penalty)
        good_units = self.quality_rng.binomial(self.start_quantity, step_yield)
        
        # Compute scrap units and finalize quantities
        good_units = int(round(good_units, 0))
        scrap_units = self.start_quantity - good_units
        self.end_quantity = good_units

        # Validate quantities
        assert good_units + scrap_units == self.start_quantity, (
            f"Amount of units accepted and scrapped does not equal the amount of units provided at production start for {work_order_id} Step No. {step_number}."
        )

        # Validate timestamps
        assert process_start is not None and process_end is not None, (
            f"Missing process timestamps on machine {self.machine_id}, WO {work_order_id}"
        )
        
        # Log production and quality events
        batch_id = self.log_production_event(process_id, process_route_id, step_number, process_start, process_end, actual_cycle_time, event_status="interrupted" if counter > 0 else("failed" if counter < 0 else "completed"))
        self.log_quality_event(env, process_id, process_route_id, step_number, batch_id, self.start_quantity, good_units, scrap_units)

        # Final log message and clear current work order context
        print(f'Work order processing ended for WO: {work_order_id} on Machine: {self.machine_id}')
        self.current_work_order = None

    def cause_failure(self, time_to_failure):
        """
        Continuous process that generates random failure events and interrupts the
        current processing job when a failure occurs.

        Parameters
        ----------
        time_to_failure : dict
            Expected keys:
            - 'low_range', 'high_range' : range bounds used to draw mean time-to-failure.
        """
        # Extract bounds for mean time-to-failure sampling
        low_bound = time_to_failure['low_range']
        high_bound = time_to_failure['high_range']
        # Continuous failure generation loop
        while True:
            # Sample mean time-to-failure and actual failure time
            mean_ttf = local_rng.integers(low=low_bound, high=high_bound)
            failure_time = local_rng.exponential(mean_ttf)
            # Wait until failure time
            yield self.env.timeout(failure_time)
            # If machine is operational, select a failure type and interrupt current process
            if self.is_operational:
                # Select failure type
                failure_type_selection = self.failure_rng.choice(["Bearing Seizure", "Drive Belt Snapped", "Hydraulic Fluid Leak", "Pneumatic Pressure Loss", "Motor Overheating", "Electrical Short Circuit", "Sensor Misalignment", "PLC Logic Error", "Lubrication Starvation", "Vacuum Pump Cavitation", "Chamber Contamination", "MFC Drift", "Wafer Handling Robot Jam", "ESD Event", "RF Generator Arc-over", "Ion Source Depletion", "Mask Misalignment", "CDS Clog", "Turbo Pump Vibration", "Calibration Drift"])
                # Interrupt the current process if one is active
                if self.current_process is not None:
                    self.current_process.interrupt(cause=failure_type_selection)
