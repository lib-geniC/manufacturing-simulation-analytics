### Process Helper Functions for Production Plant Simulation ###
"""
Process helper utilities for the Production Plant Simulation.

Author
------
Patrick Ortiz

Purpose
-------
Provide small, reusable utilities used by the SimPy-based production plant
simulation. Functions include stochastic sampling helpers, grouping and
dispatch helpers for work orders, safe input conversion helpers, and a
utility to split unit counts into production batches.

Notes / Conventions
-------------------
- Functions accept an optional `rng` or `rngs` dict; when no RNG is provided
  the module-level `local_rng` is used. This enables reproducible runs by
  injecting seeded Generator instances.
- Docstrings follow numpy-style conventions to match the project's
  documentation style and aid automated documentation generation.
- Timing-related fields are documented in seconds unless stated otherwise.
"""
####################################################################
## Required Setup ##
####################################################################

# Library Imports
import numpy as np
import pandas as pd
from typing import List, Optional, Generator

# Local RNG for non-seeded randomness
local_rng = np.random.default_rng()

####################################################################
## Function Definitions ##
####################################################################

# -----------------------------------------------------------------------------
# Interarrival time generation function
# -----------------------------------------------------------------------------
def generate_interarrival(gamma_params: dict={'shape': 3.0, 'scale': 600.0}, 
                          rng: Optional[function] = local_rng
) -> float:
    """
    Sample a single interarrival time from a Gamma distribution.

    Parameters
    ----------
    gamma_params : dict
        Dictionary with keys:
        - 'shape': shape (k) parameter
        - 'scale': scale (theta) parameter
    rng : numpy.random.Generator
        Random number generator instance to use for sampling.

    Returns
    -------
    float
        A single sample drawn from Gamma(shape, scale).
    """
    # Extract parameters
    shape = gamma_params['shape']
    scale = gamma_params['scale']

    # Sample and return an interrarrival time value
    return rng.gamma(shape, scale)
# -----------------------------------------------------------------------------
# Batch group size generation function
# -----------------------------------------------------------------------------
def generate_batch_group_size(lam: int=5, 
                              min_val: int=1, 
                              max_val: int=12, 
                              rng: Optional[function] = local_rng
) -> int:
    """
    Generate a batch group size using a Poisson draw then clamp to bounds.

    Parameters
    ----------
    lam : float
        Lambda parameter for the Poisson distribution.
    min_val : int
        Lower bound for the returned size (inclusive).
    max_val : int
        Upper bound for the returned size (inclusive).
    rng : numpy.random.Generator
        Random number generator instance to use for sampling.

    Returns
    -------
    int
        A clamped integer batch size between `min_val` and `max_val`.
    """
    # Sample from Poisson distribution values
    size = rng.poisson(lam)

    # Clamp to min/max bounds and return a batch size
    return max(min_val, min(size, max_val))
# -----------------------------------------------------------------------------
# Dispatch prioritization function
# -----------------------------------------------------------------------------
def prioritize_dispatches(fact_work_order: pd.DataFrame, 
                          dim_process: pd.DataFrame, 
                          dim_process_route: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge and sort work-order/route/process information to produce a dispatch order.

    The function joins `dim_process_route` to `dim_process` to obtain product context,
    then merges with `fact_work_order` to produce a DataFrame that can be used for
    dispatch ordering. The final DataFrame is sorted by `work_order_id`, `start_date`,
    `priority`, and `step_number`.

    Parameters
    ----------
    fact_work_order : pandas.DataFrame
        Work order facts containing at minimum: `product_id`, `work_order_id`,
        `planned_quantity`, `target_yield_rate`, `start_date`, `due_date`, `priority`.
    dim_process : pandas.DataFrame
        Process dimension containing at minimum: `process_id`, `product_id`.
    dim_process_route : pandas.DataFrame
        Process route mapping containing at minimum: `process_id`, `step_number`.

    Returns
    -------
    pandas.DataFrame
        Merged and sorted DataFrame suitable for prioritized dispatching.
    """
    # Merge process route with process to get product context
    process_merged = dim_process_route.merge(dim_process[['process_id', 'product_id']], on='process_id', how='left')
    # Merge with work order facts and sort for dispatch priority
    work_order_to_process = process_merged.merge(fact_work_order[['product_id', 'work_order_id', 'planned_quantity', 'target_yield_rate', 'start_date', 'due_date', 'priority']], on='product_id', how='left')
    # Sort by work order ID, start date, priority, and step number
    work_orders_sorted = work_order_to_process.sort_values(by=['work_order_id', 'start_date', 'priority', 'step_number'], ascending=[True, True, True, True])
    # Return the sorted DataFrame
    return work_orders_sorted
# -----------------------------------------------------------------------------
# Work order set generation function
# -----------------------------------------------------------------------------
def get_work_order_sets(df_ready_work_orders: pd.DataFrame, 
                        poisson_params: dict = {'lambda': 5, 'min_val': 1, 'max_val': 12}, 
                        rng: Optional[function] = local_rng
) -> Generator[List[pd.DataFrame], None, None]:
    """
    Yield groups of ready work orders in batches determined by a Poisson-based size.

    The generator groups ready work orders by `work_order_id` and yields a list of
    per-work-order DataFrames for each batch slice. Batch sizes are produced by
    `generate_batch_group_size()` and respect `min_val`/`max_val` bounds.

    Parameters
    ----------
    df_ready_work_orders : pandas.DataFrame
        DataFrame of ready work orders (must include `work_order_id` column).
    poisson_params : dict
        Poisson parameter dictionary with keys:
        - 'lambda': lambda parameter for Poisson sampling
        - 'min_val': minimum batch size
        - 'max_val': maximum batch size
    rng : numpy.random.Generator
        RNG used for Poisson sampling.

    Yields
    ------
    list[pandas.DataFrame]
        A list of DataFrames where each DataFrame corresponds to a single work order
        in the current batch.
    """
    # Extract Poisson parameters
    lam = poisson_params['lambda']
    min_val = poisson_params['min_val']
    max_val = poisson_params['max_val']
    
    # Get unique work order IDs
    unique_ids = df_ready_work_orders['work_order_id'].unique()

    # Initialize index
    i = 0

    # Loop through unique IDs in batches
    while i < len(unique_ids):
        # Determine current batch size and slice IDs
        current_set_size = generate_batch_group_size(lam, min_val, max_val, rng) 
        ids_to_pull = unique_ids[i : i + current_set_size]
        # Filter DataFrame for current batch IDs
        work_order_groups = df_ready_work_orders[df_ready_work_orders['work_order_id'].isin(ids_to_pull)]
        # Yield list of DataFrames grouped by work order ID
        yield [group for _, group in work_order_groups.groupby('work_order_id', sort=False)]
        # Increment index
        i += current_set_size
# -----------------------------------------------------------------------------
# Repair time generation function
# -----------------------------------------------------------------------------
def vary_repair_time(repair_behavior: dict = {'mean_val': 30, 'var_val': 0.6, 'min_bound': 300, 'max_bound': 28800}
) -> float:
    """
    Sample a repair time from a log-normal distribution and clip to bounds.

    Notes
    -----
    The project-level `repair_behavior['mean_val']` is expressed in minutes in the
    configuration. This function converts the mean to seconds for the lognormal draw
    (via multiplication by 60) then clips to `min_bound`/`max_bound` which are expected
    to be in seconds.

    Parameters
    ----------
    repair_behavior : dict
        Dictionary containing:
        - 'mean_val' : numeric (minutes)
        - 'var_val' : float (sigma for lognormal)
        - 'min_bound' : numeric (seconds)
        - 'max_bound' : numeric (seconds)

    Returns
    -------
    float
        Repair time in seconds (rounded to nearest integer).
    """
    # Extract parameters
    mean_val = repair_behavior['mean_val']
    var_val = repair_behavior['var_val']
    min_bound = repair_behavior['min_bound']
    max_bound = repair_behavior['max_bound']

    # Sample from log-normal distribution and clip to bounds
    repair_time = np.clip(local_rng.lognormal(mean=np.log(mean_val * 60), sigma=var_val), min_bound, max_bound)
    # Round to nearest integer and return
    return round(repair_time, 0)
# -----------------------------------------------------------------------------
# Safe input conversion functions
# -----------------------------------------------------------------------------
def safe_int(val: Optional[str] = None) -> Optional[int]:
    """
    Safely convert a string-like value to int, preserving None.

    Parameters
    ----------
    val : str or None
        String to convert (whitespace allowed) or None.

    Returns
    -------
    int or None
        Converted integer, or None if input was None.

    Raises
    ------
    ValueError
        If the provided value cannot be converted to an integer.
    """
    # Check for None input
    if val is None or '':
        return None
    # Attempt conversion to integer
    try:
        return int(val.strip())
    # Catch exceptions and raise ValueError
    except Exception:
        raise ValueError(f"Expected integer, got: {val}")
# # #
def safe_float(val: Optional[str] = None) -> Optional[float]:
    """
    Safely convert a string-like value to float, preserving None.

    Parameters
    ----------
    val : str or None
        String to convert (whitespace allowed) or None.

    Returns
    -------
    float or None
        Converted float, or None if input was None.

    Raises
    ------
    ValueError
        If the provided value cannot be converted to a float.
    """
    # Check for None input
    if val is None:
        return None
    # Attempt conversion to float
    try:
        return float(val.strip())
    # Catch exceptions and raise ValueError
    except Exception:
        raise ValueError(f"Expected float, got: {val!r}")

# -----------------------------------------------------------------------------
# Batch splitting function
# -----------------------------------------------------------------------------
def split_into_batches(
    unit_count: int=25, 
    max_batches: int=5
    ) -> List[int]:
    """
    Creates a list of batch counts based on provided inputs.

    The function takes in the total number of production units and the user preference of maximum number of batches
    and ouputs a list containing individual batch number counts (as evenly split as possible) that will add up to the 
    total units provided. Length of list is representative of number of batches. The function can handle odd amounts of total units
    while still maintaining as many evenly split batches as mathematically possible.

    Parameters
    ----------
    unit_count : integer, default 25
        The input data for number of units to split into batches.
    max_batches : integer, default 5
        The maximum number of batches to split up the units.

    Returns
    -------
    batches : list of integer
        A list of integers representating the counts of each individual batch required to produce the unit total provided.

    See Also
    --------
    set : For pulling unique values of batch counts out of the list.

    Examples
    --------
    Example 1: Input with even number of units.

    >>> batches = split_into_batches(100, 10)
    >>> print(batches)
    [10, 10, 10, 10, 10, 10, 10, 10, 10, 10]
    
    Example 2: Input with odd number of units.

    >>> batches = split_into_batches(113, 10)
    >>> print(batches)
    [12, 12, 12, 11, 11, 11, 11, 11, 11, 11]
    """
    # Check whether the input is a negative integer or zero
    if unit_count <= 0:
        return []
    
    # Check if the number of units is smaller than the number of batches input
    num_batches = min(max_batches, unit_count)
    
    # Compute the lowest base size for even batches per number of batches input
    base_size = unit_count // num_batches

    # Compute the remaining units not accounted for if unit_count cannot be evenly split
    remainder = unit_count % num_batches

    # Produce a list of batch counts including the addition of remainders if required
    batches = [base_size + 1] * remainder + [base_size] * (num_batches - remainder)

    # Ouput
    return batches