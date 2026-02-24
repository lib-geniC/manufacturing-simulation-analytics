### MachineType Class Definition for Production Plant Simulation ### 
"""
MachineType class for Production Plant Simulation.

Author
------
Patrick Ortiz

Purpose
-------
Lightweight wrapper around a SimPy Resource representing a type of machine.

This module defines `MachineType`, a small helper that pairs a `simpy.Resource`
with an explicit list of machine instances. The SimPy resource is used to
control capacity (how many concurrent users are allowed) while the `machines`
list contains the concrete machine objects that can be selected for work.

Notes / Conventions
-------------------
`select_machine()` assumes a resource `request()` has already been granted by
the caller. It finds the first idle machine in `self.machines` and marks it
busy before returning it. If no idle machine is found (despite capacity having
been granted) a `RuntimeError` is raised to highlight the invariant breach.
"""

####################################################################
## Required Setup ##
####################################################################

# Import SimPy for discrete-event simulation
import simpy

####################################################################

## MachineType Class Definition ##
class MachineType(object):
    """
    Represent a category/type of machines in the plant.

    Attributes
    ----------
    machine_type : str
        Logical name or identifier for the machine type.
    resource : simpy.Resource
        SimPy resource used to enforce capacity (number of machines available).
    machines : list
        List of machine objects (expected to have an `is_busy` boolean attribute).
    """
    ### Initialization Method ###
    def __init__(self, env, machine_type, count):
        """
        Initialize a MachineType.

        Parameters
        ----------
        env : simpy.Environment
            Simulation environment used to create the SimPy resource.
        machine_type : str
            Identifier/name for this machine type.
        count : int
            Capacity (number of parallel machines) for the SimPy resource.
        """
        # Store machine type identifier
        self.machine_type = machine_type
        # Resource controls concurrent access; capacity should match number of machines
        self.resource = simpy.Resource(env, capacity=count)
        # Actual machine objects are stored here; they must expose `is_busy`
        self.machines = []

    ### Machine Selection Method ###
    def select_machine(self):
        """
        Select and mark the first idle machine as busy.

        The function scans `self.machines` for a machine with `is_busy == False`.
        When found, it sets `is_busy = True` and returns the machine instance.

        Returns
        -------
        object
            The selected machine instance from `self.machines`.

        Raises
        ------
        RuntimeError
            If no idle machine is found. This indicates a logic error: a caller
            should only call this after obtaining capacity from `self.resource`.
        """
        # Select the first idle machine and mark it busy
        for m in self.machines:
            if not m.is_busy:
                m.is_busy = True
                # Return the selected machine
                return m
        # Raise runtime error for instances where no idle machine found despite capacity being granted
        raise RuntimeError("Capacity granted but no idle machine found")
