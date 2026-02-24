### Data Generation Functions for Production Plant Simulation ###
"""
Data generation utilities for the Production Plant Simulation.

Author
------
Patrick Ortiz

Purpose
-------
Generate initial synthetic tables used to instantiate a simulated
production plant (machines, products, processes, process routes and
work orders). All tables are produced with pseudo-random sampling so
that downstream simulation runs are reproducible when RNG seeds are
provided.

Notes / Conventions
-------------------
- All RNGs default to module-level `local_rng` but functions accept an
  optional `rngs` dict. If provided, functions use `rngs['structure']`
  for structural sampling to allow reproducible, coordinated draws.
- Cycle times / timing-related fields are in seconds unless otherwise
  noted.
- The output DataFrames are intended for immediate use by the SimPy
  simulation code and by the `export_to_folder` module.
"""
####################################################################
## Required Setup ##
####################################################################

# Library Imports
import math
import numpy as np
import pandas as pd
import itertools
from typing import List, Optional

# Module-level RNG used when no explicit rngs dict is provided.
local_rng = np.random.default_rng()

####################################################################
## Function Definitions ##
####################################################################

# -----------------------------------------------------------------------------
# dim_machine generation
# -----------------------------------------------------------------------------
def create_machines(num_machines: int = 10, rngs: Optional[dict] = None) -> pd.DataFrame:
    """
    Generate a machine dimension DataFrame.

    Description
    ----------
    Produces `num_machines` rows with synthetic machine properties used by
    the simulation: identifiers, machine type, operational state, ideal
    cycle time and installation date.

    Important details
    -----------------
    - If `rngs` is provided, the function uses `rngs['structure']` for
      structural sampling. Otherwise the module-level `local_rng` is used.
    - `ideal_cycle_time` is sampled per machine type and expressed in
      seconds (the simulation operates in seconds).
    - `machine_status` is currently always set to "Operational".
    - The function returns a pandas DataFrame ready for use by the
      process-generation functions.

    Parameters
    ----------
    num_machines : int, optional
        Number of machines to generate (default 10).
    rngs : dict, optional
        Optional RNG dictionary; expected key: 'structure'.

    Returns
    -------
    pandas.DataFrame
        Columns: `machine_id`, `machine_type`, `machine_status`,
        `ideal_cycle_time`, `install_date`.

    See Also
    --------
    create_products_with_processes : Uses `ideal_cycle_time` values when
        assigning step cycle times in `dim_process_route`.
    """
    # Conditional RNG assignment for reproducibility when coordinating structural sampling across functions
    if not rngs:
        rng = local_rng
    else:
        rng = rngs['structure']
    # Candidate machine types (intentionally repeated to control relative frequency)
    machine_type_weights = {
        "Lithography": 0.20,
        "Deposition": 0.25,
        "Etch": 0.20,
        "Assembly": 0.20,
        "Test_Packaging": 0.15
    }

    min_per_type = 1
    num_types = len(machine_type_weights)

    if num_machines < min_per_type*num_types:
        raise ValueError("Not enough machines to statisfy minimum per type. (Note: minimum machines required is 5)")

    machine_type_distribution = {}

    for m_type, weight in machine_type_weights.items():
        machine_type_distribution[m_type] = math.floor(weight * num_machines)

    for m_type in machine_type_distribution:
        if machine_type_distribution[m_type] < min_per_type:
            machine_type_distribution[m_type] = min_per_type

    current_total = sum(machine_type_distribution.values())
    difference = num_machines - current_total
    
    if difference > 0:
        sorted_types = sorted(
            machine_type_weights.items(),
            key=lambda x: x[1],
            reverse=True
        )
    
        i = 0
        while difference > 0:
            m_type = sorted_types[i % len(sorted_types)][0]
            machine_type_distribution[m_type] += 1
            difference -= 1
            i += 1
    elif difference < 0:
        sorted_types = sorted(
            machine_type_distribution.items(),
            key=lambda x: x[1],
            reverse=True
        )
    
        i = 0
        while difference < 0:
            m_type = sorted_types[i % len(sorted_types)][0]
            if machine_type_distribution[m_type] > min_per_type:
                machine_type_distribution[m_type] -= 1
                difference += 1
            i += 1

    machine_type = []
    machine_type_options = []
    for m_type, count in machine_type_distribution.items():
        machine_type_options.append(m_type)
        for _ in range(count):
            machine_type.append(m_type)
    
    unique_values, _ = np.unique(machine_type_options, return_counts=True)
    
    # Installation date range (days)
    start_date = np.datetime64('2022-01-01')
    end_date = np.datetime64('2024-12-31')
    all_dates = pd.date_range(start=start_date,end=end_date,freq='D').to_numpy()

    # ID and type sampling
    machine_id = rng.choice(int(1e9), size=num_machines, replace=False)
    machine_status = np.array((['Operational'] * num_machines))
    
    # Assign an ideal cycle time per machine type, then map to each machine
    ideal_cycle_key = {}
    for item in unique_values:
        ideal_cycle_key[item] = rng.integers(low=600, high=5400)
    ideal_cycle_time = []
    for item in machine_type:
        ideal_cycle_time.append(ideal_cycle_key[item])
    ideal_cycle_time = np.asarray(ideal_cycle_time)

    # Install dates
    install_date = rng.choice(all_dates, size=num_machines, replace=False)

    # Build DataFrame
    df = pd.DataFrame(
        {
            'machine_id': machine_id,
            'machine_type': machine_type,
            'machine_status': machine_status,
            'ideal_cycle_time': ideal_cycle_time, # unit = seconds
            'install_date': install_date,
            }
        )

    # Output
    return df

# -----------------------------------------------------------------------------
# dim_product, dim_process, dim_process_route generation
# -----------------------------------------------------------------------------
def create_products_with_processes(
    dim_machine: pd.DataFrame, 
    num_products: int = 10,
    min_steps: int = 1,
    max_steps: int = 1,
    p_key: List = [0.25, 0.25, 0.25, 0.25],
    rngs: Optional[dict] = None
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Generate product and process tables: `dim_product`, `dim_process`, `dim_process_route`.

    Description
    ----------
    - `dim_product` contains product metadata and target yield rates.
    - `dim_process` contains a high-level process identifier and name per product.
    - `dim_process_route` expands each process into one or more ordered steps
      and assigns a machine type and an associated step cycle time (drawn from
      the `dim_machine` table).

    Key behavior and updates
    ------------------------
    - Probability weighting via `p_key`:
        * If `p_key` equals the default uniform list, product names are sampled
          uniformly from the full flattened product name list.
        * If `p_key` contains custom weights, those weights are distributed
          proportionally across the products in each family before sampling.
    - Steps per process are drawn for each `process_id` using `rng.integers`
      in the range `[min_steps, max_steps]`; each step becomes a row in the
      process route table.
    - Step cycle times are matched to sampled machines' `ideal_cycle_time`
      so route steps have realistic, consistent timing.

    Parameters
    ----------
    dim_machine : pandas.DataFrame
        Machine table used to derive `step_cycle_time` and `machine_type`.
    num_products : int, optional
        Number of products to generate (default 10).
    min_steps, max_steps : int, optional
        Minimum and maximum number of steps per process.
    p_key : list, optional
        Four-element list specifying family selection probabilities
        for ['Logic', 'Memory', 'Analog', 'Power'] respectively.
    rngs : dict, optional
        Optional RNG dict; expected key: 'structure'.
    
    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        (dim_product, dim_process, dim_process_route)

    Notes
    -----
    - `target_yield_rate` is sampled from a Beta distribution (parameters a=9.8, b=6.0).
    - `sample_per_yield` is a small fraction used for downstream quality sampling.
    """
    # -----------------------------------------------------------------------------
    # dim_product generation
    # -----------------------------------------------------------------------------
    # Select RNG
    if not rngs:
        rng = local_rng
    else:
        rng = rngs['structure']

    # Beta distribution parameters for yield sampling (empirically chosen)
    a, b = 9.8, 6.0
    
    # Product family -> list of product names
    product_classification_key = {'Logic':["L-Series Nano PLC", "FlowMaster Process Controller", "SyncHub Multi-Axis Master", "CoreNode Distributed CPU", "Axiom Edge Gateway", "Velocity RTU", "GridLink Network Processor", "ShiftStation I/O Terminal", "Vault Industrial Data Storage", "Bridge Protocol Converter", "Prime Mainframe Controller", "FlexIO Remote Expansion Module", "Nexus Central Communication Hub", "Swift Micro-Controller", "Pathfinder Intelligent Router", "TaskMaster Sequencing Unit", "Omni Integrated Control Center", "Motive Engine Drive", "Atlas Topology Mapper", "Matrix Rack-Mount Chassis", "Insight Machine Vision", "Proximity Inductive Sensor", "Thermocore Temperature Probe", "FlowScan Ultrasonic Meter", "Presstige High-Precision Gauge", "LevelMax Liquid Depth Sensor", "BeamLine Photoelectric Array", "Trace RFID Tag Reader", "CodeScan 2D Barcode Imager", "Axis360 Rotary Encoder", "LoadForce Strain Gauge", "G-Sense Vibration Monitor", "AcoustiX Sound Level Meter", "Humidistat Climate Sensor", "Spectral Gas Monitor", "PureSense Air Quality Probe", "Echo Rangefinder", "Vista Smart Surveillance", "Halo 360 Safety Sensor", "PointIR Infrared Point Sensor", "ViewPort Operator Panel", "Glass Industrial Touchscreen", "Dash Performance Analytics", "VizPro High-Res Display", "Status LED Indicator Tower", "Alert Wireless Notifier", "Command Integrated Console", "Grafix HMI Design Suite", "PanelMate Compact Interface", "Beacon Remote Status Light", "HMI-700 Series Display", "O-Panel Compact", "Visionary AR Interface", "TouchPoint Terminal", "DirectView Console", "OmniDashboard", "SignalCore Light", "Broadside Annunciator", "VividPanel 4K", "UserNode Access Point", "Kinetic Servo Drive", "VFD-X Variable Frequency Inverter", "Actuon Pneumatic Cylinder", "Grip Robotic End-Effector", "Arm-5 Modular Robotic Link", "Torque Electronic Gearbox", "Phase Power Supply Module", "FlowValve Solenoid Actuator", "Transcon Conveyor Motor", "Stepper Precision Motion Driver", "Chiller-Pro Thermal Management", "Ignite Plasma Torch", "Fusion Welding Inverter", "MillSpindle CNC Drive", "ForcePress Hydraulic Unit", "SpinMaster Centrifuge", "FeedSync Material Loader", "SortRight Diverter Arm", "LabelJet High-Speed Applicator", "PackBot Automated Sealer", "Xcelerator OS", "CloudSync Enterprise Bridge", "Enterprise-MES Execution System", "ScadaView Supervision Software", "NetSecure Industrial Firewall", "Twin Digital Simulation Tool", "Predict AI Maintenance Suite", "DesignStudio Engineering Environment", "Script Automated Logic Editor", "Optima Yield Optimizer", "Query Production Database", "LinkNet Proprietary Bus", "Connect Universal API Hub", "Learn-ML Machine Learning Module", "Audit Compliance Tracker", "ReportGen Analytics Engine", "Standard Quality Management", "Sigma Lean Process Manager", "FlowVSM Value Stream Mapper", "SafeCrypt Encryption Module", "Shield NEMA-Rated Enclosure", "Guard Light Curtain Array", "E-Stop Safety Disconnect", "Lock Magnetic Interlock", "Fencing Perimeter Guard", "Breaker Power Distribution", "FanCool Thermal Exhaust", "RackMount Server Cabinet", "Tray Cable Management", "SecureGate Access Control", "Safeguard Zone Monitor", "FireWatch Thermal Detection", "BlastShield Enclosure", "AntiVibe Mounting Base", "ConduitX Hardened Piping", "CleanAir Filtration Unit", "IsoMount Vibration Dampener", "GroundWire Continuity Tester", "PowerGuard Surge Protector", "SealTight Gasket Series", "MicronPrecision Caliper", "LaserTrack 3D Scanner", "DepthMaster Probe", "ProfileX Laser Profiler", "SurfaceScan Roughness Tester", "WeightNode Industrial Scale", "DensityMeter Pro", "HardnessCore Tester", "CheckPoint Metrology Station", "VolumePulse Ultrasonic Volume", "GaugeMate Calibration Tool", "SpecCheck Material Analyzer", "X-RayVision Internal Inspector", "OpticalComparator Pro", "EdgeDetect Digital Micrometer", "BatchAudit Sampler", "VeriSize Dimensional Checker", "FlowVerify Calibration Rig", "ForceVerify Load Cell", "TrueAlign Laser Level", "LiftForce Pallet Jack", "HoverFreight Air Bearing", "SortMaster Automated Diverter", "PickPoint Bin System", "TrackWay Linear Guide", "CartRun AGV Interface", "StackPro Automated Stacker", "WrapStrong Pallet Wrapper", "DockSync Loading Bay Control", "CraneMaster Overhead Controller", "HoistLink Digital Chain", "BeltTension Smart Pulley", "BunkerLevel Silo Sensor", "ScaleBridge Truck Weigher", "PackFlow Accumulation Table", "RoutePlan Logistics Engine", "BinSense Inventory Sensor", "PalletScan RFID Gate", "FreightFlow Management Software", "LoadBalance Weight Distribution", "HydroPress Pump", "TempSteady Heat Exchanger", "ValveCore Butterfly Valve", "FilterFlow Particulate System", "SteamMaster Pressure Regulator", "ChillStream Refrigeration Unit", "MixMaster Industrial Agitator", "PureDose Chemical Doser", "TankSight Level Transmitter", "BoilerPoint Control Unit", "NozzleJet Precision Spreader", "VacuForce Suction Generator", "FlowRatio Blending Valve", "ThermalGuard Insulation Wrap", "CryoLink Low-Temp Valve", "DrainMaster Automatic Purge", "SolenoidX Direct Actuator", "DuctFlow Ventilation Controller", "MistAway Oil Mist Collector", "SprayPulse Coating Unit", "L100 Micro-Controller", "L500 Mid-Range PLC", "L900 Enterprise CPU", "X-Series Expansion Bus", "I/O-8 High Density Module", "AC-Drive 400V", "DC-Servo 24V", "RJ-Industrial Connector", "DIN-Rail Mount Power", "FiberLink Data Cable", "Protocore Communication Card", "AnalogPulse Signal Isolate", "DigitalSnap Relay", "OptoCouple Isolation Module", "BusTerminator Pro", "ShieldWire EMI Cable", "PhaseLink Three-Phase Monitor", "StepDrive Micro-Stepper", "HMI-Lite Panel", "EdgeCompute Node"],
                              'Memory':["HBM4 Next-Gen Stack", "DRAM-X High Capacity", "NAND-Pro 300-Layer", "SRAM-Link Fast Cache", "NVMe-Industrial SSD", "MRAM-Persistent Core", "ReRAM Low-Power Module", "DDR6 High-Speed Interface", "LPDDR6 Mobile Hub", "FRAM-Endurance Chip", "FlashVault Secure Drive", "PCM-Phase Change Unit", "SyncData Buffer", "OptiCache 5.0", "Enterprise-SSD Rack", "SOCAMM2 Hybrid Module", "Xtacking 4.0 Vertical Flash", "CBA-Bonded Storage Array", "Penta-Level Cell Drive", "Inference Context Memory", "KV-Cache Acceleration Unit", "Spectrum-X Context Fabric", "Rubin HBM-Stack", "BlueField-4 DPU Storage", "Ultra-Bandwidth Bridge", "Axiom Industrial SD", "PureState Flash Array", "Velocity DDR5 Interface", "Pathfinder Data Buffer", "CoreVault Embedded Flash", "GridStore Network RAM", "SwiftCache Micro-Module", "Atlas Topology Storage", "ShiftStation Memory Card", "Prime-DRAM Controller", "FlexStorage Expansion", "Nexus Memory Gateway", "TaskMaster Sequencer RAM", "Omni-Data Storage Hub", "Matrix Multi-Die Stack", "Insight Vision Buffer", "Proximity Data Cache", "Thermocore Log Memory", "FlowScan Data Logger", "Presstige Calibration RAM", "LevelMax History Storage", "BeamLine Array Cache", "Trace RFID Tag Memory", "CodeScan Imager Buffer", "Axis360 Encoder Log", "LoadForce Gauge Storage", "G-Sense Vibration Log", "AcoustiX Audio Buffer", "Humidistat History RAM", "Spectral Analysis Storage", "PureSense Air Log", "Echo Rangefinder Cache", "Vista Surveillance Drive", "Halo Safety Log", "PointIR Thermal Memory", "ViewPort HMI Cache", "Glass Touchscreen RAM", "Dash Analytics Buffer", "VizPro Display Memory", "Status Indicator Log", "Alert Notification RAM", "Command Console Storage", "Grafix Design Buffer", "PanelMate Interface RAM", "Beacon Status Memory", "HMI-1000 Video RAM", "O-Panel Local Storage", "Visionary AR Buffer", "TouchPoint System RAM", "DirectView Console Drive", "OmniDashboard Cache", "SignalCore Light Memory", "Broadside Message RAM", "VividPanel 8K Buffer", "UserNode Access Storage", "Kinetic Servo Cache", "VFD-X Drive Memory", "Actuon Position RAM", "Grip Robotic Buffer", "Arm-10 Kinematic Storage", "Torque Gearbox Log", "Phase Power Memory", "FlowValve Actuator RAM", "Transcon Motor Storage", "Stepper Driver Cache", "Chiller-Pro System RAM", "Ignite Plasma Log", "Fusion Welding Storage", "MillSpindle CNC RAM", "ForcePress Load Memory", "SpinMaster Logic Cache", "FeedSync Loader Buffer", "SortRight Diverter RAM", "LabelJet Applicator Storage", "PackBot Sealer Memory", "Xcelerator OS Drive", "CloudSync Bridge RAM", "Enterprise-MES Database", "ScadaView History Drive", "NetSecure Firewall Log", "Twin Digital Mirror RAM", "Predict AI Training Stack", "DesignStudio Asset RAM", "Script Logic Storage", "Optima Yield Database", "Query Industrial SQL Drive", "LinkNet Bus Memory", "Connect API Cache", "Learn-ML Inference Stack", "Audit Compliance Log", "ReportGen Analytics RAM", "Standard Quality Storage", "Sigma Lean Metric RAM", "FlowVSM Mapper Cache", "SafeCrypt Key Storage", "Shield Enclosure Log", "Guard Safety Curtains RAM", "E-Stop Event Logger", "Lock Interlock History", "Fencing Perimeter RAM", "Breaker Power Log", "FanCool Thermal RAM", "RackMount Server Storage", "Tray Cable Log", "SecureGate Access RAM", "Safeguard Zone Memory", "FireWatch Thermal Drive", "BlastShield Event RAM", "AntiVibe Mounting Log", "ConduitX Hardened Storage", "CleanAir Filter Memory", "IsoMount Dampener RAM", "GroundWire Continuity Log", "PowerGuard Surge RAM", "SealTight Gasket Log", "MicronPrecision RAM", "LaserTrack 3D Cache", "DepthMaster Probe RAM", "ProfileX Laser Storage", "SurfaceScan Tester RAM", "WeightNode Scale Memory", "DensityMeter Data RAM", "HardnessCore Log", "CheckPoint Station RAM", "VolumePulse Data Storage", "GaugeMate Calibration RAM", "SpecCheck Analyzer RAM", "X-RayVision Internal RAM", "OpticalComparator Storage", "EdgeDetect Micrometer RAM", "BatchAudit Sampler RAM", "VeriSize Dimension RAM", "FlowVerify Rig Storage", "ForceVerify Load RAM", "TrueAlign Laser RAM", "LiftForce Jack Memory", "HoverFreight Bearing RAM", "SortMaster Diverter Storage", "PickPoint Bin RAM", "TrackWay Guide Log", "CartRun AGV RAM", "StackPro Stacker Storage", "WrapStrong Pallet RAM", "DockSync Bay Storage", "CraneMaster Overhead RAM", "HoistLink Chain Memory", "BeltTension Pulley RAM", "BunkerLevel Silo RAM", "ScaleBridge Truck Storage", "PackFlow Table RAM", "RoutePlan Engine RAM", "BinSense Inventory RAM", "PalletScan RFID RAM", "FreightFlow Management RAM", "LoadBalance Weight RAM", "HydroPress Pump RAM", "TempSteady Exchanger RAM", "ValveCore Butterfly RAM", "FilterFlow System RAM", "SteamMaster Pressure RAM", "ChillStream Refrigeration RAM", "MixMaster Agitator RAM", "PureDose Chemical RAM", "TankSight Level RAM", "BoilerPoint Control RAM", "NozzleJet Spreader RAM", "VacuForce Suction RAM", "FlowRatio Blending RAM", "ThermalGuard Wrap RAM", "CryoLink Low-Temp RAM", "DrainMaster Purge RAM", "SolenoidX Actuator RAM", "DuctFlow Ventilation RAM", "MistAway Collector RAM", "SprayPulse Coating RAM", "M-100 Micro-RAM", "M-500 Mid-Range Flash", "M-900 Enterprise SSD", "B-Series Expansion Bus RAM", "Flash-8 High Density", "DDR-Drive 400V", "Servo-Cache 24V", "Industrial-RJ Link Memory", "DIN-Power Mount RAM", "FiberLink Data Storage"],
                              'Analog':["SignalPrime Converter", "WaveForm Modulator", "PureBridge Strain Gauge", "IsoAmp Isolated Amplifier", "FlowSense Signal Conditioner", "Axiom Analog Input", "FluxGate Current Sensor", "ThermalCouple Interface", "VoltLine Precision Reference", "PhaseSync Oscillator", "Spectral-A Signal Filter", "Velocity Transducer Node", "Helix Coil Driver", "OmniBuffer Impedance Matcher", "AcoustiCore Audio Preamp", "Pathfinder Signal Router", "UnityGain Line Driver", "EchoTune Ultrasonic Driver", "SightLine Video Decoder", "LinearPeak Comparator", "DraftMaster Pressure Transmit", "CurrentLoop 4-20mA Hub", "Resonance Feedback Loop", "FrequencyShift Converter", "AnalogMatrix Switcher", "OptoIsolated Gate Driver", "DifferentialPro Link", "SignalVault Data Logger", "LevelShift Voltage Matcher", "PulseWidth Signal Modulator", "WideBand RF Interface", "NanoVolt Precision Meter", "MicroAmp Leakage Detector", "SineGen Wave Generator", "AudioPath Mixer", "InductoCore Coil Monitor", "CapacitorBank Controller", "VariableGain Multiplier", "ZeroDrift Operational Amp", "LowNoise Signal Chain", "A-100 Discrete Input", "A-500 Signal Processor", "A-900 High-Speed ADC", "SyncPhase Clock Gen", "BridgeBalance Module", "TraceAnalog RFID Interface", "HydroSignal Fluid Monitor", "VibeCore Acceleration Amp", "RangeLink Proximity Driver", "SpectralLink UV Sensor"],
                              'Power':["VoltGuard Surge Protector", "AmpMaster Load Balancer", "PowerPrime Supply Unit", "Kinetic Energy Storage", "GridSafe Circuit Breaker", "PhaseShift Inverter", "WattWatch Energy Meter", "TerraGround Earth Monitor", "FluxDensity Transformer", "SolarNexus PV Controller", "BatteryCore Management System", "OptiLoad Demand Manager", "PureSine DC-AC Converter", "ThermalSafe Power Module", "ChargeLink EV Station", "StepDown Voltage Regulator", "CurrentForge Welding Power", "HighVolt Static Dissipator", "PowerVault UPS System", "SmartBus Distribution Rail", "MegaWatt Station Master", "CellBalance Battery Array", "InductorMax Power Filter", "SwitchGear Matrix", "FuseLink Digital Protector", "SolidState Power Relay", "PowerStream Bus Coupler", "IsoPower Isolated Supply", "EcoFlow Energy Recovery", "PeakDemand Peak Shaver", "PowerGrid Network Gateway", "MotiveForce Traction Drive", "DC-Master Industrial Supply", "AC-Drive High Torque", "DirectCurrent Rectifier", "SuperCap Energy Buffer", "PowerShield EMI Filter", "BreakerPoint Smart Switch", "LoadSense Current Monitor", "MultiPhase Sync Unit", "P-100 Compact Supply", "P-500 Heavy Duty Drive", "P-900 Grid Scale Inverter", "ActiveHarmonic Filter", "SoftStart Motor Guard", "DeltaWye Phase Converter", "PowerPath Redundant Switch", "ArcFlash Detection Node", "SafeVolt Isolation Barrier", "GenSet Synchronizer"],
                              }
    
    # When uniform p_key use flattened sampling; otherwise compute per-product probabilities
    if p_key == [0.25, 0.25, 0.25, 0.25]:
        # If uniform distribution provided options are selected using the efficient numpy sampler
        product_name_options_chained = itertools.chain.from_iterable(product_classification_key.values())
        product_name_options = list(product_name_options_chained)
        local_bool = num_products > len(product_name_options)
        product_name = rng.choice(product_name_options, size=num_products, replace=local_bool)
    else:
        # Build a flattened product list and a matching probability vector
        product_name_options = []
        product_probability = []
        
        logic_products = [item for item in product_classification_key['Logic']]
        memory_products = [item for item in product_classification_key['Memory']]
        analog_products = [item for item in product_classification_key['Analog']]
        power_products = [item for item in product_classification_key['Power']]
        
        all_products_grouped = [logic_products, memory_products, analog_products, power_products]

        i = 0
        for sublist in all_products_grouped:
            num_prod_per_family = len(sublist)
            # Distribute family mass evenly across members
            temp_probability = [p_key[i] / num_prod_per_family]
            
            probability_list = temp_probability * num_prod_per_family
            product_probability.append(probability_list)

            for item in sublist:
                # Build product name list for sampling
                product_name_options.append(item)

            i += 1
        
        local_bool = num_products > len(product_name_options)

        # Normalize final probability vector
        product_probability_flat = itertools.chain.from_iterable(product_probability)
        product_prob_final = np.fromiter(product_probability_flat, dtype=np.float64)
        product_prob_final = product_prob_final / np.sum(product_prob_final, dtype=np.float64)

        # Sample product names using the computed per-product probabilities
        product_name = rng.choice(product_name_options, size=num_products, replace=local_bool, p=product_prob_final)
    
    # Product identifiers and families
    product_id = rng.choice(int(1e9), size=num_products, replace=False)
    product_family = []
    for string in product_name:
        key = next((k for k, v in product_classification_key.items() if string in v), None)
        product_family.append(key)

    # Yield sampling and quality sampling fraction
    target_yield_rate = local_rng.beta(a=a, b=b, size=num_products)
    sample_per_yield = local_rng.uniform(0.05, 0.3,size=num_products)

    # dim_product DataFrame
    df1 = pd.DataFrame(
        {
            'product_id': product_id,
            'product_name': product_name,
            'product_family': product_family,
            'target_yield_rate': target_yield_rate,
            'sample_per_yield': sample_per_yield,
            }
        )

    # -----------------------------------------------------------------------------
    # dim_process generation
    # -----------------------------------------------------------------------------
    # Process names pool and sampling
    process_name_options = ["Ingot Slicing", "Silicon Carbonization", "Epitaxial Substrate Growth", "Diamond-Wire Wafering", "High-Purity Lapping", "Chemical-Mechanical Planarization", "Atomic-Flat Polishing", "Wafer Laser Marking", "Edge Grinding", "Deionized Water Scrubbing", "Oxygen-Free Annealing", "Thermal Stress Relief", "Surface Passivation", "Crystal Seed Orientation", "Hydrogen Flaking", "Multi-Wire Slicing", "Mirror-Finish Buffing", "Plasma Surface Prep", "Substrate Doping", "Extreme Ultraviolet Exposure", "ArF Immersion Lithography", "Photoresist Spin-Coating", "Deep Ultraviolet Scanning", "Mask Alignment", "Electron Beam Patterning", "Soft Baking", "Post-Exposure Baking", "Positive Resist Development", "Negative Resist Hardening", "Resist Edge-Bead Removal", "Direct-Write Lithography", "Nano-Imprint Lithography", "Multi-Patterning Double Exposure", "Quadruple Patterning SAQP", "Hardmask Deposition", "Photoresist Ashing", "Reticle Inspection", "Overlay Correction", "Metrology Alignment", "Atomic Layer Deposition", "Plasma-Enhanced Chemical Vapor Deposition", "Physical Vapor Deposition", "Magnetron Sputtering", "High-Density Plasma Deposition", "Thermal Oxidation", "Low-Pressure CVD", "Metal-Organic CVD", "Electroplating ECD", "Barrier Layer Seed Deposition", "Copper Electrochemical Plating", "Silicon Nitride Passivation", "Interlayer Dielectric Formation", "High-k Dielectric Deposition", "Aluminum Metallization", "Tungsten Plug Deposition", "Epitaxial Silicon Growth", "Thin Film Annealing", "Vapor-Phase Epitaxy", "Ion Beam Deposition", "Deep Reactive Ion Etching", "Plasma Etching", "Atomic Layer Etching", "Thermal ALE", "Buffered Oxide Etching", "Isotropic Wet Etching", "Anisotropic Dry Etching", "Chlorine-Based Etch", "Fluorine-Based Plasma Strip", "Ion Beam Milling", "Radical Etching", "Poly-Silicon Etching", "Via Trench Sculpting", "Metal Line Etch", "Gate Stack Etching", "Spacer Removal", "Photoresist Wet Stripping", "Selective Material Removal", "Surface Leveling CMP", "Nano-Channel Etch", "High-Energy Ion Implantation", "Phosphorus N-Type Doping", "Boron P-Type Doping", "Arsenic Ion Injection", "Shallow Junction Formation", "Retardation Layer Implantation", "Halo Doping", "Well Formation", "Source-Drain Extension Doping", "Flash Lamp Annealing", "Rapid Thermal Processing", "Laser Spike Annealing", "Dopant Activation", "Crystal Damage Repair", "Silicon-on-Insulator Modification", "Plasma Doping", "Deep Well Injection", "Threshold Voltage Adjustment", "Sub-Surface Ion Scoping", "Copper Damascene Processing", "Dual-Damascene Via Formation", "Interconnect Wiring", "Multi-Layer Metal Stacking", "Barrier Layer Sputtering", "Vias and Trenches Filling", "Contact Hole Opening", "Pad Metallization", "Passivation Layer Sealing", "Low-k Insulator Coating", "Wire Bonding Pad Formation", "Redistribution Layer Routing", "Under-Bump Metallization", "Solder Bump Reflow", "Flip-Chip Interconnect", "Through-Silicon Via Etching", "Micro-Bump Formation", "Wafer Back-Grinding", "Laser Dicing", "Stealth Dicing", "Die Pick-and-Place", "Epoxy Die Attachment", "Conductive Paste Bonding", "Thermosonic Wire Bonding", "Ultrasonic Capillary Bonding", "Flip-Chip Vacuum Alignment", "Underfill Dispensing", "Capillary Underfill Flow", "Resin Encapsulation", "Injection Molding", "Compression Molding", "Laser Part Marking", "Lead-Frame Trimming", "Ball Grid Array Soldering", "Heat Sink Attachment", "Component Curing", "Vacuum De-Gassing", "Final Structural Bond"] + [f"Layer-{i} {step}" for i in range(1, 151) for step in ["Photolithography", "Plasma Etch", "CVD Deposition", "CMP Planarization", "Ion Implantation"]]
    
    # Process ID sampling and pairing with products
    process_id = rng.choice(int(1e9), size=num_products, replace=False)
    process_name = rng.choice(process_name_options, size=num_products)
    product_id_proc = rng.choice(product_id, size=num_products, replace=False)

    # dim_process DataFrame
    df2 = pd.DataFrame(
        {
            'process_id': process_id,
            'process_name': process_name,
            'product_id': product_id_proc,
            }
        )

    # -----------------------------------------------------------------------------
    # dim_process_route generation
    # -----------------------------------------------------------------------------
    # Determine number of steps per process (random integer between min_steps and max_steps)
    step_key = {ID: rng.integers(low=min_steps, high=max_steps, endpoint=True) for ID in process_id}
    
    # Expand each process into its sequence of steps
    process_id_PR = []
    step_number = []
    for k, v in step_key.items():
        for step in range(1, v+1): 
            process_id_PR.append(k)
            step_number.append(step)
    num_process_steps = len(process_id_PR)
    
    # Map machine-level cycle times and types to route steps by sampling machines
    cycle_time_S = dim_machine['ideal_cycle_time'].values
    machine_id_S = dim_machine['machine_id'].values
    machine_type_S = dim_machine['machine_type'].values

    # Randomized selection of possible machine and process step pairs
    machine_id = rng.choice(machine_id_S, size=len(process_id_PR))

    # Pairing correct cycle times to newly generated process steps with machine types defined
    cycle_time_key = {machine_id_S[x]: cycle_time_S[x] for x in range(len(machine_id_S))}
    type_key = {machine_id_S[x]: machine_type_S[x] for x in range(len(machine_id_S))}
    step_cycle_time = []
    machine_type = []
    for id_machine in machine_id:
        time_val = cycle_time_key[id_machine]
        type_val = type_key[id_machine]
        step_cycle_time.append(time_val)
        machine_type.append(type_val)
    process_route_id = rng.choice(int(1e9), size=num_process_steps, replace=False)

    # dim_process_route DataFrame
    df3 = pd.DataFrame(
        {
            'process_route_id': process_route_id,
            'process_id': process_id_PR,
            'step_number': step_number,
            'machine_type': machine_type,
            'step_cycle_time': step_cycle_time,
            }
        )

    # Output
    return df1, df2, df3

# -----------------------------------------------------------------------------
# fact_work_order generation
# -----------------------------------------------------------------------------
def create_work_orders(
    dim_product: pd.DataFrame,
    fact_work_order: Optional[pd.DataFrame] = None,
    num_work_orders: int=1,
    rngs: Optional[dict] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Create and/or append synthetic work orders.

    Description
    ----------
    Generates `num_work_orders` new work orders for products in `dim_product`.
    If `fact_work_order` is provided, new rows are appended while preserving
    the existing index structure; if not provided, a new work order DataFrame
    is created.

    Behavior notes
    --------------
    - Work order IDs are generated with the prefix "WO-25" and are guaranteed
      unique with respect to the provided `fact_work_order`.
    - Start/due dates are sampled in 2025 and due dates include a small lead
      time (3-21 days) ensuring due > start.
    - `planned_quantity` and `batch_size` are sampled to create realistic
      batching behavior; `batch_size` uses a lower bound of 10 and an
      upper bound of half the planned quantity (or 11 to avoid zero).
    - The function returns a tuple: (updated_fact_work_order, new_work_orders)

    Parameters
    ----------
    dim_product : pandas.DataFrame
        Product table (used for sampling `product_id` and pulling target yields).
    fact_work_order : pandas.DataFrame, optional
        Existing work order table to append to; if None a new DataFrame is created.
    num_work_orders : int, optional
        Number of new work orders to generate (default 1).
    rngs : dict, optional
        Optional RNG dict; expected key: 'structure'.

    Returns
    -------
    tuple(pandas.DataFrame, pandas.DataFrame)
        (full_work_order_table, new_work_orders_table)
    """
    # Select RNG
    if not rngs:
        rng = local_rng
    else:
        rng = rngs['structure']
    
    # Initialize existing table if not provided
    if fact_work_order is None:
        fact_work_order = pd.DataFrame(columns=['work_order_id','product_id','planned_quantity','batch_size','start_date','due_date','priority'])
    
    exisiting_ids = set(fact_work_order['work_order_id'].values)
    generated_ids = set()

    # Set of priority options (0-3) for sampling
    priority_options = [0, 1, 2, 3]

    # Sample product IDs and pull corresponding target yield rates for work order generation
    product_id_S = dim_product['product_id'].values
    product_yield_S = dim_product['target_yield_rate'].values
    yield_rate_key = dict(zip(product_id_S, product_yield_S))

    # Date sampling window and safe end date (room for lead time)
    st_date, ed_date = np.datetime64('2025-01-01'), np.datetime64('2025-12-31')
    max_slack_allowed = 30
    safe_end_date = ed_date - np.timedelta64(max_slack_allowed, 'D')

    # Buffers for generated rows
    work_order_id = []
    start_date = []
    due_date = []
    planned_quantity = []
    batch_size = []
    priority = []
    product_id = []
    idx = []

    # Loop to generate specified number of work orders with unique IDs and realistic attributes
    for i in range(0, num_work_orders):
        # Ensure unique work_order_id
        while True:
            wo_id = 'WO-25' + str(rng.integers(low=1, high=1000000000))
            if wo_id in exisiting_ids or wo_id in generated_ids:
                # If pre-exisiting, generates a new work order id and checks again
                continue
            generated_ids.add(wo_id)
            work_order_id.append(wo_id)
            
            # Indexing logic to ensure new rows are appended correctly to existing table structure
            if i == 0:
                # If first run, appends to end of table (which will be index 0 if original table was empty)
                idx.append(len(fact_work_order))
                idx_temp = idx[0] + 1
                break
            else:
                # Create list of required indices for final table input
                idx.append(idx_temp)
                idx_temp += 1
                break

        # Sample realistic start/due dates and validate ordering
        start_date.append(rng.choice(pd.date_range(st_date, safe_end_date, freq='D').to_numpy()))
        lead_times = rng.integers(3,21)
        due_date.append(start_date[i] + np.timedelta64(lead_times, 'D'))
        if due_date[-1] <= start_date[-1]:
            raise ValueError(
                f"Invalid due date generated: start={start_date[-1]}, due={due_date[-1]}"
            )

        # Quantity and batching
        planned_quantity.append(rng.integers(low=100, high=2000))
        batch_size.append(rng.integers(low=10,high=max(11, planned_quantity[i] // 2)))
        priority.append(rng.choice(priority_options))
        product_id.append(rng.choice(product_id_S))
        
        # Map product IDs to their target yields for returned rows
        target_yield_rate = []
        for ID in product_id:
            target_yield_rate.append(yield_rate_key[ID])
        target_yield_rate = np.asarray(target_yield_rate)

    # New rows DataFrame (df1) and appended result (df)
    df1 = pd.DataFrame(
        {
            'work_order_id': work_order_id,
            'product_id': product_id,
            'planned_quantity': planned_quantity,
            'target_yield_rate': target_yield_rate,
            'batch_size': batch_size,
            'start_date': start_date,
            'due_date': due_date,
            'priority': priority,
            }, index = idx
        )

    # Check whether the original work order table used for input was empty
    if fact_work_order.empty:
        df = df1
    else:
        df = pd.concat([fact_work_order, df1])

    # Ouput
    return df, df1
