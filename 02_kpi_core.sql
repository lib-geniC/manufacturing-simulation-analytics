-- KPI View Level
/*
KPI View: Cycle Time Performance

Purpose:
    Capture actual vs ideal cycle time performance at the production step level.
    Enables machine-level performance analysis, bottleneck identification,
    and cycle time variance evaluation.

Grain:
    One row per work order × machine × process step.

Key Fields:
    - ideal_cycle_time
    - cycle_duration
    - cycle_time_difference

Notes:
    This view is intentionally unaggregated to support downstream
    averaging, ranking, and variance analysis by machine or process.
*/

CREATE OR REPLACE VIEW analytics.kpi_cycle_time_performance AS
SELECT
	work_order_id,
	machine_id,
	process_id,
	process_route_id,
	ideal_cycle_time,
	cycle_duration,
	cycle_time_difference,

	scenario_id
FROM analytics.v_production_step_execution;

/*
KPI View: Work Order Lead Time

Purpose:
    Measure total end-to-end lead time for each completed work order.
    Supports delivery performance, prioritization analysis, and lead-time distribution metrics.

Grain:
    One row per work order.

Key Metrics:
    - lead_time_seconds
    - lead_time_days
    - priority
    - product_family

Notes:
    Lead time is calculated using simulation timestamps and converted
    to calendar days using sim_start_date as the reference point.
*/

CREATE OR REPLACE VIEW analytics.kpi_work_order_lead_time AS
SELECT
	work_order_id,
	product_id,
	product_family,
	priority,
	work_order_lead_time AS lead_time_seconds,
	(work_order_lead_time) / 86400.00 AS lead_time_days,
	start_date,
	due_date,
	(sim_start_date + (work_order_end_time || ' seconds')::INTERVAL)::DATE AS completion_date,

	scenario_id
FROM analytics.v_work_order_lifecycle;

/*
KPI View: On-Time Delivery Flag

Purpose:
    Determine whether each work order was completed on or before its due date.
    Serves as the base layer for on-time delivery rate calculations.

Grain:
    One row per work order.

Key Fields:
    - due_date
    - actual_end_date
    - on_time_flag (1 = on time, 0 = late)

Notes:
    Simulation timestamps are converted to calendar dates using sim_start_date.
    The on_time_flag enables flexible aggregation by product, priority, or time period.
*/

CREATE OR REPLACE VIEW analytics.kpi_work_order_ontime_delivery_rate AS
SELECT
	work_order_id,
	product_family,
	
	due_date,
	work_order_start_time,
	(sim_start_date + (work_order_start_time || ' seconds')::INTERVAL)::DATE AS actual_start_date,
	work_order_end_time,
	(sim_start_date + (work_order_end_time || ' seconds')::INTERVAL)::DATE AS actual_end_date,
	CASE
		WHEN 
			(sim_start_date 
				+ (work_order_end_time || ' seconds')::INTERVAL
			)::DATE <= due_date 
		THEN 1
		ELSE 0
	END AS on_time_flag,

	scenario_id
FROM analytics.v_work_order_lifecycle;

/*
KPI View: Machine Throughput Detail

Purpose:
    Capture unit-level production output and yield inputs by machine and process route.
    Supports machine throughput, yield, and bottleneck analysis.

Grain:
    One row per production step execution.

Key Fields:
    - step_input_quantity
    - units_approved
    - units_scrapped
    - machine_id

Notes:
    This view intentionally avoids aggregation to allow flexible
    machine-level and route-level throughput analysis.
*/

CREATE OR REPLACE VIEW analytics.kpi_throughput_machine AS
SELECT
	machine_id,
	process_route_id,
	batch_id,
	step_input_quantity,
	units_approved,
	units_scrapped,

	scenario_id
FROM analytics.v_production_step_execution;

/*
KPI View: Daily Throughput Base

Purpose:
    Provide work-order-level completion data for daily throughput analysis.

Grain:
    One row per completed work order.

Key Fields:
    - completion_date
    - units_approved
    - planned_quantity
    - work_order_lead_time

Notes:
    Completion date is derived from simulation timestamps.
    Aggregation to daily throughput occurs in downstream KPI queries.
*/

CREATE OR REPLACE VIEW analytics.kpi_throughput_daily AS
SELECT
	work_order_id,
	(sim_start_date + (work_order_end_time || ' seconds')::INTERVAL)::DATE AS completion_date,
	work_order_lead_time,
	units_approved,
	planned_quantity,

	scenario_id
FROM analytics.v_work_order_lifecycle
WHERE work_order_end_time IS NOT NULL
GROUP BY work_order_id, completion_date, work_order_lead_time, units_approved, planned_quantity, scenario_id;

/*
KPI View: Product Family Throughput Base

Purpose:
    Provide work-order-level output data segmented by product family.

Grain:
    One row per completed work order.

Key Fields:
    - product_family
    - completion_date
    - units_approved
    - planned_quantity

Notes:
    Designed to support yield and throughput comparisons across product families.
*/

CREATE OR REPLACE VIEW analytics.kpi_throughput_product AS
SELECT
	product_family,
	(sim_start_date + (work_order_end_time || ' seconds')::INTERVAL)::DATE AS completion_date,
	units_approved,
	planned_quantity,
	effective_yield_rate,

	scenario_id
FROM analytics.v_work_order_lifecycle
WHERE work_order_end_time IS NOT NULL;
	