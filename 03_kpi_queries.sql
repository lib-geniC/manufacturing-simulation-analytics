/*
	Core KPIs:
		A) Machine Cycle Time
		B) Overall On-Time Delivery Rate / On-Time Delivery Rate by Product
		C) Throughput (per Day, Machine, Product)
		D) Lead-Time Distribution
		E) Longest Work Orders
		F) Product & Priority Segmentation (Lead-Time, Work Order Count)
		G) Completion Date Segmentation (Lead-Time)
	Advanced KPIs:
		A) Capacity Efficiency (Utilization vs Throughput)
		B) Failure Impact Index
		C) Flow Efficiency (Lean KPI)
		D) Bottleneck Analysis
*/

-- Set the intended Scenario ID for queries:
"SA-MP1005T365IA1800WP12560SE42R1"
"SB-MP1005T365IA1200WP00SE42R1"
"SC-MP1005T365IA1200WP8060SE42R1"
"VSA-MP1005T365IA1800WP12560SE101R1"
"VSB-MP1005T365IA1200WP00SE101R1"
"VSC-MP1005T365IA1200WP8060SE101R1"
"VSA-MP1005T365IA1800WP12560SE2024R1"
"VSA-MP1005T365IA1800WP12560SE777R1"
SET SESSION vars.scenario_id = 'VSA-MP1005T365IA1800WP12560SE777R1';
---------------------------------------------------------------------------------------------------------------
---- Scenario Summary Reports:
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
CREATE OR REPLACE FUNCTION analytics.get_scenario_kpis(p_scenario_id TEXT)
RETURNS TABLE (
    scenario_id TEXT,
    scenario_name TEXT,
    total_days INT,
    total_work_orders INT,
    avg_waiting_time NUMERIC,
    avg_processing_time NUMERIC,
    on_time_delivery_rate NUMERIC,
    median_lead_time_days NUMERIC,
    p90_lead_time_days NUMERIC,
    avg_utilization_pct NUMERIC,
    total_failures INT,
    avg_plant_yield NUMERIC,
    avg_flow_efficiency NUMERIC,
    avg_daily_throughput NUMERIC,
	
    created_at TIMESTAMP
)
LANGUAGE sql
AS $$
WITH ds AS (
    SELECT scenario_name
    FROM staging.dim_scenario
    WHERE scenario_id = p_scenario_id
),
awvpt AS (
    SELECT
        AVG(work_order_lead_time - total_ideal_cycle_time) AS avg_waiting_time,
        AVG(total_ideal_cycle_time) AS avg_processing_time
    FROM analytics.v_work_order_lifecycle
    WHERE scenario_id = p_scenario_id
),
otdr AS (
    SELECT
        SUM(on_time_flag)::NUMERIC / COUNT(*)::NUMERIC AS on_time_delivery_rate
    FROM analytics.kpi_work_order_ontime_delivery_rate
    WHERE scenario_id = p_scenario_id
),
ltd AS (
    SELECT
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lead_time_days) AS median_lead_time_days,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY lead_time_days) AS p90_lead_time_days
    FROM analytics.kpi_work_order_lead_time
    WHERE scenario_id = p_scenario_id
),
aup AS (
    SELECT
        AVG(utilization_pct) AS avg_utilization_pct,
        SUM(num_failures) AS total_failures,
        AVG(machine_yield_rate) AS avg_plant_yield
    FROM analytics.v_machine_utilization
    WHERE scenario_id = p_scenario_id
),
fea AS (
    SELECT
        AVG(total_ideal_cycle_time / work_order_lead_time) AS avg_flow_efficiency
    FROM analytics.v_work_order_lifecycle
    WHERE scenario_id = p_scenario_id
),
dta AS (
    SELECT
        COUNT(*)::NUMERIC / COUNT(DISTINCT completion_date)::NUMERIC AS avg_daily_throughput
    FROM analytics.kpi_throughput_daily
    WHERE scenario_id = p_scenario_id
),
fwo AS (
    SELECT
        COUNT(DISTINCT work_order_id) AS total_work_orders
    FROM staging.fact_work_order
    WHERE scenario_id = p_scenario_id
),
tcal AS (
    SELECT
        MAX(sim_day_floor) AS total_days
    FROM analytics.v_time_calendar
    WHERE scenario_id = p_scenario_id
)
SELECT
    p_scenario_id AS scenario_id,
    ds.scenario_name,
    tcal.total_days,
    fwo.total_work_orders,
    awvpt.avg_waiting_time,
    awvpt.avg_processing_time,
    otdr.on_time_delivery_rate,
    ltd.median_lead_time_days,
    ltd.p90_lead_time_days,
    aup.avg_utilization_pct,
    aup.total_failures,
    aup.avg_plant_yield,
    fea.avg_flow_efficiency,
    dta.avg_daily_throughput,

    NOW() AS created_at
FROM ds, tcal, fwo, awvpt, otdr, ltd, aup, fea, dta;
$$;

CREATE MATERIALIZED VIEW analytics.mv_scenario_kpi_summary AS
SELECT k.*
FROM staging.dim_scenario s
CROSS JOIN LATERAL analytics.get_scenario_kpis(s.scenario_id) k;
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
SELECT *
FROM analytics.mv_scenario_kpi_summary
WHERE scenario_id = current_setting('vars.scenario_id')::TEXT;
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
-- For Reference: Optional KPIs that May be of Relevance
	-- Arrival rate
	SELECT
	    COUNT(*) / MAX(work_order_start_time) AS arrivals_per_second,
		(COUNT(*) / MAX(work_order_start_time) * 86400) AS arrivals_per_day,
		(COUNT(*) / MAX(work_order_start_time) * 604800) AS arrivals_per_week,
		(COUNT(*) / MAX(work_order_start_time) * 2628000) AS arrivals_per_month,
		(COUNT(*) / MAX(work_order_start_time) * 7776000) AS arrivals_per_quarter
	FROM staging.fact_work_order;
	-- Throughput rate
	SELECT
	    COUNT(*) / MAX(work_order_end_time) AS completions_per_second,
		(COUNT(*) / MAX(work_order_end_time) * 86400) AS completions_per_day,
		(COUNT(*) / MAX(work_order_end_time) * 604800) AS completions_per_week,
		(COUNT(*) / MAX(work_order_end_time) * 2628000) AS completions_per_month,
		(COUNT(*) / MAX(work_order_end_time) * 7776000) AS completions_per_quarter
	FROM staging.fact_work_order
	WHERE work_order_end_time IS NOT NULL;
	-- Work order start count by day
	SELECT
		start_date,
		SUM(COUNT(*)) OVER(PARTITION BY start_date) AS work_order_count
	FROM staging.fact_work_order
	GROUP BY start_date;
---------------------------------------------------------------------------------------------------------------
---- Core Machine, Product, and Date Specific KPIs:
-- Standard
/*
KPI: Machine Cycle Time

Purpose:
    Calculate the average cycle time spent processing a product batch versus the expected cycle time.

Grain:
    One row per machine_id.

Key Metrics:
    - avg_machine_cycle_time

Interpretation:
    Average observed cycle duration per machine compared to its ideal cycle time.
    Higher deviations may indicate inefficiencies or capacity constraints.
*/
SELECT 
	machine_id,
	ideal_cycle_time,
	AVG(cycle_duration) AS avg_machine_cycle_time
FROM analytics.kpi_cycle_time_performance
WHERE scenario_id = current_setting('vars.scenario_id')::TEXT
GROUP BY machine_id, ideal_cycle_time
ORDER BY machine_id;
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
/*
KPI: Product Family On-Time Delivery Rate

Purpose:
    Calculate on-time delivery performance for each product family across all work orders.

Grain:
    One row per product family.

Key Metric:
    - on_time_delivery_rate

Interpretation:
    Ratio of work orders completed on or before their due date.
*/
SELECT
    product_family,
    SUM(on_time_flag)::NUMERIC / COUNT(*)::NUMERIC AS on_time_delivery_rate
FROM analytics.kpi_work_order_ontime_delivery_rate
WHERE scenario_id = current_setting('vars.scenario_id')::TEXT
GROUP BY product_family
ORDER BY on_time_delivery_rate ASC;
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
/*
KPI: Daily Throughput

Purpose:
    Calculate the throughput (completed units and orders) for each production day.

Grain:
    One row per day of production completed.

Key Metric:
    - total_units_produced
	- total_work_orders_completed
	- effective_yield_rate

Interpretation:
    Shows daily production output and yield performance.
    Useful for identifying volume variability and operational trends over time.
*/
SELECT
	completion_date,
	COUNT(DISTINCT work_order_id) AS total_work_orders_completed,
	SUM(units_approved) AS total_units_produced,
	SUM(planned_quantity) AS total_units_planned,
	SUM(units_approved)::NUMERIC / SUM(planned_quantity)::NUMERIC AS effective_yield_rate
FROM analytics.kpi_throughput_daily
WHERE scenario_id = current_setting('vars.scenario_id')::TEXT
GROUP BY completion_date
ORDER BY completion_date ASC;
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
/*
KPI: Product Family Throughput

Purpose:
    Calculate the throughput (completed units and orders) for each product family.

Grain:
    One row per product family.

Key Metric:
    - total_units_produced
	- effective_yield_rate

Interpretation:
    Ratio of the total number of units produced and the planned quantity of units for each product family.
*/
SELECT
	product_family,
	SUM(units_approved) AS total_units_produced,
	SUM(planned_quantity) AS total_units_planned,
	SUM(units_approved)::NUMERIC / SUM(planned_quantity)::NUMERIC AS effective_yield_rate
FROM analytics.kpi_throughput_product
WHERE scenario_id = current_setting('vars.scenario_id')::TEXT
GROUP BY product_family
ORDER BY effective_yield_rate ASC;
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
/*
KPI: Machine Throughput

Purpose:
    Calculate the throughput (completed units and orders) for each machine.

Grain:
    One row per machine.

Key Metric:
	- total_quantity_input
    - total_units_produced
	- effective_yield_rate

Interpretation:
    Ratio of the total number of units produced and the total number of units input for each machine.
*/
SELECT
	machine_id,
	SUM(step_input_quantity) AS total_quantity_input,
	SUM(units_approved) AS total_units_produced,
	SUM(units_approved)::NUMERIC / SUM(step_input_quantity)::NUMERIC AS effective_yield_rate
FROM analytics.kpi_throughput_machine
WHERE scenario_id = current_setting('vars.scenario_id')::TEXT
GROUP BY machine_id
ORDER BY total_units_produced DESC;
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
/*
KPI: Top 20 Longest Work Orders

Purpose:
    Pull the top 20 longest work order lead times.

Grain:
    One row per work order.

Key Metric:
	- lead_time_days

Interpretation:
    Total number of days it took for a work order to be completed from start to finish.
*/
SELECT
    work_order_id,
    product_family,
    priority,
    lead_time_days
FROM analytics.kpi_work_order_lead_time
WHERE scenario_id = current_setting('vars.scenario_id')::TEXT
ORDER BY lead_time_days DESC
LIMIT 20;
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
/*
KPI: Product Family & Priority Segmentation

Purpose:
    Calculate the average lead time in days and the total number of work orders completed by product and priority.

Grain:
    One row per product and priority combination.

Key Metric:
    - avg_lead_time_days
	- num_work_orders

Interpretation:
    Tracks how average lead time evolves product families and priority levels,
    highlighting systemic improvements or degradation.
*/
SELECT
    product_family,
    priority,
    AVG(lead_time_days) AS avg_lead_time_days,
    COUNT(*) AS num_work_orders
FROM analytics.kpi_work_order_lead_time
WHERE scenario_id = current_setting('vars.scenario_id')::TEXT
GROUP BY product_family, priority
ORDER BY avg_lead_time_days DESC;
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
/*
KPI: Completion Date Segmentation

Purpose:
    Calculate the average lead time in days by completion date.

Grain:
    One row per completion date

Key Metric:
    - avg_lead_time_days

Interpretation:
    Tracks how average lead time evolves over time,
    highlighting systemic improvements or degradation.
*/
SELECT
    completion_date,
    AVG(lead_time_days) AS avg_lead_time_days
FROM analytics.kpi_work_order_lead_time
WHERE scenario_id = current_setting('vars.scenario_id')::TEXT
GROUP BY completion_date
ORDER BY completion_date;
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
/*
KPI: Mean-Time-Between Failures

Purpose:
	Identify the average amount of operting time of each machine type prior to failure.

Grain:
	One row per machine.

Key Metric:
	- MTBF_Interval
	- MTBF_Total_Time

Interpretation:
	Average amount of time it takes machines of a specific type to fail during production.
*/
SELECT
	machine_id,
	machine_type,
	AVG(avg_operating_duration_till_failure) AS MTBF_Interval,
	SUM((total_process_f - (total_downtime_f + total_final_stretch_f)))::NUMERIC / NULLIF(SUM(num_failures), 0) AS MTBF_Total_Time
FROM analytics.v_machine_utilization
WHERE scenario_id = current_setting('vars.scenario_id')::TEXT
GROUP BY
	machine_id,
	machine_type;
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 

-- Advanced:

/*
KPI: Downtime Ratio

Purpose:
    Identify the ratio of downtime vs total time spent processing for each machine.

Grain:
    One row per machine.

Key Metric:
    - downtime_ratio = total_downtime / total_processing_time

Interpretation:
    Lower values indicate less time spent in repair or out-of-service during processing.
*/
SELECT
	machine_id,
	total_downtime / NULLIF(total_processing_time, 0) AS downtime_ratio
FROM analytics.v_machine_utilization
WHERE scenario_id = current_setting('vars.scenario_id')::TEXT
ORDER BY downtime_ratio DESC NULLS LAST;
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
/*
KPI: Flow Efficiency

Purpose:
    Identify flow inefficiency by comparing theoretical processing time to actual end-to-end lead time.

Grain:
    One row per work order.

Key Metric:
    - flow_efficiency = total_ideal_cycle_time / work_order_lead_time

Interpretation:
    Lower values indicate excessive waiting, blocking, or queuing.
*/
SELECT
	work_order_id,
	work_order_lead_time,
	total_ideal_cycle_time,
	total_ideal_cycle_time / work_order_lead_time AS flow_efficiency
FROM analytics.v_work_order_lifecycle
WHERE scenario_id = current_setting('vars.scenario_id')::TEXT
ORDER BY flow_efficiency ASC;
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 
/*
KPI: Capacity Efficiency

Purpose:
    Identify capacity inefficiency by comparing utlization percent to total units approved.

Grain:
    One row per machine.

Key Metric:
    - utilization_pct
	- total_units_approved

Interpretation:
    Higher utilization percent alongside higher total units approved indicates higher efficiency.
*/
-- By Machine
SELECT
	machine_id,
	utilization_pct,
	total_units_approved
FROM analytics.v_machine_utilization
WHERE scenario_id = current_setting('vars.scenario_id')::TEXT
ORDER BY utilization_pct DESC NULLS LAST;
--
-- By Machine Type
SELECT machine_type,
       AVG(utilization_pct),
       MAX(utilization_pct),
       MIN(utilization_pct)
FROM analytics.v_machine_utilization
WHERE scenario_id = current_setting('vars.scenario_id')::TEXT
GROUP BY machine_type
ORDER BY AVG(utilization_pct) DESC;
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
/*
Analysis: Bottleck Identification

Purpose:
    Identify machines with utilization higher than 85% indicating failure may result in bottleneck.

Grain:
    One row per machine.

Key Metric:
    - utilization_pct
	- avg_cycle_time_difference

Interpretation:
    Machines with sustained high utilization are more likely to constrain flow.
    Elevated cycle time variance or downtime increases bottleneck risk.
*/
SELECT
	machine_id,
	machine_type,
	utilization_pct,
	avg_cycle_time_difference,
	total_downtime
FROM analytics.v_machine_utilization
WHERE utilization_pct > 0.85 AND scenario_id = current_setting('vars.scenario_id')::TEXT
ORDER BY utilization_pct DESC;
-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- 