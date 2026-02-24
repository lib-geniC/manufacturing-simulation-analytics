-- Query for Base Views 

CREATE MATERIALIZED VIEW analytics.v_work_order_lifecycle AS

WITH process_agg AS (
	SELECT 
		dproc.scenario_id,
		dproc.product_id,
		COUNT(*) AS num_steps,
		SUM(dpr.step_cycle_time) AS total_ideal_cycle_time
	FROM staging.dim_process dproc
	JOIN staging.dim_process_route dpr
		ON dproc.process_id = dpr.process_id
		AND dproc.scenario_id = dpr.scenario_id
	GROUP BY dproc.product_id, dproc.scenario_id
),

downtime_agg AS (
	SELECT
		scenario_id,
		work_order_id,
		COUNT(*) AS num_failures,
		SUM(failure_end - failure_start) AS total_downtime
	FROM staging.fact_downtime_event
	GROUP BY work_order_id, scenario_id
),

quality_agg AS (
	SELECT
		scenario_id,
		work_order_id,
		units_approved,
		COALESCE(SUM(units_scrapped), 0) AS units_scrapped
		FROM (
			SELECT *,
				ROW_NUMBER() OVER (
					PARTITION BY work_order_id, scenario_id
					ORDER BY event_time DESC
				) rn
			FROM staging.fact_quality_event
		) q
		WHERE rn = 1
	GROUP BY
		work_order_id,
		units_approved,
		scenario_id
)

SELECT 
	fwo.work_order_id, 
	fwo.product_id, 
	dprod.product_family,
	
	fwo.planned_quantity, 
	fwo.target_yield_rate, 
	fwo.priority, 
	fwo.start_date, 
	fwo.due_date,
	DATE '2025-01-01' AS sim_start_date, -- represents simulation time = 0 reference
	
	fwo.work_order_start_time, 
	fwo.work_order_end_time, 
	(fwo.work_order_end_time - fwo.work_order_start_time) AS work_order_lead_time,
	
	pa.num_steps,
	pa.total_ideal_cycle_time,
	
	da.num_failures,
	da.total_downtime,
	
	qa.units_approved,
	qa.units_scrapped,

	CASE
		WHEN fwo.planned_quantity > 0 THEN
			qa.units_approved::NUMERIC / fwo.planned_quantity
		ELSE NULL
	END AS effective_yield_rate,

	fwo.scenario_id
		
FROM staging.fact_work_order fwo

LEFT JOIN staging.dim_product dprod
	ON fwo.product_id = dprod.product_id
	AND fwo.scenario_id = dprod.scenario_id

LEFT JOIN process_agg pa
	ON fwo.product_id = pa.product_id
	AND fwo.scenario_id = pa.scenario_id
	
LEFT JOIN downtime_agg da
	ON fwo.work_order_id = da.work_order_id
	AND fwo.scenario_id = da.scenario_id
	
LEFT JOIN quality_agg qa
	ON fwo.work_order_id = qa.work_order_id
	AND fwo.scenario_id = qa.scenario_id

GROUP BY
	fwo.work_order_id,
	fwo.product_id,
	fwo.scenario_id,
	dprod.product_family,
	fwo.planned_quantity,
	fwo.target_yield_rate,
	fwo.priority,
	fwo.start_date,
	fwo.due_date,
	fwo.work_order_start_time,
	fwo.work_order_end_time,
	pa.num_steps,
	pa.total_ideal_cycle_time,
	da.num_failures,
	da.total_downtime,
	qa.units_approved,
	qa.units_scrapped;

------------------------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW analytics.v_production_step_execution AS

WITH downtime_agg AS (
    SELECT
        work_order_id,
        process_route_id,
        machine_id,
        scenario_id,
        COUNT(*) AS num_failures,
        COUNT(DISTINCT failure_type) AS num_failure_modes,
        SUM(failure_end - failure_start) AS total_failure_duration,
        AVG(usage_duration) AS avg_operating_duration_till_failure
    FROM staging.fact_downtime_event
    GROUP BY
        work_order_id,
        process_route_id,
        machine_id,
        scenario_id
),
quality_agg AS (
    SELECT
        work_order_id,
        batch_id,
        scenario_id,
        MAX(initial_quantity) AS step_input_quantity,
        SUM(units_approved) AS units_approved,
        SUM(units_scrapped) AS units_scrapped
    FROM staging.fact_quality_event
    GROUP BY
        work_order_id,
        batch_id,
        scenario_id
)
SELECT
    fpe.work_order_id,
    fpe.machine_id,
    fpe.process_route_id,
    fpe.process_id,
    fpe.step_number,
    fpe.batch_id,

    fpe.process_start,
    fpe.process_end,
    (fpe.process_end - fpe.process_start) AS cycle_duration,

    fpe.ideal_cycle_time,
    fpe.actual_cycle_time,
    fpe.actual_cycle_time - fpe.ideal_cycle_time AS cycle_time_difference,

    fpe.event_status,

    COALESCE(da.num_failures, 0) AS num_failures,
    COALESCE(da.total_failure_duration, 0) AS total_failure_duration,
    COALESCE(da.num_failure_modes, 0) AS num_failure_modes,
    COALESCE(da.avg_operating_duration_till_failure, 0) AS avg_operating_duration_till_failure,

    qa.step_input_quantity,
    qa.units_approved::INT,
    qa.units_scrapped::INT,

    CASE
        WHEN qa.step_input_quantity > 0
        THEN qa.units_approved::NUMERIC / qa.step_input_quantity
        ELSE NULL
    END AS step_yield_rate,

    fpe.scenario_id

FROM staging.fact_production_event fpe

LEFT JOIN downtime_agg da
    ON fpe.work_order_id = da.work_order_id
   AND fpe.process_route_id = da.process_route_id
   AND fpe.machine_id = da.machine_id
   AND fpe.scenario_id = da.scenario_id

LEFT JOIN quality_agg qa
    ON fpe.work_order_id = qa.work_order_id
   AND fpe.batch_id = qa.batch_id
   AND fpe.scenario_id = qa.scenario_id;
-----------------------------------------------------------------------------------------------------
CREATE MATERIALIZED VIEW analytics.v_machine_utilization AS
WITH downtime_agg AS (
	SELECT
		machine_id,
		COUNT(*) AS num_failures,
		COUNT(DISTINCT failure_type) AS num_failure_modes,
		AVG(usage_duration) AS avg_operating_duration_till_failure,
		SUM(failure_end - failure_start) AS total_downtime,

		scenario_id
	FROM staging.fact_downtime_event
	GROUP BY machine_id, scenario_id
),

production_agg AS (
	SELECT
		machine_id,
		COUNT(*) AS num_production_events,
		COUNT(DISTINCT work_order_id) AS num_work_orders_processed,
		SUM(process_end - process_start) AS total_processing_time,
		AVG(actual_cycle_time - ideal_cycle_time) AS avg_cycle_time_difference,
		MAX(process_end) AS simulation_horizon_time,
		SUM(process_end - process_start) / NULLIF(MAX(process_end), 0) AS utilization_pct,

		scenario_id
	FROM staging.fact_production_event
	GROUP BY machine_id, scenario_id
),

quality_agg AS (
	SELECT
		machine_id,
		COUNT(DISTINCT batch_id) AS num_batches_completed,
		SUM(initial_quantity) AS total_units_input,
		AVG(initial_quantity) AS avg_input_quantity,
		SUM(units_approved) AS total_units_approved,
		SUM(units_scrapped) AS total_units_scrapped,
		AVG(units_approved) AS avg_num_approved_units,
		AVG(units_scrapped) AS avg_num_scrapped_units,
		-- AVG(units_approved::NUMERIC / NULLIF(initial_quantity::NUMERIC, 0) AS avg_batch_yield_rate,
		SUM(units_approved)::NUMERIC / NULLIF(SUM(initial_quantity)::NUMERIC, 0) AS machine_yield_rate,

		scenario_id
	FROM staging.fact_quality_event
	GROUP BY machine_id, scenario_id
),

downtime_production_failures_comb AS (
	SELECT
		dpc.machine_id,
		SUM((process_end - process_start)) AS total_process_f,
		SUM((process_end - failure_end)) AS total_final_stretch_f,
		SUM((failure_end - failure_start)) AS total_downtime_f,
		dpc.scenario_id
	FROM (
			SELECT
				fde.machine_id,
				fde.failure_start,
				fde.failure_end,
				fpe.process_start,
				fpe.process_end,
				fpe.event_status,

				fpe.scenario_id
			FROM staging.fact_downtime_event fde
			LEFT JOIN staging.fact_production_event fpe
				ON fde.work_order_id = fpe.work_order_id
				AND fde.machine_id = fpe.machine_id
				AND fde.process_id = fpe.process_id
				AND fde.process_route_id = fpe.process_route_id
				AND fde.scenario_id = fpe.scenario_id
			GROUP BY
				fde.machine_id,
				fde.failure_start,
				fde.failure_end,
				fpe.process_start,
				fpe.process_end,
				fpe.event_status,
				fpe.scenario_id
		) dpc
		WHERE dpc.event_status = 'interrupted'
	GROUP BY dpc.machine_id, dpc.scenario_id
)
SELECT
	dm.machine_id,
	dm.machine_type,
	dm.ideal_cycle_time,

	COUNT(DISTINCT dpr.process_route_id) AS num_assigned_process_positions,

	da.num_failures,
	da.num_failure_modes,
	da.avg_operating_duration_till_failure,
	da.total_downtime,
	
	pa.num_production_events,
	pa.num_work_orders_processed,
	pa.total_processing_time,
	pa.avg_cycle_time_difference,
	pa.simulation_horizon_time,
	pa.utilization_pct,

	qa.num_batches_completed,
	qa.total_units_input,
	qa.avg_input_quantity,
	qa.total_units_approved,
	qa.total_units_scrapped,
	qa.avg_num_approved_units,
	qa.avg_num_scrapped_units,
	-- qa.avg_batch_yield_rate,
	qa.machine_yield_rate,

	dpfc.total_process_f,
	dpfc.total_final_stretch_f,
	dpfc.total_downtime_f,

	dm.scenario_id
	
FROM staging.dim_machine dm

LEFT JOIN staging.dim_process_route dpr
	ON dm.machine_type = dpr.machine_type
	AND dm.scenario_id = dpr.scenario_id

LEFT JOIN downtime_agg da
	ON dm.machine_id = da.machine_id
	AND dm.scenario_id = da.scenario_id

LEFT JOIN production_agg pa
	ON dm.machine_id = pa.machine_id
	AND dm.scenario_id = pa.scenario_id

LEFT JOIN quality_agg qa
	ON dm.machine_id = qa.machine_id
	AND dm.scenario_id = qa.scenario_id

LEFT JOIN downtime_production_failures_comb dpfc
	ON dm.machine_id = dpfc.machine_id
	AND dm.scenario_id = dpfc.scenario_id

GROUP BY
	dm.machine_id,
	dm.machine_type,
	dm.ideal_cycle_time,
	dm.scenario_id,
	da.num_failures,
	da.num_failure_modes,
	da.avg_operating_duration_till_failure,
	da.total_downtime,
	pa.num_production_events,
	pa.num_work_orders_processed,
	pa.total_processing_time,
	pa.avg_cycle_time_difference,
	pa.simulation_horizon_time,
	pa.utilization_pct,
	qa.num_batches_completed,
	qa.total_units_input,
	qa.avg_input_quantity,
	qa.total_units_approved,
	qa.total_units_scrapped,
	qa.avg_num_approved_units,
	qa.avg_num_scrapped_units,
	-- qa.avg_batch_yield_rate,
	qa.machine_yield_rate,
	dpfc.total_process_f,
	dpfc.total_final_stretch_f,
	dpfc.total_downtime_f;
--------
CREATE OR REPLACE VIEW analytics.v_time_calendar AS 

SELECT DISTINCT 
	t.sim_time, 
	t.sim_time / 3600.0 AS sim_hour, 
	t.sim_time / 86400.0 AS sim_day, 
	FLOOR(t.sim_time / 3600.0) AS sim_hour_floor, 
	FLOOR(t.sim_time / 86400.0) AS sim_day_floor ,

	t.scenario_id
FROM ( 
	SELECT scenario_id, process_start AS sim_time FROM staging.fact_production_event 
	UNION ALL 
	SELECT scenario_id, process_end FROM staging.fact_production_event 
	UNION ALL SELECT scenario_id, failure_start FROM staging.fact_downtime_event 
	UNION ALL SELECT scenario_id, failure_end FROM staging.fact_downtime_event 
	UNION ALL SELECT scenario_id, event_time FROM staging.fact_quality_event 
) t;