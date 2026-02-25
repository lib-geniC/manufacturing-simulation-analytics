
> ## Manufacturing Production Data Analytics Platform
>
> ### Overview
>
> This project is an end-to-end data analytics workflow built around a simulated multi-stage manufacturing environment.
>
> A discrete-event simulation engine generates high-volume synthetic operational data, which is stored in a structured relational warehouse and transformed into analytical views for KPI reporting and scenario comparison.
>
> The focus of this project is data modeling, analytics engineering, performance measurement, and structured experimentation.
>
>---
>
> ### Project Purpose
> 
> This project demonstrates the ability to:
>  - Design a snythetic data generation system
>  - Build a normalized relational schema
>  - Construct analytical SQL views
>  - Perform scenario-based experimentation
>  - Identify system constraints through data analysis
>  - Translate raw event data into decision-ready KPIs
>
> The simulated production system serves as a realistic data source for advanced analytics.
>
>---
>
> ### Architecture
>
> Simulation Engine (Python) 
> -> PostgreSQL Relational Warehouse
> -> Indexed Analytical Views (SQL)
> -> Scenario Comparison & KPI Layer
>
> The emphasis is on transforming granular event-level data into structured, query-efficient analytics.
>
>---
>
> ### Data Model
>
> #### Fact Tables
>
> - fact_work_order
> - fact_production_event
> - fact_downtime_event
> - fact_quality_event
>
> #### Dimension Tables
>
> - dim_scenario
> - dim_machine
> - dim_product
> - dim_process
> - dim_process_route
>
> All simulation runs are tagged with:
> - scenario_id
> - scenario_name
>
> This enables controlled scenario comparison within the same warehouse.
>
>---
>
> ### Scenarios Analyzed (365-Day Horizon)
>
> Three structured scenarios were executed:
>
> #### Scenario A - Controlled Baseline
>
> - Balanced demand
> - Work Order in Progress (WIP) cap enabled
> - Stable throughput and lead times
>
> #### Scenario B - Unconstrained Growth
>
> - Same demand
> - No WIP cap
> - Queue build-up
> - Throughput plateau
>
> #### Scenario C - Lean Control
>
> - High arrival pressure
> - WIP cap enabled
> - Improved flow efficiency
>
>---
>
> ### Key Analytical Findings
> 
> - The system exhibits a structural throughput ceiling (~210 work orders/day).
> - Increasing arrival rate beyond capacity increases queue time but not output.
> - Bottleneck resources saturate while parallel resources remain underutilized.
> - WIP control improves flow efficiency without increasing throughput.
> 
> This project demonstrates constraint-driven system behavior using empirical data analysis.
>
>---
>
> ### Example KPIs Computed
> - Average utilization %
> - Machine-level bottleneck ranking
> - Median & P90 lead times
> - Flow efficiency
> - On-Time delivery rate
> - Failure frequency and downtime impact
> - Daily throughput
>
>---
>
> ### Technical Skills Demonstrated
>
> #### Data Engineering
>
> - Relational schema design
> - Index strategy and performance tuning
> - CTE optimization
> - Aggregation pipelines
> - Scenario tagging for comparative analysis
>
> #### Analytics
>
> - Throughput analysis
> - Bottleneck identification
> - Lead time percentile analysis
> - Flow efficiency computation
> - Capacity vs demand analysis
>
> #### Programming
>
> - Python simulation modeling
> - Stochastic modeling
> - Data generation pipelines
>
>---
>
> ### Technologies Used
>
> - Python (simpy, numpy, pandas)
> - PostgreSQL
> - SQL (CTEs, window functions, indexing)
>
>---
>
> ### Repository Structure
>
>     simulation/	→ Simulation engine
>     database/	→ Schema & analytical SQL
>     analysis/	→ KPI & scenario queries
>     results/	→ Scenario summary outputs
>     images/		→ Architecture & visualization assets
>
>---
>
> ### How to Run
> 
> 1. Install dependencies:
>
>        pip install - r requirements.txt
> 
> 2. Configure database connection.
>	1. Set up PostgreSQL instance and create database.
>
>          psql -U your_username -f database/00_db_setup.sql
>          psql -U your_username -d your_database -f database/00_schema_setup.sql
>
>	2. Update connection parameters by creating a .env file with the following variables defined:
>
>     	    PG_HOST=your_host>	        PG_PORT=your_port
>			PG_DATABASE=your_database
>			PG_USER=your_username
>			PG_PASSWORD=your_password
>
> 3. Run simulation:
>
> 	     python simulation/run_simulation.py
>
> 4. Execute SQL scripts in /database to create schema and analytical views.
>
>---
>
> ### Portfolio Context
>
> This project is part of a broader data analytics portfolio focused on:
> 
> - Operations and planning analytics (manufacturing, utility, etc.)
> - Engineering and R&D data environments
> - Structured experimentation
> - Performance modeling
>
> It demonstrates the ability to convert complex event data into structured analytical insight.
>
>---
>
> Patrick Ortiz<br>
> Data Analytics Portfolio
>
>---