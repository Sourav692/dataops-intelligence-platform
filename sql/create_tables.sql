-- DataOps Intelligence Platform - Table Creation Scripts

-- Jobs metadata table
CREATE OR REPLACE TABLE main.dataops_intelligence.jobs_metadata (
    workspace_id STRING,
    job_id STRING,
    job_name STRING,
    owned_by STRING,
    creator_email STRING,
    created_time TIMESTAMP,
    job_type STRING,
    collected_at TIMESTAMP
) USING DELTA;

-- Job runs table
CREATE OR REPLACE TABLE main.dataops_intelligence.job_runs (
    workspace_id STRING,
    job_id STRING,
    run_id STRING,
    run_name STRING,
    run_type STRING,
    trigger_type STRING,
    creator_email STRING,
    period_start_time TIMESTAMP,
    period_end_time TIMESTAMP,
    result_state STRING,
    lifecycle_state STRING,
    state_message STRING,
    termination_code STRING,
    duration_ms BIGINT,
    collected_at TIMESTAMP
) USING DELTA;

-- Job metrics table
CREATE OR REPLACE TABLE main.dataops_intelligence.job_metrics (
    cluster_id STRING,
    day DATE,
    avg_cpu_usage_percent DOUBLE,
    avg_cpu_idle_percent DOUBLE,
    avg_memory_usage_percent DOUBLE,
    avg_network_bytes DOUBLE,
    metric_points BIGINT,
    collected_at TIMESTAMP
) USING DELTA;

-- Job health table
CREATE OR REPLACE TABLE main.dataops_intelligence.job_health (
    workspace_id STRING,
    job_id STRING,
    job_name STRING,
    owned_by STRING,
    health_score DOUBLE,
    health_category STRING,
    requires_attention BOOLEAN,
    success_rate DOUBLE,
    success_rate_score DOUBLE,
    stability_score DOUBLE,
    coefficient_of_variation DOUBLE,
    calculated_at TIMESTAMP
) USING DELTA;

-- Job baselines table
CREATE OR REPLACE TABLE main.dataops_intelligence.job_baselines (
    workspace_id STRING,
    job_id STRING,
    total_runs BIGINT,
    avg_duration_ms DOUBLE,
    stddev_duration_ms DOUBLE,
    median_duration_ms DOUBLE,
    p90_duration_ms DOUBLE,
    min_duration_ms DOUBLE,
    max_duration_ms DOUBLE,
    upper_threshold_ms DOUBLE,
    lower_threshold_ms DOUBLE,
    coefficient_of_variation DOUBLE,
    calculated_at TIMESTAMP
) USING DELTA;

-- Job anomalies table
CREATE OR REPLACE TABLE main.dataops_intelligence.job_anomalies (
    workspace_id STRING,
    job_id STRING,
    anomaly_type STRING,
    anomaly_score DOUBLE,
    recent_avg_duration DOUBLE,
    avg_duration_ms DOUBLE,
    z_score DOUBLE,
    detected_at TIMESTAMP
) USING DELTA;

-- Pipeline runs monitoring table
CREATE OR REPLACE TABLE main.dataops_intelligence.pipeline_runs (
    run_id STRING,
    status STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds BIGINT,
    records_processed STRING,
    tables_updated BIGINT,
    alerts_sent STRING,
    errors STRING,
    environment STRING,
    collection_window_days INT
) USING DELTA;
