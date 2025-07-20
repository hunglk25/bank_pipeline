-- Create logging tables for data pipeline
-- These tables will store pipeline execution logs, errors, and quality check results

-- 1. Pipeline execution log table
CREATE TABLE IF NOT EXISTS pipeline_execution_log (
    log_id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    dag_id VARCHAR(100) NOT NULL,
    task_id VARCHAR(100) NOT NULL,
    execution_date TIMESTAMP NOT NULL,
    start_time TIMESTAMP DEFAULT NOW(),
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL, -- SUCCESS, FAILED, RUNNING
    error_message TEXT,
    records_processed INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 2. Data quality check log table
CREATE TABLE IF NOT EXISTS data_quality_log (
    log_id SERIAL PRIMARY KEY,
    run_id VARCHAR(100),
    check_timestamp TIMESTAMP DEFAULT NOW(),
    table_name VARCHAR(100),
    check_type VARCHAR(50), -- NULL_CHECK, DUPLICATE_CHECK, BUSINESS_RULE, etc.
    issue_count INTEGER DEFAULT 0,
    issues_json TEXT, -- JSON array of specific issues
    status VARCHAR(20) NOT NULL, -- PASSED, FAILED
    execution_time_ms INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 3. Data upload error log table
CREATE TABLE IF NOT EXISTS data_upload_error_log (
    error_id SERIAL PRIMARY KEY,
    run_id VARCHAR(100),
    table_name VARCHAR(100),
    error_timestamp TIMESTAMP DEFAULT NOW(),
    error_type VARCHAR(50), -- DATABASE_ERROR, VALIDATION_ERROR, etc.
    error_message TEXT,
    failed_record_json TEXT, -- JSON of the record that failed
    retry_count INTEGER DEFAULT 0,
    resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 4. General pipeline error log table
CREATE TABLE IF NOT EXISTS pipeline_error_log (
    error_id SERIAL PRIMARY KEY,
    run_id VARCHAR(100),
    dag_id VARCHAR(100),
    task_id VARCHAR(100),
    error_timestamp TIMESTAMP DEFAULT NOW(),
    error_type VARCHAR(50),
    error_message TEXT,
    stack_trace TEXT,
    context_data TEXT, -- JSON with additional context
    severity VARCHAR(20) DEFAULT 'ERROR', -- ERROR, WARNING, CRITICAL
    created_at TIMESTAMP DEFAULT NOW()
);

-- 5. Data statistics table for monitoring
CREATE TABLE IF NOT EXISTS data_statistics_log (
    stat_id SERIAL PRIMARY KEY,
    run_id VARCHAR(100),
    table_name VARCHAR(100),
    stat_timestamp TIMESTAMP DEFAULT NOW(),
    record_count INTEGER,
    null_count INTEGER,
    duplicate_count INTEGER,
    avg_processing_time_ms INTEGER,
    min_value DECIMAL,
    max_value DECIMAL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_pipeline_execution_run_id ON pipeline_execution_log(run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_execution_status ON pipeline_execution_log(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_execution_date ON pipeline_execution_log(execution_date);

CREATE INDEX IF NOT EXISTS idx_data_quality_run_id ON data_quality_log(run_id);
CREATE INDEX IF NOT EXISTS idx_data_quality_status ON data_quality_log(status);
CREATE INDEX IF NOT EXISTS idx_data_quality_table ON data_quality_log(table_name);

CREATE INDEX IF NOT EXISTS idx_upload_error_run_id ON data_upload_error_log(run_id);
CREATE INDEX IF NOT EXISTS idx_upload_error_resolved ON data_upload_error_log(resolved);
CREATE INDEX IF NOT EXISTS idx_upload_error_timestamp ON data_upload_error_log(error_timestamp);

CREATE INDEX IF NOT EXISTS idx_pipeline_error_run_id ON pipeline_error_log(run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_error_severity ON pipeline_error_log(severity);
CREATE INDEX IF NOT EXISTS idx_pipeline_error_timestamp ON pipeline_error_log(error_timestamp);

CREATE INDEX IF NOT EXISTS idx_data_stats_run_id ON data_statistics_log(run_id);
CREATE INDEX IF NOT EXISTS idx_data_stats_table ON data_statistics_log(table_name);

-- Create a view for easy monitoring of pipeline health
CREATE OR REPLACE VIEW pipeline_health_summary AS
SELECT 
    DATE(execution_date) as execution_date,
    dag_id,
    COUNT(*) as total_runs,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful_runs,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_runs,
    AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_duration_seconds,
    SUM(records_processed) as total_records_processed
FROM pipeline_execution_log 
WHERE execution_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(execution_date), dag_id
ORDER BY execution_date DESC, dag_id;

-- Create a view for data quality summary
CREATE OR REPLACE VIEW data_quality_summary AS
SELECT 
    DATE(check_timestamp) as check_date,
    table_name,
    check_type,
    COUNT(*) as total_checks,
    COUNT(CASE WHEN status = 'PASSED' THEN 1 END) as passed_checks,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_checks,
    SUM(issue_count) as total_issues
FROM data_quality_log 
WHERE check_timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(check_timestamp), table_name, check_type
ORDER BY check_date DESC, table_name, check_type;

-- Insert sample data for testing (optional)
-- INSERT INTO pipeline_execution_log (run_id, dag_id, task_id, execution_date, status)
-- VALUES ('test_run_001', 'bank_data_pipeline', 'generate_data', NOW(), 'SUCCESS');

COMMIT;
