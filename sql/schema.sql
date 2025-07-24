CREATE TABLE IF NOT EXISTS Customer (
  CustomerID SERIAL PRIMARY KEY,
  NationalID VARCHAR(20) UNIQUE NOT NULL,   -- CCCD/Passport
  Name VARCHAR(100),
  Address VARCHAR(255),
  Contact VARCHAR(50),
  Username VARCHAR(50) UNIQUE,
  PasswordHash VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS Device (
  DeviceID SERIAL PRIMARY KEY,
  CustomerID INT REFERENCES Customer(CustomerID),
  DeviceType VARCHAR(50),
  DeviceInfo VARCHAR(255),
  IsVerified BOOLEAN DEFAULT FALSE,
  LastUsed TIMESTAMP
);

CREATE TABLE IF NOT EXISTS Account (
  AccountID SERIAL PRIMARY KEY,
  CustomerID INT REFERENCES Customer(CustomerID),
  AccountType VARCHAR(50),
  Balance DECIMAL(15,2),
  Currency CHAR(3),
  Status VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS Transaction (
  TransactionID SERIAL PRIMARY KEY,
  FromAccountID INT REFERENCES Account(AccountID),
  ToAccountID INT REFERENCES Account(AccountID),
  TxnType VARCHAR(20),
  Amount DECIMAL(15,2),
  Timestamp TIMESTAMP,
  RiskFlag BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS AuthenticationLog (
  AuthID SERIAL PRIMARY KEY,
  CustomerID INT REFERENCES Customer(CustomerID),
  DeviceID INT REFERENCES Device(DeviceID),
  AuthMethod VARCHAR(20),   -- e.g. OTP, Biometric, Password
  AuthStatus VARCHAR(10),   -- e.g. SUCCESS/FAIL
  Timestamp TIMESTAMP
);


CREATE TABLE IF NOT EXISTS RiskAlerts (
    AlertID SERIAL PRIMARY KEY,
    CustomerID INT REFERENCES Customer(CustomerID),
    TransactionID INT REFERENCES Transaction(TransactionID),
    AlertType VARCHAR(50) NOT NULL, -- HIGH_VALUE_NO_STRONG_AUTH, UNVERIFIED_DEVICE, DAILY_LIMIT_EXCEEDED
    AlertLevel VARCHAR(20) DEFAULT 'MEDIUM', -- LOW, MEDIUM, HIGH, CRITICAL
    Description VARCHAR(255),
    Status VARCHAR(20) DEFAULT 'OPEN', -- OPEN, INVESTIGATING, RESOLVED, FALSE_POSITIVE
    CreatedAt TIMESTAMP DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_customers_cccd ON Customer(NationalID);
CREATE INDEX IF NOT EXISTS idx_accounts_customer ON Account(CustomerID);
CREATE INDEX IF NOT EXISTS idx_devices_customer ON Device(CustomerID);
CREATE INDEX IF NOT EXISTS idx_auth_customer ON AuthenticationLog(CustomerID);
CREATE INDEX IF NOT EXISTS idx_transactions_account ON Transaction(FromAccountID);
CREATE INDEX IF NOT EXISTS idx_transactions_time ON Transaction(Timestamp);
CREATE INDEX IF NOT EXISTS idx_risk_alerts_customer ON RiskAlerts(CustomerID);
CREATE INDEX IF NOT EXISTS idx_risk_alerts_status ON RiskAlerts(Status);


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

COMMIT;

