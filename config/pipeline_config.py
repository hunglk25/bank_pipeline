"""
Pipeline Configuration Module
Contains all configuration settings for the banking data pipeline
"""

import os
from typing import Dict, Any
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str = os.getenv('DB_HOST', 'postgres_data')
    port: int = int(os.getenv('DB_PORT', '5432'))
    database: str = os.getenv('DB_NAME', 'mydata')
    user: str = os.getenv('DB_USER', 'user')
    password: str = os.getenv('DB_PASSWORD', 'userpass')
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class DataGenerationConfig:
    """Data generation configuration"""
    customer_count: int = int(os.getenv('CUSTOMER_COUNT', '10'))
    min_devices_per_customer: int = 1
    max_devices_per_customer: int = 2
    min_accounts_per_customer: int = 1
    max_accounts_per_customer: int = 3
    transaction_count: int = 50
    auth_log_count: int = 30
    
    # Business rules
    min_balance: float = 1000.0
    max_balance: float = 1000000.0
    min_transaction_amount: float = 100.0
    max_transaction_amount: float = 50000.0
    risk_threshold: float = 20000.0  # Transactions above this are flagged as risky

@dataclass
class DataQualityConfig:
    """Data quality check configuration"""
    critical_null_fields: list = None
    unique_fields: list = None
    foreign_key_checks: list = None
    business_rules_enabled: bool = True
    max_allowed_null_percentage: float = 0.05  # 5%
    max_allowed_duplicate_percentage: float = 0.01  # 1%
    
    def __post_init__(self):
        if self.critical_null_fields is None:
            self.critical_null_fields = [
                ('Customer', 'NationalID'),
                ('Customer', 'Name'),
                ('Customer', 'Username'),
                ('Account', 'CustomerID'),
                ('Account', 'Balance'),
                ('Transaction', 'Amount'),
                ('Transaction', 'FromAccountID'),
                ('Transaction', 'ToAccountID')
            ]
        
        if self.unique_fields is None:
            self.unique_fields = [
                ('Customer', 'NationalID'),
                ('Customer', 'Username'),
            ]
        
        if self.foreign_key_checks is None:
            self.foreign_key_checks = [
                ('Device', 'CustomerID', 'Customer', 'CustomerID'),
                ('Account', 'CustomerID', 'Customer', 'CustomerID'),
                ('Transaction', 'FromAccountID', 'Account', 'AccountID'),
                ('Transaction', 'ToAccountID', 'Account', 'AccountID'),
                ('AuthenticationLog', 'CustomerID', 'Customer', 'CustomerID'),
                ('AuthenticationLog', 'DeviceID', 'Device', 'DeviceID')
            ]

@dataclass
class PipelineConfig:
    """Main pipeline configuration"""
    pipeline_name: str = 'bank_data_pipeline'
    dag_id: str = 'bank_data_pipeline'
    schedule_interval: str = '@daily'
    max_active_runs: int = 1
    catchup: bool = False
    
    # Email notifications
    email_on_failure: bool = True
    email_on_retry: bool = False
    email_list: list = None
    
    # Retry configuration
    retries: int = 2
    retry_delay_minutes: int = 5
    
    # Timeouts
    task_timeout_minutes: int = 30
    dagrun_timeout_minutes: int = 120
    
    def __post_init__(self):
        if self.email_list is None:
            self.email_list = [os.getenv('ADMIN_EMAIL', 'admin@company.com')]

@dataclass
class LoggingConfig:
    """Logging configuration"""
    log_level: str = os.getenv('LOG_LEVEL', 'INFO')
    log_format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    log_to_db: bool = True
    log_to_file: bool = False
    log_file_path: str = '/opt/airflow/logs/pipeline.log'
    
    # Database logging tables
    pipeline_log_table: str = 'pipeline_execution_log'
    quality_log_table: str = 'data_quality_log'
    error_log_table: str = 'pipeline_error_log'
    upload_error_table: str = 'data_upload_error_log'

# Global configuration instances
DB_CONFIG = DatabaseConfig()
DATA_GEN_CONFIG = DataGenerationConfig()
QUALITY_CONFIG = DataQualityConfig()
PIPELINE_CONFIG = PipelineConfig()
LOGGING_CONFIG = LoggingConfig()

# Airflow connection IDs
POSTGRES_CONN_ID = 'postgres_data'
POSTGRES_DEFAULT_CONN_ID = 'postgres_default'
EMAIL_CONN_ID = 'email_default'

# File paths (for Airflow environment)
SCRIPT_PATHS = {
    'data_generator': '/opt/airflow/src/generate_data_airflow.py',
    'quality_checker': '/opt/airflow/src/data_quality_standards.py',
    'data_uploader': '/opt/airflow/src/data_uploader.py',
    'schema_file': '/opt/airflow/sql/schema.sql',
    'log_tables_file': '/opt/airflow/sql/create_log_tables.sql'
}

def get_config() -> Dict[str, Any]:
    """Get all configuration as a dictionary"""
    return {
        'database': DB_CONFIG,
        'data_generation': DATA_GEN_CONFIG,
        'data_quality': QUALITY_CONFIG,
        'pipeline': PIPELINE_CONFIG,
        'logging': LOGGING_CONFIG,
        'connections': {
            'postgres_conn_id': POSTGRES_CONN_ID,
            'postgres_default_conn_id': POSTGRES_DEFAULT_CONN_ID,
            'email_conn_id': EMAIL_CONN_ID
        },
        'script_paths': SCRIPT_PATHS
    }

def print_config():
    """Print current configuration (for debugging)"""
    config = get_config()
    print("=== PIPELINE CONFIGURATION ===")
    for section, values in config.items():
        print(f"\n[{section.upper()}]")
        if hasattr(values, '__dict__'):
            for key, value in values.__dict__.items():
                if 'password' in key.lower():
                    print(f"  {key}: {'*' * len(str(value))}")
                else:
                    print(f"  {key}: {value}")
        else:
            for key, value in values.items():
                print(f"  {key}: {value}")

if __name__ == "__main__":
    print_config()